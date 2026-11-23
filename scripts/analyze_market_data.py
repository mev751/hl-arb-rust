#!/usr/bin/env python3
"""
High-Performance Analysis of Recorded Market Data (Binance vs Hyperliquid)
Uses line striding (1:20) for 20x faster processing on 1 vCPU without losing statistical fidelity.
Analyzes:
1. Basis & Spread Distribution across 5+ days
2. Hyperliquid & Binance Bid-Ask Spreads & Liquidity (Depth)
3. Mode 1 (Spread Widening) Mean-Reversion: Entry >= 30 bps -> Exit <= 20 bps
4. Mode 2 (Spread Narrowing/Inversion) Mean-Reversion: Entry <= 5 bps (or <= 0) -> Exit >= 18/20 bps
5. Holding Time, Maximum Adverse Excursion (MAE), and Completion Rate (Win Rate)
6. Parameter Grid Sensitivity (Entry/Exit thresholds comparison)
"""

import sys
import os
import glob
import gzip
import json
import math
import time

def percentile(sorted_list, p):
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_list[int(k)]
    d0 = sorted_list[int(f)] * (c - k)
    d1 = sorted_list[int(c)] * (k - f)
    return d0 + d1

def format_dur(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"

def main():
    start_wall = time.time()
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/home/emc/bn-hl-hedg-arb/data/skhy-live"
    files = sorted(glob.glob(os.path.join(data_dir, "spreads-*.jsonl.gz")))
    if not files:
        print(f"No spreads-*.jsonl.gz files found in {data_dir}")
        sys.exit(1)

    print(f"Found {len(files)} files in {data_dir}. Scanning data range with stride=15...")
    sys.stdout.flush()

    time_series = [] # list of (ts_sec, mid_basis, cross_short_bn, cross_long_bn, hl_bid_sz, hl_ask_sz, bn_spread, hl_spread)
    
    total_raw_records = 0
    stride = 15 # Sample every 15th line (~1-2 samples per second, 15x speedup)
    
    for f_idx, filepath in enumerate(files):
        if (f_idx + 1) % 15 == 0 or f_idx == len(files) - 1:
            print(f"  Processed {f_idx + 1}/{len(files)} files ({(f_idx + 1)*100//len(files)}%)...")
            sys.stdout.flush()
            
        current_sec = -1
        latest_record = None
        
        try:
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                for line_idx, line in enumerate(f):
                    total_raw_records += 1
                    if line_idx % stride != 0:
                        continue
                    try:
                        d = json.loads(line)
                        wall_ns = d.get("local_receive_wall", 0)
                        sec = wall_ns // 1_000_000_000
                        if sec != current_sec:
                            if latest_record is not None:
                                time_series.append(latest_record)
                            current_sec = sec
                        latest_record = (
                            sec,
                            float(d.get("mid_basis_bps", 0.0)),
                            float(d.get("cross_short_bn_bps", 0.0)),
                            float(d.get("cross_long_bn_bps", 0.0)),
                            float(d.get("hl_bid_sz", 0.0)),
                            float(d.get("hl_ask_sz", 0.0)),
                            float(d.get("bn_spread_bps", 0.0)),
                            float(d.get("hl_spread_bps", 0.0)),
                        )
                    except Exception:
                        continue
            if latest_record is not None:
                time_series.append(latest_record)
        except Exception as e:
            print(f"Warning reading {filepath}: {e}", file=sys.stderr)

    if not time_series:
        print("No valid records parsed.")
        sys.exit(1)

    elapsed_read = time.time() - start_wall
    print(f"\nDone reading data in {elapsed_read:.1f}s!")
    print(f"Total raw ticks scanned: ~{total_raw_records:,}")
    print(f"Resampled uniform snapshots: {len(time_series):,}")
    
    start_sec = time_series[0][0]
    end_sec = time_series[-1][0]
    total_duration_hours = (end_sec - start_sec) / 3600.0
    print(f"Time span: {total_duration_hours:.2f} hours ({(total_duration_hours/24.0):.2f} days)")
    sys.stdout.flush()
    
    # 1. Distribution of mid_basis_bps
    mid_basis_list = [r[1] for r in time_series]
    sorted_basis = sorted(mid_basis_list)
    n = len(sorted_basis)
    
    mean_basis = sum(sorted_basis) / n
    std_basis = math.sqrt(sum((x - mean_basis)**2 for x in sorted_basis) / n)
    
    p001 = percentile(sorted_basis, 0.001)
    p01 = percentile(sorted_basis, 0.01)
    p05 = percentile(sorted_basis, 0.05)
    p10 = percentile(sorted_basis, 0.10)
    p25 = percentile(sorted_basis, 0.25)
    p50 = percentile(sorted_basis, 0.50)
    p75 = percentile(sorted_basis, 0.75)
    p90 = percentile(sorted_basis, 0.90)
    p95 = percentile(sorted_basis, 0.95)
    p99 = percentile(sorted_basis, 0.99)
    p999 = percentile(sorted_basis, 0.999)
    
    print("\n" + "="*80)
    print("1. MID BASIS (BPS) DISTRIBUTION (BN Mid vs HL Mid)")
    print("="*80)
    print(f"  Count:       {n:,}")
    print(f"  Mean:        {mean_basis:+.2f} bps")
    print(f"  Std Dev:     {std_basis:.2f} bps")
    print(f"  Min:         {sorted_basis[0]:+.2f} bps")
    print(f"  0.1%:        {p001:+.2f} bps")
    print(f"  1.0%:        {p01:+.2f} bps")
    print(f"  5.0%:        {p05:+.2f} bps")
    print(f"  10.0%:       {p10:+.2f} bps")
    print(f"  25.0% (Q1):  {p25:+.2f} bps")
    print(f"  50.0% (Med): {p50:+.2f} bps")
    print(f"  75.0% (Q3):  {p75:+.2f} bps")
    print(f"  90.0%:       {p90:+.2f} bps")
    print(f"  95.0%:       {p95:+.2f} bps")
    print(f"  99.0%:       {p99:+.2f} bps")
    print(f"  99.9%:       {p999:+.2f} bps")
    print(f"  Max:         {sorted_basis[-1]:+.2f} bps")

    # Histogram buckets
    buckets = [
        ("< -10 bps", lambda x: x < -10),
        ("[-10, 0) bps", lambda x: -10 <= x < 0),
        ("[0, 5) bps", lambda x: 0 <= x < 5),
        ("[5, 10) bps", lambda x: 5 <= x < 10),
        ("[10, 15) bps", lambda x: 10 <= x < 15),
        ("[15, 20) bps", lambda x: 15 <= x < 20),
        ("[20, 25) bps", lambda x: 20 <= x < 25),
        ("[25, 30) bps", lambda x: 25 <= x < 30),
        ("[30, 35) bps", lambda x: 30 <= x < 35),
        ("[35, 40) bps", lambda x: 35 <= x < 40),
        (">= 40 bps", lambda x: x >= 40),
    ]
    
    print("\n  Time Distribution by Basis Range:")
    for label, fn in buckets:
        cnt = sum(1 for x in mid_basis_list if fn(x))
        pct = (cnt / n) * 100.0
        hrs = (cnt * (total_duration_hours * 3600.0 / n)) / 3600.0
        bar = "#" * int(pct / 2)
        print(f"    {label:<15}: {pct:5.2f}% ({hrs:5.1f}h)  {bar}")

    # 2. Hyperliquid Top Book Depth
    hl_bid_szs = sorted([r[4] for r in time_series])
    hl_ask_szs = sorted([r[5] for r in time_series])
    print("\n" + "="*80)
    print("2. HYPERLIQUID L1 DEPTH (COIN QUANTITY AT BBO)")
    print("="*80)
    print(f"  Bid Depth (coins): Median={percentile(hl_bid_szs, 0.5):.2f}, 10%={percentile(hl_bid_szs, 0.1):.2f}, 90%={percentile(hl_bid_szs, 0.9):.2f}")
    print(f"  Ask Depth (coins): Median={percentile(hl_ask_szs, 0.5):.2f}, 10%={percentile(hl_ask_szs, 0.1):.2f}, 90%={percentile(hl_ask_szs, 0.9):.2f}")
    pct_bid_ge_01 = (sum(1 for x in hl_bid_szs if x >= 0.1) / n) * 100.0
    pct_ask_ge_01 = (sum(1 for x in hl_ask_szs if x >= 0.1) / n) * 100.0
    pct_bid_ge_10 = (sum(1 for x in hl_bid_szs if x >= 1.0) / n) * 100.0
    pct_ask_ge_10 = (sum(1 for x in hl_ask_szs if x >= 1.0) / n) * 100.0
    print(f"  % Time Depth >= 0.1 coin: Bid {pct_bid_ge_01:.1f}%, Ask {pct_ask_ge_01:.1f}%")
    print(f"  % Time Depth >= 1.0 coin: Bid {pct_bid_ge_10:.1f}%, Ask {pct_ask_ge_10:.1f}%")

    # 3. Mode 1 Simulation
    def simulate_mode1(entry_th, exit_th, max_hold_sec=14400):
        trades = []
        in_trade = False
        entry_time = 0
        entry_basis = 0.0
        peak_basis = 0.0
        
        for sec, basis, _, _, _, _, _, _ in time_series:
            if not in_trade:
                if basis >= entry_th:
                    in_trade = True
                    entry_time = sec
                    entry_basis = basis
                    peak_basis = basis
            else:
                if basis > peak_basis:
                    peak_basis = basis
                mae = peak_basis - entry_basis
                dur = sec - entry_time
                if basis <= exit_th:
                    trades.append((dur, mae, True))
                    in_trade = False
                elif dur >= max_hold_sec:
                    trades.append((dur, mae, False))
                    in_trade = False
        return trades

    def simulate_mode2(entry_th, exit_th, max_hold_sec=14400):
        trades = []
        in_trade = False
        entry_time = 0
        entry_basis = 0.0
        trough_basis = 0.0
        
        for sec, basis, _, _, _, _, _, _ in time_series:
            if not in_trade:
                if basis <= entry_th:
                    in_trade = True
                    entry_time = sec
                    entry_basis = basis
                    trough_basis = basis
            else:
                if basis < trough_basis:
                    trough_basis = basis
                mae = entry_basis - trough_basis
                dur = sec - entry_time
                if basis >= exit_th:
                    trades.append((dur, mae, True))
                    in_trade = False
                elif dur >= max_hold_sec:
                    trades.append((dur, mae, False))
                    in_trade = False
        return trades

    print("\n" + "="*80)
    print("3. MODE 1 SIMULATION (正向套利: 逢高做空基差 Short BN Maker + Long HL Taker)")
    print("   Baseline logic: Entry at >= 30.0 bps, Exit at <= 20.0 bps")
    print("="*80)
    
    m1_configs = [
        (35.0, 20.0),
        (32.0, 20.0),
        (30.0, 20.0),
        (30.0, 22.0),
        (28.0, 20.0),
        (28.0, 22.0),
        (25.0, 18.0),
    ]
    
    print(f"{'Entry':>7} | {'Exit':>6} | {'Target':>7} | {'Trades':>6} | {'Per Day':>7} | {'WinRate':>7} | {'Med Hold':>8} | {'90% Hold':>8} | {'Avg MAE':>8} | {'Max MAE':>8}")
    print("-" * 92)
    for eth, xth in m1_configs:
        tr = simulate_mode1(eth, xth)
        if not tr:
            print(f"{eth:7.1f} | {xth:6.1f} | {eth-xth:7.1f} |      0 |     0.0 |     N/A |      N/A |      N/A |      N/A |      N/A")
            continue
        total_tr = len(tr)
        per_day = total_tr / (total_duration_hours / 24.0)
        wins = [t for t in tr if t[2]]
        win_rate = (len(wins) / total_tr) * 100.0
        durs = sorted([t[0] for t in wins])
        maes = sorted([t[1] for t in tr])
        med_dur = format_dur(percentile(durs, 0.5)) if durs else "N/A"
        p90_dur = format_dur(percentile(durs, 0.9)) if durs else "N/A"
        avg_mae = sum(maes) / len(maes)
        max_mae = maes[-1]
        print(f"{eth:7.1f} | {xth:6.1f} | {eth-xth:7.1f} | {total_tr:6d} | {per_day:7.1f} | {win_rate:6.1f}% | {med_dur:>8} | {p90_dur:>8} | {avg_mae:7.1f}b | {max_mae:7.1f}b")

    print("\n" + "="*80)
    print("4. MODE 2 SIMULATION (反向套利: 逢低做多基差 Long BN Maker + Short HL Taker)")
    print("   User Idea: Entry at <= 5.0 bps (or <= 0 bps), Exit at >= 18.0/20.0 bps")
    print("="*80)

    m2_configs = [
        (0.0, 15.0),
        (0.0, 18.0),
        (5.0, 15.0),
        (5.0, 18.0),
        (5.0, 20.0),
        (8.0, 18.0),
        (10.0, 18.0),
        (10.0, 20.0),
        (12.0, 20.0),
    ]

    print(f"{'Entry':>7} | {'Exit':>6} | {'Target':>7} | {'Trades':>6} | {'Per Day':>7} | {'WinRate':>7} | {'Med Hold':>8} | {'90% Hold':>8} | {'Avg MAE':>8} | {'Max MAE':>8}")
    print("-" * 92)
    for eth, xth in m2_configs:
        tr = simulate_mode2(eth, xth)
        if not tr:
            print(f"{eth:7.1f} | {xth:6.1f} | {xth-eth:7.1f} |      0 |     0.0 |     N/A |      N/A |      N/A |      N/A |      N/A")
            continue
        total_tr = len(tr)
        per_day = total_tr / (total_duration_hours / 24.0)
        wins = [t for t in tr if t[2]]
        win_rate = (len(wins) / total_tr) * 100.0
        durs = sorted([t[0] for t in wins])
        maes = sorted([t[1] for t in tr])
        med_dur = format_dur(percentile(durs, 0.5)) if durs else "N/A"
        p90_dur = format_dur(percentile(durs, 0.9)) if durs else "N/A"
        avg_mae = sum(maes) / len(maes)
        max_mae = maes[-1]
        print(f"{eth:7.1f} | {xth:6.1f} | {xth-eth:7.1f} | {total_tr:6d} | {per_day:7.1f} | {win_rate:6.1f}% | {med_dur:>8} | {p90_dur:>8} | {avg_mae:7.1f}b | {max_mae:7.1f}b")

    print("\n" + "="*80)
    print("ANALYSIS COMPLETED SUCCESSFULLY")
    print("="*80)

if __name__ == "__main__":
    main()
