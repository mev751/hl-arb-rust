# Bidirectional Arbitrage (Mode 1 Upward + Mode 2 Downward) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement bidirectional statistical arbitrage in `MakerTakerEngine` to support both Mode 1 (Spread $\ge 30\text{ bps}$: Short BN Maker + Long HL Taker, exit at $\le 20\text{ bps}$) and Mode 2 (Spread $\le 5\text{ bps}$: Long BN Maker + Short HL Taker, exit at $\ge 18\text{ bps}$).

**Architecture:** Extend `StrategyConfig` with reverse arbitrage parameters (`enable_reverse_arb`, `reverse_entry_spread_bps`, `reverse_exit_spread_bps`, and adaptive deltas). Add `ArbDirection` tracking in `arb-core` and make order quoting, hedging (`reduce_only`), fill accounting, emergency liquidation, and roundtrip PnL calculation fully symmetric for long and short spread positions.

**Tech Stack:** Rust 2021, Tokio, `rust_decimal`, `parking_lot`, Serde, Tracing.

**Spec:** Statistical basis mean reversion on `xyz:SKHY` (Hyperliquid) vs `SKHYUSDT` (Binance Futures), structural median $\sim 19.0\text{ bps}$.

## Global Constraints

- Execution mode: Binance GTX Post-Only Maker (0% fee) -> Hyperliquid WS `post` IOC Taker (0.86 bps fee).
- Bidirectional roundtrip fees: Exactly $1.72\text{ bps}$.
- No regression on existing Mode 1 execution, REST deduplication, or cancel watchdog.
- All existing and new tests must pass `cargo test`.

## Review Focus

1. `reduce_only` flag on Hyperliquid IOC hedges must be `true` when reducing existing position and `false` when opening new position in either direction.
2. In Mode 2, BN quotes `Buy` for entry and `Sell` for exit; pricing math must maintain correct sign and quantization relative to HL BBO.
3. Roundtrip PnL accounting for Mode 2 must compute $(P_{exit,bn} - P_{entry,bn}) + (P_{entry,hl} - P_{exit,hl})$.
4. Max hold emergency close must handle `pos_hl < 0` by buying HL to close short position.
5. Mutual exclusion when Idle: Mode 1 and Mode 2 entry quotes cannot conflict or flip back and forth erratically.

---

### Task 1: Add `ArbDirection` to `arb-core` and extend `TradeRoundTrip`

**Files:**
- Modify: `crates/arb-core/src/types.rs`
- Test: `crates/arb-core/tests/` or inline tests

- [x] Add `ArbDirection` enum (`ShortSpread`, `LongSpread`) with `Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq`.
- [x] Add `direction: Option<ArbDirection>` to `TradeRoundTrip`.
- [x] Verify `cargo check -p arb-core`.

---

### Task 2: Extend `StrategyConfig` with Reverse Arb Parameters in `arb-strategy`

**Files:**
- Modify: `crates/arb-strategy/src/strategy.rs`
- Modify: `config/default.toml`

- [x] Add configuration fields to `StrategyConfig`:
  - `enable_reverse_arb: bool` (default: `true`)
  - `reverse_entry_spread_bps: Decimal` (default: `dec!(5.0)`)
  - `reverse_exit_spread_bps: Decimal` (default: `dec!(18.0)`)
  - `adaptive_reverse_entry_delta_bps: Decimal` (default: `dec!(14.0)`)
  - `adaptive_reverse_exit_delta_bps: Decimal` (default: `dec!(1.0)`)
- [x] Add helper methods to `StrategyConfig`:
  - `reverse_thresholds(&self, ema_basis_bps: Option<f64>) -> (Decimal, Decimal)`
- [x] Update `config/default.toml` with default reverse arb settings.
- [x] Verify `cargo check -p arb-strategy`.

---

### Task 3: Implement Symmetric Bidirectional Quoting and Hedging in `MakerTakerEngine`

**Files:**
- Modify: `crates/arb-engine/src/maker_taker.rs`

- [x] Update `RoundAccumulator` to track `direction: Option<ArbDirection>`.
- [x] Update `compute_desired_quote`:
  - When in position:
    - If `pos_bn < 0`: Mode 1 exit (Buy BN Maker when $spread \le eff\_exit\_bps$).
    - If `pos_bn > 0`: Mode 2 exit (Sell BN Maker when $spread \ge eff\_reverse\_exit\_bps$).
  - When idle:
    - If $spread \ge eff\_entry\_bps$: Mode 1 entry (Sell BN Maker).
    - If $spread \le eff\_reverse\_entry\_bps$ and `enable_reverse_arb`: Mode 2 entry (Buy BN Maker).
- [x] Fix `hedge_delta_if_needed`:
  - Compute `reduce_only` dynamically based on current `pos_hl` sign:
    `reduce_only = (is_buy && pos_hl < 0) || (!is_buy && pos_hl > 0)`.
- [x] Update `record_bn_fill` and `record_hl_fill` to account for `ArbDirection` in entry/exit notional and fees.
- [x] Update `maybe_finalize_round` PnL computation:
  - If `ShortSpread`: `hl_pnl = exit_hl - entry_hl`, `bn_pnl = entry_bn - exit_bn`.
  - If `LongSpread`: `hl_pnl = entry_hl - exit_hl`, `bn_pnl = exit_bn - entry_bn`.
- [x] Update `check_max_hold_emergency` to handle `hl_qty < 0` (Buy HL IOC with `reduce_only = true`).
- [x] Verify `cargo check -p arb-engine`.

---

### Task 4: Integration and Unit Tests for Bidirectional Arbitrage

**Files:**
- Modify: `crates/arb-engine/tests/maker_taker_test.rs`

- [x] Write `test_maker_taker_reverse_arb_roundtrip`:
  - Push quotes with spread $\le 5\text{ bps}$ (e.g. BN 100.04/100.06, HL 100.00/100.01).
  - Verify engine places BN Maker Buy at 100.05.
  - Simulate BN fill and verify engine hedges with HL IOC Sell (`reduce_only = false`).
  - Push quotes with spread $\ge 18\text{ bps}$ (e.g. BN 100.19/100.21, HL 100.00/100.01).
  - Verify engine places BN Maker Sell at 100.20 (`reduce_only = true`).
  - Simulate BN fill and verify engine hedges with HL IOC Buy (`reduce_only = true`).
  - Verify position returns to 0, round is finalized, and net PnL is positive.
- [x] Write `test_bidirectional_mode1_then_mode2`:
  - Run Mode 1 roundtrip, then Mode 2 roundtrip, verify both succeed.
- [x] Write `test_reverse_arb_max_hold_emergency`:
  - Verify short HL / long BN position triggers emergency market close on timeout.
- [x] Run all tests: `cargo test --package arb-engine --test maker_taker_test`.
