use rust_decimal::Decimal;

/// Compute midpoint basis in basis points (bps):
/// `mid_basis_bps = ((bn_mid - hl_mid) / avg_mid) * 10,000`
pub fn compute_mid_basis_bps(bn_mid: Decimal, hl_mid: Decimal) -> Option<Decimal> {
    if bn_mid <= Decimal::ZERO || hl_mid <= Decimal::ZERO {
        return None;
    }
    let avg_mid = (bn_mid + hl_mid) / Decimal::from(2);
    if avg_mid.is_zero() {
        return None;
    }
    let bps_factor = Decimal::from(10_000);
    Some(((bn_mid - hl_mid) / avg_mid) * bps_factor)
}

/// Compute executable spread in basis points:
/// When buying HL and hedging by selling BN (HL Buy IOC, BN Sell Market):
/// `exec_spread_bps = (bn_bid - hl_ask) / avg_mid * 10,000`
pub fn compute_long_hl_exec_spread_bps(
    hl_ask: Decimal,
    bn_bid: Decimal,
    reference_mid: Decimal,
) -> Option<Decimal> {
    if reference_mid <= Decimal::ZERO || hl_ask <= Decimal::ZERO || bn_bid <= Decimal::ZERO {
        return None;
    }
    let bps_factor = Decimal::from(10_000);
    Some(((bn_bid - hl_ask) / reference_mid) * bps_factor)
}

/// Compute executable spread in basis points:
/// When selling HL and hedging by buying BN (HL Sell IOC, BN Buy Market):
/// `exec_spread_bps = (hl_bid - bn_ask) / reference_mid * 10,000`
pub fn compute_short_hl_exec_spread_bps(
    hl_bid: Decimal,
    bn_ask: Decimal,
    reference_mid: Decimal,
) -> Option<Decimal> {
    if reference_mid <= Decimal::ZERO || hl_bid <= Decimal::ZERO || bn_ask <= Decimal::ZERO {
        return None;
    }
    let bps_factor = Decimal::from(10_000);
    Some(((hl_bid - bn_ask) / reference_mid) * bps_factor)
}

/// Quantize order quantity to step size (rounds down by default to prevent margin overflow).
pub fn quantize_qty(qty: Decimal, step_size: Decimal) -> Decimal {
    if step_size <= Decimal::ZERO || qty <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let units = (qty / step_size).floor();
    units * step_size
}

/// Quantize price to tick size.
/// If `round_up` is true (e.g. for Buy IOC limit price), rounds towards ceiling.
/// If false (e.g. for Sell), rounds towards floor.
pub fn quantize_price(price: Decimal, tick_size: Decimal, round_up: bool) -> Decimal {
    if tick_size <= Decimal::ZERO || price <= Decimal::ZERO {
        return price;
    }
    let steps = price / tick_size;
    let rounded_steps = if round_up {
        steps.ceil()
    } else {
        steps.floor()
    };
    rounded_steps * tick_size
}

/// Compute Hyperliquid IOC limit price allowing for configured slippage in bps.
/// For a Buy: limit = ask * (1 + slippage_bps / 10,000) rounded UP to tick_size.
/// For a Sell: limit = bid * (1 - slippage_bps / 10,000) rounded DOWN to tick_size.
pub fn compute_hl_ioc_limit_price(
    best_price: Decimal,
    slippage_bps: Decimal,
    tick_size: Decimal,
    is_buy: bool,
) -> Decimal {
    let bps_factor = Decimal::from(10_000);
    let slippage_multiplier = slippage_bps / bps_factor;

    let target_price = if is_buy {
        best_price * (Decimal::ONE + slippage_multiplier)
    } else {
        best_price * (Decimal::ONE - slippage_multiplier)
    };

    quantize_price(target_price, tick_size, is_buy)
}

/// Calculate net edge after accounting for venue taker/maker fees.
/// `fees_bps`: total expected roundtrip fees across both legs in basis points.
pub fn compute_net_edge_bps(gross_spread_bps: Decimal, roundtrip_fees_bps: Decimal) -> Decimal {
    gross_spread_bps - roundtrip_fees_bps
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_compute_mid_basis_bps() {
        let bn_mid = dec!(100.50);
        let hl_mid = dec!(100.00);
        let basis = compute_mid_basis_bps(bn_mid, hl_mid).unwrap();
        // (100.50 - 100.00) / 100.25 * 10000 = 0.50 / 100.25 * 10000 ≈ 49.875 bps
        assert!(basis > dec!(49.8) && basis < dec!(49.9));
    }

    #[test]
    fn test_compute_long_hl_exec_spread() {
        let hl_ask = dec!(100.10);
        let bn_bid = dec!(100.60);
        let mid = dec!(100.35);
        let spread = compute_long_hl_exec_spread_bps(hl_ask, bn_bid, mid).unwrap();
        // (100.60 - 100.10) / 100.35 * 10000 = 0.50 / 100.35 * 10000 ≈ 49.825 bps
        assert!(spread > dec!(49.8) && spread < dec!(49.9));
    }

    #[test]
    fn test_quantize_qty() {
        let step = dec!(0.1);
        let qty = dec!(1.258);
        assert_eq!(quantize_qty(qty, step), dec!(1.2));
    }

    #[test]
    fn test_compute_hl_ioc_limit_price() {
        let ask = dec!(10.00);
        let slippage_bps = dec!(10.0); // 0.1% -> 10.01
        let tick = dec!(0.01);
        let limit = compute_hl_ioc_limit_price(ask, slippage_bps, tick, true);
        assert_eq!(limit, dec!(10.01));
    }
}
