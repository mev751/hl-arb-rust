use arb_core::VenueId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub venue: VenueId,
    pub symbol: String,
    pub rate: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct FundingCalibrator {
    /// Latest observed funding rates per (Venue, Symbol)
    rates: HashMap<(VenueId, String), FundingSnapshot>,
}

impl FundingCalibrator {
    pub fn new() -> Self {
        Self {
            rates: HashMap::new(),
        }
    }

    pub fn update_rate(&mut self, venue: VenueId, symbol: &str, rate: Decimal) {
        self.rates.insert(
            (venue, symbol.to_string()),
            FundingSnapshot {
                venue,
                symbol: symbol.to_string(),
                rate,
                timestamp: Utc::now(),
            },
        );
    }

    /// Calculate net funding carry in basis points expected over `holding_hours`.
    /// Positive carry means we receive funding across the hedged pair; negative carry means we pay.
    /// E.g. If Long HL and Short BN:
    /// Carry = (HL funding received - BN funding paid) * hours
    pub fn compute_expected_carry_bps(
        &self,
        symbol_hl: &str,
        symbol_bn: &str,
        holding_hours: Decimal,
    ) -> Decimal {
        let rate_hl = self
            .rates
            .get(&(VenueId::Hyperliquid, symbol_hl.to_string()))
            .map(|s| s.rate)
            .unwrap_or(Decimal::ZERO);

        let rate_bn = self
            .rates
            .get(&(VenueId::BinanceFutures, symbol_bn.to_string()))
            .map(|s| s.rate)
            .unwrap_or(Decimal::ZERO);

        // Hyperliquid rates are typically 1-hour based, Binance is 8-hour based.
        // Normalize both to hourly:
        let hourly_hl = rate_hl; // 1-hour rate
        let hourly_bn = rate_bn / Decimal::from(8); // normalize 8h to 1h

        // For Long HL / Short BN:
        // Long HL pays funding if rate > 0, receives if rate < 0
        // Short BN receives funding if rate > 0, pays if rate < 0
        // Net received = (rate_bn - rate_hl)
        let net_hourly_rate = hourly_bn - hourly_hl;
        let bps_factor = Decimal::from(10_000);

        net_hourly_rate * holding_hours * bps_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_funding_carry_calibration() {
        let mut calibrator = FundingCalibrator::new();
        // BN 8h funding is +0.08% (+0.0008), so hourly = +0.0001
        calibrator.update_rate(VenueId::BinanceFutures, "SKHYUSDT", dec!(0.0008));
        // HL 1h funding is +0.00005
        calibrator.update_rate(VenueId::Hyperliquid, "xyz:SKHY", dec!(0.00005));

        // Over 4 hours holding:
        // hourly carry = 0.0001 - 0.00005 = 0.00005
        // 4 hours = 0.0002 = 2.0 bps
        let carry_bps = calibrator.compute_expected_carry_bps("xyz:SKHY", "SKHYUSDT", dec!(4));
        assert_eq!(carry_bps, dec!(2.0));
    }
}
