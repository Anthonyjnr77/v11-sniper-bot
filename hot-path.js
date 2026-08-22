export const DIRECT_BUILDER_ORDER = [
  "local Pump SDK",
  "local PumpSwap SDK",
  "PumpPortal-trade-local",
];

export const PRE_TRADE_MAX_MARKET_CAP_USD = 6000;

export function shouldRejectPreTradeMarketCap(marketCapUsd) {
  // Fresh detections may not have a reconstructed market cap yet.
  if (marketCapUsd === null || marketCapUsd === undefined) return false;
  return Number(marketCapUsd) >= PRE_TRADE_MAX_MARKET_CAP_USD;
}

export function orderBuyBuilders(candidates) {
  return [...candidates].sort((left, right) => {
    const leftRank = DIRECT_BUILDER_ORDER.indexOf(left.name);
    const rightRank = DIRECT_BUILDER_ORDER.indexOf(right.name);

    const leftIsDirect = leftRank >= 0;
    const rightIsDirect = rightRank >= 0;

    if (leftIsDirect !== rightIsDirect) {
      return leftIsDirect ? -1 : 1;
    }

    if (leftRank >= 0 && rightRank >= 0) {
      return leftRank - rightRank;
    }

    if (left.name === "Jupiter") return 1;
    if (right.name === "Jupiter") return -1;

    return left.name.localeCompare(right.name);
  });
}

export function selectBuyRoute({ confirmedPumpFunCurve, curveComplete, confirmedMigratedToPumpSwap }) {
  if (confirmedPumpFunCurve && !curveComplete) return "pumpfun";
  if (confirmedMigratedToPumpSwap) return "pumpswap";
  return "recovery";
}
