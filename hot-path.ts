export interface BuyBuilderCandidate<T = Uint8Array | null> {
  name: string;
  kind?: "direct" | "fallback";
  build: () => Promise<T>;
}

export const PRE_TRADE_MAX_MARKET_CAP_USD = 6_000;

export function shouldRejectPreTradeMarketCap(marketCapUsd: number | null | undefined): boolean {
  // Fresh detections may not have a reconstructed market cap yet.
  if (marketCapUsd === null || marketCapUsd === undefined) return false;
  return Number(marketCapUsd) >= PRE_TRADE_MAX_MARKET_CAP_USD;
}

const DIRECT_BUILDER_ORDER = [
  "local Pump SDK",
  "local PumpSwap SDK",
  "PumpPortal-trade-local",
];

export function orderBuyBuilders<T extends BuyBuilderCandidate>(candidates: T[]): T[] {
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

    return left.name.localeCompare(right.name);
  });
}

export function selectBuyRoute({
  confirmedPumpFunCurve,
  curveComplete,
  confirmedMigratedToPumpSwap,
}: {
  confirmedPumpFunCurve: boolean;
  curveComplete: boolean;
  confirmedMigratedToPumpSwap: boolean;
}): "pumpfun" | "pumpswap" | "recovery" {
  if (confirmedPumpFunCurve && !curveComplete) return "pumpfun";
  if (confirmedMigratedToPumpSwap) return "pumpswap";
  return "recovery";
}
