export interface BuyBuilderCandidate<T = Uint8Array | null> {
  name: string;
  kind?: "direct" | "fallback";
  build: () => Promise<T>;
}

const DIRECT_BUILDER_ORDER = [
  "PumpPortal-trade-local-pump",
  "PumpPortal-trade-local",
  "local Pump SDK",
  "local PumpSwap SDK",
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

    if (left.name === "Jupiter") return 1;
    if (right.name === "Jupiter") return -1;

    return left.name.localeCompare(right.name);
  });
}
