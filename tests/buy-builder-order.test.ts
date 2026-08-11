import assert from "node:assert/strict";
import test from "node:test";

import { orderBuyBuilders, type BuyBuilderCandidate } from "../hot-path.js";

test("orders direct Pump builders ahead of Jupiter", () => {
  const candidates: BuyBuilderCandidate[] = [
    { name: "Jupiter", kind: "fallback", build: async () => null },
    { name: "local PumpSwap SDK", kind: "direct", build: async () => null },
    { name: "PumpPortal-trade-local", kind: "direct", build: async () => null },
    { name: "local Pump SDK", kind: "direct", build: async () => null },
    // PumpPortal pump-only route deprecated; not included
  ];

  const ordered = orderBuyBuilders(candidates);

  assert.deepEqual(
    ordered.map((candidate: BuyBuilderCandidate) => candidate.name),
    ["PumpPortal-trade-local", "local Pump SDK", "local PumpSwap SDK", "Jupiter"]
  );
});
