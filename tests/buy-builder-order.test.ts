import assert from "node:assert/strict";
import test from "node:test";

import {
  PRE_TRADE_MAX_MARKET_CAP_USD,
  orderBuyBuilders,
  shouldRejectPreTradeMarketCap,
  type BuyBuilderCandidate,
} from "../hot-path.js";

test("keeps the buy hot path direct-only", () => {
  const candidates: BuyBuilderCandidate[] = [
    { name: "Jupiter", kind: "fallback", build: async () => null },
    { name: "local PumpSwap SDK", kind: "fallback", build: async () => null },
    { name: "PumpPortal-trade-local", kind: "direct", build: async () => null },
    { name: "local Pump SDK", kind: "direct", build: async () => null },
  ];

  const ordered = orderBuyBuilders(candidates);

  const names = ordered.map((candidate: BuyBuilderCandidate) => candidate.name);
  // V15 contract: direct builders must be exactly these two and come first
  assert.equal(names[0], "PumpPortal-trade-local");
  assert.equal(names[1], "local Pump SDK");
  // Ensure the direct kinds are first
  assert.equal(ordered[0]?.kind, "direct");
  assert.equal(ordered[1]?.kind, "direct");
});

test("rejects pre-trade market caps above the V14 threshold", () => {
  // Unknown market cap should be rejected (safety-first)
  assert.equal(shouldRejectPreTradeMarketCap(null), true);
  assert.equal(shouldRejectPreTradeMarketCap(4999), false);
  assert.equal(shouldRejectPreTradeMarketCap(PRE_TRADE_MAX_MARKET_CAP_USD), false);
  assert.equal(shouldRejectPreTradeMarketCap(5000.01), true);
  assert.equal(shouldRejectPreTradeMarketCap(10_000), true);
});
