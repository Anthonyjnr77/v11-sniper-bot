import assert from "node:assert/strict";
import test from "node:test";

import {
  PRE_TRADE_MAX_MARKET_CAP_USD,
  orderBuyBuilders,
  selectBuyRoute,
  shouldRejectPreTradeMarketCap,
  type BuyBuilderCandidate,
} from "../hot-path.js";

test("keeps the buy hot path direct-only", () => {
  const candidates: BuyBuilderCandidate[] = [
    { name: "Jupiter", kind: "fallback", build: async () => null },
    { name: "local PumpSwap SDK", kind: "direct", build: async () => null },
    { name: "PumpPortal-trade-local", kind: "direct", build: async () => null },
    { name: "local Pump SDK", kind: "direct", build: async () => null },
  ];

  const ordered = orderBuyBuilders(candidates);

  const names = ordered.map((candidate: BuyBuilderCandidate) => candidate.name);
  // Local builders are primary; PumpPortal is the secondary fallback.
  assert.equal(names[0], "local Pump SDK");
  assert.equal(names[1], "local PumpSwap SDK");
  // Ensure the direct kinds are first
  assert.equal(ordered[0]?.kind, "direct");
  assert.equal(ordered[1]?.kind, "direct");
});

test("routes confirmed incomplete curves to Pump.fun only", () => {
  assert.equal(
    selectBuyRoute({
      confirmedPumpFunCurve: true,
      curveComplete: false,
      confirmedMigratedToPumpSwap: false,
    }),
    "pumpfun",
  );
});

test("routes PumpSwap only after confirmed migration", () => {
  assert.equal(
    selectBuyRoute({
      confirmedPumpFunCurve: true,
      curveComplete: true,
      confirmedMigratedToPumpSwap: true,
    }),
    "pumpswap",
  );
});

test("does not select a venue when migration state is unknown", () => {
  assert.equal(
    selectBuyRoute({
      confirmedPumpFunCurve: false,
      curveComplete: false,
      confirmedMigratedToPumpSwap: false,
    }),
    "recovery",
  );
});

test("rejects only known pre-trade market caps above the V14 threshold", () => {
  // Fresh detections can have an unknown market cap without being blocked.
  assert.equal(shouldRejectPreTradeMarketCap(null), false);
  assert.equal(shouldRejectPreTradeMarketCap(4999), false);
  assert.equal(PRE_TRADE_MAX_MARKET_CAP_USD, 6000);
  assert.equal(shouldRejectPreTradeMarketCap(PRE_TRADE_MAX_MARKET_CAP_USD), true);
  assert.equal(shouldRejectPreTradeMarketCap(5999.99), false);
  assert.equal(shouldRejectPreTradeMarketCap(10_000), true);
});
