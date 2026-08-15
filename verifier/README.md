Verifier
========

This is a standalone read-only verifier that reconstructs pre-buy Pump.fun bonding-curve reserves and market-cap using BN integer math.

Usage:

1. Create an input JSON file (example below).
2. Run:

```bash
node verifier/verifier.mjs verifier/input.json
```

Input JSON example:

{
  "postVirtualQuoteReserves": "100000000000",
  "postVirtualTokenReserves": "500000000000",
  "solAmount": "1000000000",
  "mintSupply": "1000000",
  "realTokenReserves": "500000000000",
  "feeTiers": [
    { "maxMarketCap": 100000000, "feeBps": 500 },
    { "maxMarketCap": 500000000, "feeBps": 300 },
    { "maxMarketCap": null, "feeBps": 100 }
  ]
}

Notes:
- The verifier expects integer values (strings are accepted). All math is done with `bn.js`.
- Provide the exact `feeTiers` from your cached bonding-curve configuration to ensure deterministic results.
- The script does not modify any production code.
