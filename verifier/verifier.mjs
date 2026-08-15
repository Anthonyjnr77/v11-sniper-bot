import BN from "bn.js";
import fs from "fs";

function toBN(v) {
  if (BN.isBN(v)) return v;
  if (typeof v === "string") return new BN(v);
  if (typeof v === "number") return new BN(String(v));
  return new BN(String(v));
}

function floorDiv(a, b) {
  return a.div(b);
}

function ceilDiv(a, b) {
  return a.add(b).subn(1).div(b);
}

// Compute inputAmount = floor((amount - 1) * 10000 / (10000 + totalFeeBps))
function computeInputAmount(solAmountBn, totalFeeBps) {
  const A = solAmountBn.subn(1).mul(new BN(10000));
  const D = new BN(10000 + totalFeeBps);
  if (A.lt(new BN(0))) return new BN(0);
  return floorDiv(A, D);
}

// Given post virtual reserves (Q_post, T_post) and inputAmount i,
// solve x = floor(i * T_post / (Q_post - i)).
function computeTokensReceivedFromPost(Q_post, T_post, inputAmount) {
  const denom = Q_post.sub(inputAmount);
  if (denom.lte(new BN(0))) return null; // invalid
  const numer = inputAmount.mul(T_post);
  return floorDiv(numer, denom);
}

// bondingCurveMarketCap: marketCap = virtualQuoteReserves * mintSupply / virtualTokenReserves
function computeMarketCap(virtualQuoteReserves_pre, mintSupply, virtualTokenReserves_pre) {
  // integer floor division
  return floorDiv(virtualQuoteReserves_pre.mul(mintSupply), virtualTokenReserves_pre);
}

function pickFeeForMarketCap(feeTiers, marketCap) {
  // feeTiers: array of { maxMarketCap: string|null, feeBps: number }
  for (const tier of feeTiers) {
    if (tier.maxMarketCap === null) return tier.feeBps;
    const max = toBN(tier.maxMarketCap);
    if (marketCap.lte(max)) return tier.feeBps;
  }
  return feeTiers[feeTiers.length - 1].feeBps;
}

function runVerification(event) {
  // Required fields
  const Q_post = toBN(event.postVirtualQuoteReserves);
  const T_post = toBN(event.postVirtualTokenReserves);
  const solAmount = toBN(event.solAmount);
  const mintSupply = toBN(event.mintSupply);
  const realTokenReserves = event.realTokenReserves ? toBN(event.realTokenReserves) : null;
  const feeTiers = (event.feeTiers || []).map(t => ({ maxMarketCap: t.maxMarketCap == null ? null : String(t.maxMarketCap), feeBps: Number(t.feeBps) }));

  if (!Q_post || !T_post || !solAmount || !mintSupply || feeTiers.length === 0) {
    return { status: 'INSUFFICIENT_DATA', reason: 'Missing required fields (postVirtualQuoteReserves, postVirtualTokenReserves, solAmount, mintSupply, feeTiers)'};
  }

  const consistent = [];
  const checked = [];

  // Iterate over candidate feeBps values from feeTiers (unique values)
  const candidateBps = [...new Set(feeTiers.map(t => t.feeBps))];
  for (const totalFeeBps of candidateBps) {
    const i = computeInputAmount(solAmount, totalFeeBps);
    if (i.lte(new BN(0))) {
      checked.push({ totalFeeBps, ok: false, reason: 'inputAmount==0' });
      continue;
    }

    if (Q_post.lte(i)) {
      checked.push({ totalFeeBps, ok: false, reason: 'postVirtualQuoteReserves <= inputAmount (invalid)' });
      continue;
    }

    const xCand = computeTokensReceivedFromPost(Q_post, T_post, i);
    if (xCand === null) {
      checked.push({ totalFeeBps, ok: false, reason: 'denominator<=0' });
      continue;
    }

    // final tokens received is min(xCand, realTokenReserves) if real present
    let finalTokens = xCand;
    if (realTokenReserves) {
      if (xCand.gte(realTokenReserves)) finalTokens = realTokenReserves;
    }

    const virtualTokenReserves_pre = T_post.add(finalTokens);
    const virtualQuoteReserves_pre = Q_post.sub(i);

    if (virtualTokenReserves_pre.lte(new BN(0)) || virtualQuoteReserves_pre.lte(new BN(0))) {
      checked.push({ totalFeeBps, ok: false, reason: 'pre reserves invalid' });
      continue;
    }

    const preMarketCap = computeMarketCap(virtualQuoteReserves_pre, mintSupply, virtualTokenReserves_pre);

    // Determine which fee tier would be chosen for this preMarketCap
    const selectedFee = pickFeeForMarketCap(feeTiers, preMarketCap);

    const ok = selectedFee === totalFeeBps;

    const entry = {
      totalFeeBps,
      inputAmount: i.toString(),
      tokensReceivedCandidate: xCand.toString(),
      finalTokens: finalTokens.toString(),
      virtualQuoteReserves_pre: virtualQuoteReserves_pre.toString(),
      virtualTokenReserves_pre: virtualTokenReserves_pre.toString(),
      preMarketCap: preMarketCap.toString(),
      selectedFee,
      ok,
    };
    checked.push(entry);
    if (ok) consistent.push(entry);
  }

  let status = 'INSUFFICIENT_DATA';
  if (consistent.length === 1) status = 'VALID';
  else if (consistent.length > 1) status = 'AMBIGUOUS';

  return { status, candidates: checked, consistent };
}

function usage() {
  console.log('Usage: node verifier.mjs <input.json>');
  console.log('Input JSON example keys: postVirtualQuoteReserves, postVirtualTokenReserves, solAmount, mintSupply, realTokenReserves (opt), feeTiers (array of { maxMarketCap:null|number, feeBps:number })');
}

async function main() {
  const arg = process.argv[2];
  if (!arg) { usage(); process.exit(1); }
  const raw = fs.readFileSync(arg, 'utf8');
  const event = JSON.parse(raw);
  const res = runVerification(event);
  console.log(JSON.stringify(res, null, 2));
}

if (import.meta.url === `file://${process.argv[1]}`) main();
