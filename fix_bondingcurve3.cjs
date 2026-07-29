const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
let code = fs.readFileSync(file, 'utf8');

const oldCode = \const solAmount = new BN(BUY_AMOUNT);
     const tokenAmount = getBuyTokenAmountFromSolAmount({
       global,
       feeConfig,
       mintSupply: mintState.supply,
        bondingCurve: (cachedCurve?.bondingCurve ?? (buyState.bondingCurve instanceof PublicKey ? buyState.bondingCurve : new PublicKey(buyState.bondingCurve))) as PublicKey,
       amount: solAmount,
       quoteMint: new PublicKey(SOL_MINT),
     });\;

const newCode = \const solAmount = new BN(BUY_AMOUNT);
     // buyState.bondingCurve is already a BondingCurve object with all reserves
     // If we have cached reserves, create a BondingCurve from them (for fresh tokens)
     let bondingCurveData;
     if (cachedCurve) {
       bondingCurveData = {
         virtualTokenReserves: cachedCurve.virtualTokenReserves,
         virtualQuoteReserves: cachedCurve.virtualSolReserves,
         realTokenReserves: cachedCurve.realTokenReserves,
         realQuoteReserves: cachedCurve.realSolReserves,
         tokenTotalSupply: mintState.supply,
         complete: false,
         creator: PublicKey.default,
         isMayhemMode: false,
         isCashbackCoin: false,
         quoteMint: new PublicKey(SOL_MINT),
       };
     } else {
       bondingCurveData = buyState.bondingCurve;
     }
     const tokenAmount = getBuyTokenAmountFromSolAmount({
       global,
       feeConfig,
       mintSupply: mintState.supply,
       bondingCurve: bondingCurveData,
       amount: solAmount,
       quoteMint: new PublicKey(SOL_MINT),
     });\;

if (code.includes(oldCode)) {
    code = code.replace(oldCode, newCode);
    fs.writeFileSync(file, code);
    console.log('Fixed bondingCurve usage');
} else {
    console.log('Pattern not found exactly, trying variant...');
}