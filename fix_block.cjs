const fs = require('fs');
const code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

const oldBlock = \
const solAmount = new BN(BUY_AMOUNT);
       // buyState.bondingCurve is already a BondingCurve object with all reserves
       // If we have cached reserves, create a BondingCurve from them (for fresh tokens)
       let bondingCurveData;
       if (cachedCurve) {
         bondingCurveData = {
           virtualTokenReserves: cachedCurve.virtualTokenReserves,
           virtualQuoteReserves: cachedCurve.virtualSolReserves,
           realTokenReserves: cachedCurve.realTokenReserves,
           realQuoteReserves: cachedCurve.realSolReserves,
           tokenTotalSupply: new BN(0),
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

const newBlock = \
const solAmount = new BN(BUY_AMOUNT);
       let bondingCurveData;
       if (cachedCurve) {
         bondingCurveData = {
           virtualTokenReserves: cachedCurve.virtualTokenReserves,
           virtualQuoteReserves: cachedCurve.virtualSolReserves,
           realTokenReserves: cachedCurve.realTokenReserves,
           realQuoteReserves: cachedCurve.realSolReserves,
           tokenTotalSupply: mintState.supply || new BN(0),
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

if (code.includes(oldBlock)) {
    const newCode = code.replace(oldBlock, newBlock);
    fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', newCode);
    console.log('Fixed bondingCurve block');
} else {
    console.log('Old block not found, searching...');
    const idx = code.indexOf('bondingCurveData = {');
    if (idx >= 0) {
        console.log('Found at idx', idx);
        console.log(code.substring(idx, idx + 800));
    }
}