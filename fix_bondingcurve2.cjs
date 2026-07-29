const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
let code = fs.readFileSync(file, 'utf8');

// Fix the bondingCurve usage
const oldCode = 'const solAmount = new BN(BUY_AMOUNT);\n       const tokenAmount = getBuyTokenAmountFromSolAmount({\n         global,\n         feeConfig,\n         mintSupply: mintState.supply,\n          bondingCurve: (cachedCurve?.bondingCurve ?? buyState.bondingCurve),\n         amount: solAmount,\n         quoteMint: new PublicKey(SOL_MINT),\n       });';

const newCode = 'const solAmount = new BN(BUY_AMOUNT);\n       // buyState.bondingCurve is already a BondingCurve object with all reserves\n       // If we have cached reserves, create a BondingCurve from them (for fresh tokens)\n       let bondingCurveData;\n       if (cachedCurve) {\n         bondingCurveData = {\n           virtualTokenReserves: cachedCurve.virtualTokenReserves,\n           virtualQuoteReserves: cachedCurve.virtualSolReserves,\n           realTokenReserves: cachedCurve.realTokenReserves,\n           realQuoteReserves: cachedCurve.realSolReserves,\n           tokenTotalSupply: mintState.supply,\n           complete: false,\n           creator: PublicKey.default,\n           isMayhemMode: false,\n           isCashbackCoin: false,\n           quoteMint: new PublicKey(SOL_MINT),\n         };\n       } else {\n         bondingCurveData = buyState.bondingCurve;\n       }\n       const tokenAmount = getBuyTokenAmountFromSolAmount({\n         global,\n         feeConfig,\n         mintSupply: mintState.supply,\n         bondingCurve: bondingCurveData,\n         amount: solAmount,\n         quoteMint: new PublicKey(SOL_MINT),\n       });';

if (code.includes(oldCode)) {
    code = code.replace(oldCode, newCode);
    fs.writeFileSync(file, code);
    console.log('Fixed bondingCurve usage');
} else {
    console.log('Pattern not found, searching...');
    const idx = code.indexOf('bondingCurve: (cachedCurve');
    if (idx >= 0) {
        console.log('Found near:', idx, code.substring(idx, idx + 300));
    }
}