var fs = require('fs');
var code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

code = code.replace('mintSupply: mintState.supply instanceof BN ? mintState.supply : new BN(mintState.supply.toString()),', 'mintSupply: (typeof mintState.supply === \"bigint\" ? new BN(mintState.supply.toString()) : mintState.supply),');

code = code.replace('tokenTotalSupply: mintState.supply instanceof BN ? mintState.supply : new BN(mintState.supply.toString()),', 'tokenTotalSupply: (typeof mintState.supply === \"bigint\" ? new BN(mintState.supply.toString()) : mintState.supply),');

fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', code);
console.log('Fixed instanceof checks');