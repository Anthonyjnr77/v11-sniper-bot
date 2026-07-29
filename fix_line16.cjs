const fs = require('fs');
const code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

// The issue is that line 16 contains literal backslash-n characters
const fixed = code.replace('import BN from \"bn.js\";\\n', 'import BN from \"bn.js\";');

fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', fixed);
console.log('Fixed line 16');