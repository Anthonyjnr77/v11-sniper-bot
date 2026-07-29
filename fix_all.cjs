const fs = require('fs');
const code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

let newCode = code
  .replace(/import BN from \"bn.js\";\\\\n/g, 'import BN from \"bn.js\";')
  .replace(/\\\\n\\\\r/g, '\n')
  .replace(/\\\\r/g, '\n');

fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', newCode);
console.log('Fixed all line endings');