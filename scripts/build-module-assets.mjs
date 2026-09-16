/** Build optional vault assets from the preserved Node implementation.
 * Prerequisite: npm ci in mcp/servers/vault; this is a build command, never
 * invoked by runtime startup or module resolution.
 */
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import fs from 'node:fs/promises';
import {createHash} from 'node:crypto';
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = path.join(root, 'packages/runtime/src/openagent_core/mcp/servers/vault');
const dest = path.join(root, 'packages/modules/src/openagent_modules/resources/vault');
const require = createRequire(path.join(source, 'package.json'));
const {build} = require('esbuild');
await fs.mkdir(path.join(dest, 'dist'), {recursive:true});
const result = await build({
  entryPoints: [path.join(source, 'server.ts')], outfile:path.join(dest, 'dist/server.mjs'),
  bundle:true, platform:'node', target:'node20', format:'esm', metafile:true,
  banner:{js:'import { createRequire as __oaCreateRequire } from "node:module"; const require = __oaCreateRequire(import.meta.url);'},
  legalComments:'linked',
});
await fs.copyFile(path.join(source,'package.json'), path.join(dest,'package.json'));
for (const name of ['macos-trash', 'windows-trash.exe']) {
  await fs.copyFile(path.join(source,'node_modules/trash/lib',name),path.join(dest,'dist',name));
}
// Collect dependency notices from exactly the package roots reached by esbuild.
const packages = new Set([source]);
for (const file of Object.keys(result.metafile.inputs)) {
  let dir = path.dirname(path.resolve(root,file));
  while (dir.startsWith(source)) {
    try { await fs.access(path.join(dir,'package.json')); packages.add(dir); break; } catch {}
    dir=path.dirname(dir);
  }
}
const notices=[];
for (const dir of [...packages].sort()) {
  const pkg=JSON.parse(await fs.readFile(path.join(dir,'package.json'),'utf8'));
  const files=(await fs.readdir(dir)).filter(x=>/^(licen[sc]e|copying|notice)(\.|$)/i.test(x));
  notices.push(`\n=== ${pkg.name} ${pkg.version} (${pkg.license || 'see notice'}) ===\n`);
  for (const file of files) {
    if ((await fs.stat(path.join(dir,file))).isFile()) notices.push(await fs.readFile(path.join(dir,file),'utf8'));
  }
}
await fs.writeFile(path.join(dest,'THIRD_PARTY_NOTICES.txt'), notices.join('\n'));
const artifacts=[];
for (const file of ['dist/server.mjs','dist/server.mjs.LEGAL.txt','dist/macos-trash','dist/windows-trash.exe','package.json','THIRD_PARTY_NOTICES.txt']) {
  const bytes=await fs.readFile(path.join(dest,file));
  artifacts.push({file,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex')});
}
await fs.writeFile(path.join(dest,'manifest.json'),JSON.stringify({module:'vault',node:'>=20',source:'openagent_core/mcp/servers/vault',artifacts},null,2)+'\n');
console.log(`Built vault resource (${artifacts.reduce((a,x)=>a+x.bytes,0)} bytes) with ${packages.size} dependency notices`);
