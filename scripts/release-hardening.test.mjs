import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync } from 'node:crypto';
import { mkdtemp, readFile, writeFile, symlink, rm, mkdir, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { artifactFingerprint, manifestDigest, signManifest, verifyReleaseFiles, validateManifest } from './release-artifacts.mjs';
import { encryptSecret, decryptSecret } from '../packages/security/dist/secret-crypto.js';
import { hashPassword, verifyPassword, hashPasswordAsync, verifyPasswordAsync } from '../packages/security/dist/passwords.js';
import { validBotTokenRef } from '../packages/security/dist/bot-token-ref.js';
import { resolveIntegrationUrl, runIntegrationTestCase } from '../packages/integration-tests/dist/runner.js';
import { buildReleaseManifest, verifyReleaseManifest } from '../packages/deployment/dist/release-manifest.js';

const keys = generateKeyPairSync('ed25519');
const pem = keys.privateKey.export({ format:'pem', type:'pkcs8' });
const pub = keys.publicKey.export({ format:'pem', type:'spki' });
const version = '1.0.0-rc.6';
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(),'rc6-test-')); t.after(() => rm(dir,{recursive:true,force:true}));
  await writeFile(join(dir,'release.zip'),'artifact content');
  const manifest = {schema:'cajeerbots.release.manifest.v1',product:'cajeerbots',version,channel:'rc',commit:'a'.repeat(40),files:[await artifactFingerprint(dir,'release.zip')]};
  manifest.digest=manifestDigest(manifest);
  const bytes=Buffer.from(JSON.stringify(manifest)); await writeFile(join(dir,'manifest.json'),bytes); await writeFile(join(dir,'manifest.sig'),signManifest(bytes,pem));
  return {dir,manifest,verify:()=>verifyReleaseFiles(dir,'manifest.json','manifest.sig',pub,version)};
}
test('release verification checks signature and actual artifact bytes',async t=>{const f=await fixture(t);assert.deepEqual(await f.verify(),f.manifest);await writeFile(join(f.dir,'release.zip'),'tampered');await assert.rejects(f.verify(),/Artifact mismatch/);});
test('modified manifest and wrong key rejected before artifact reads',async t=>{const f=await fixture(t); const other=generateKeyPairSync('ed25519').publicKey.export({format:'pem',type:'spki'});await assert.rejects(verifyReleaseFiles(f.dir,'manifest.json','manifest.sig',other,version),/signature invalid/);await writeFile(join(f.dir,'manifest.json'),JSON.stringify({...f.manifest,version:'1.0.0'}));await assert.rejects(f.verify(),/signature invalid/);});
test('reject duplicate, unsafe paths, forged digest and wrong version',async t=>{const f=await fixture(t);for(const path of ['../evil','/etc/passwd','sub/file','release.ZIP']){const m={...f.manifest,files:[...f.manifest.files,{...f.manifest.files[0],path}]};m.digest=manifestDigest(m);assert.throws(()=>validateManifest(m));}assert.throws(()=>validateManifest({...f.manifest,digest:'0'.repeat(64)}));await assert.rejects(verifyReleaseFiles(f.dir,'manifest.json','manifest.sig',pub,'1.0.0'),/Unexpected release version/);});
test('artifact symlink is rejected',async t=>{const f=await fixture(t);await rm(join(f.dir,'release.zip'));await symlink(join(f.dir,'manifest.json'),join(f.dir,'release.zip'));await assert.rejects(f.verify(),/regular file/);});
test('GCM requires 96-bit IV and full 128-bit tag',()=>{const key='a'.repeat(48);const encrypted=encryptSecret('private',key);assert.equal(decryptSecret(encrypted,key),'private');assert.throws(()=>decryptSecret({...encrypted,authTag:Buffer.from(encrypted.authTag,'base64url').subarray(0,12).toString('base64url')},key));assert.throws(()=>decryptSecret({...encrypted,iv:'YQ'},key));assert.throws(()=>decryptSecret({...encrypted,ciphertext:'YQ'},key));});
test('async password hashing remains compatible and malformed costs rejected',async()=>{const opts={pepper:'pepper'};const hash=hashPassword('strong-password',opts);assert(await verifyPasswordAsync('strong-password',hash,opts));const newer=await hashPasswordAsync('strong-password',opts);assert(verifyPassword('strong-password',newer,opts));assert.equal(await verifyPasswordAsync('strong-password',newer,{pepper:'wrong'}),false);assert.equal(await verifyPasswordAsync('strong-password',newer.replace('$210000$','$999999999$'),opts),false);assert.equal(verifyPassword('strong-password',newer.replace('$210000$','$999999999$'),opts),false);assert.equal(await verifyPasswordAsync('x'.repeat(513),newer,opts),false);});
test('provider token reference cannot resolve app credentials or another provider',()=>{for(const ref of ['env:JWT_SECRET','env:CAJEER_ADMIN_TOKEN','env:DATABASE_URL','env:VK_BOT_TOKEN','raw-token','env:TELEGRAM_BOT_TOKEN_','secret:../key'])assert(!validBotTokenRef('telegram',ref));assert(validBotTokenRef('telegram','env:TELEGRAM_BOT_TOKEN'));assert(validBotTokenRef('telegram','env:TELEGRAM_BOT_TOKEN_SECOND'));assert(validBotTokenRef('discord','secret:bot.community.discord.main','community'));assert(!validBotTokenRef('discord','secret:bot.other.discord.main','community'));assert(!validBotTokenRef('discord','secret:jwt-secret','community'));});
test('integration request restricted to configured origin and API path',()=>{const base='http://127.0.0.1:3000';assert.equal(resolveIntegrationUrl({path:'/api/health'},base).href,base+'/api/health');for(const request of [{url:'http://169.254.169.254/api/x'},{path:'//evil.test/api/x'},{baseUrl:'http://evil.test'},{path:'/api/../private'},{url:'http://user@127.0.0.1:3000/api/x'},{url:'http://127.0.0.1:3000/api/x#fragment'}])assert.throws(()=>resolveIntegrationUrl(request,base));});
test('integration dry run does not execute mutating steps or claim pass',async()=>{const mutations=[];const prisma={integrationTestRun:{create:async()=>({id:'run'}),update:async ({data})=>data},integrationTestStepRun:{create:async({data})=>mutations.push(data)},integrationTestCase:{update:async()=>{}},eventTopic:{upsert:async()=>assert.fail('dry-run mutated event topic')}};const result=await runIntegrationTestCase({prisma,dryRun:true,testCase:{id:'case',suiteId:'suite',definition:{steps:[{kind:'event_publish',request:{headers:{authorization:'secret'}}}]}}});assert.equal(result.summary.status,'skipped');assert.equal(mutations[0].status,'skipped');assert(!JSON.stringify(mutations).includes('secret'));});
test('deployment manifest checks digest and unsafe paths',()=>{const file={path:'artifact.zip',sizeBytes:42,sha256:'a'.repeat(64)};const manifest=buildReleaseManifest({product:'cajeerbots',version,channel:'rc',files:[file]});assert(verifyReleaseManifest(manifest).valid);assert(!verifyReleaseManifest({...manifest,files:[{...file,sizeBytes:43}]}).valid);assert.throws(()=>buildReleaseManifest({product:'cajeerbots',version,channel:'rc',files:[{...file,path:'../secret'}]}));});
test('release packager archives committed inputs, excludes local secrets, signs and verifies',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'rc6-pack-'));t.after(()=>rm(dir,{recursive:true,force:true}));await mkdir(join(dir,'scripts'));await mkdir(join(dir,'packages','sdk','src','generated'),{recursive:true});
  for(const name of ['release-dist.mjs','release-artifacts.mjs','release-verify.mjs'])await copyFile(new URL(name,import.meta.url),join(dir,'scripts',name));
  for(const [name,data] of Object.entries({'VERSION':version,'package.json':JSON.stringify({version}),'pnpm-lock.yaml':'fixture-only lockfile\n','.env.production.example':'EXAMPLE=true','docker-compose.prod.yml':'services: {}','packages/sdk/src/generated/contract.ts':'export const fixture = true;'}))await writeFile(join(dir,name),data);
  const git=(...args)=>{const r=spawnSync('git',args,{cwd:dir,encoding:'utf8'});assert.equal(r.status,0,r.stderr);return r.stdout;};git('init','-q');git('add','.');git('-c','user.name=Test','-c','user.email=test@example.org','commit','-qm','fixture');await writeFile(join(dir,'.env.production'),'PRIVATE=not-for-release');await writeFile(join(dir,'scripts','untracked.txt'),'not-for-release');const key=join(dir,'test-key.pem');await writeFile(key,pem);
  const run=spawnSync(process.execPath,['scripts/release-dist.mjs'],{cwd:dir,env:{...process.env,RELEASE_SIGNING_KEY_FILE:key,RELEASE_REQUIRE_EVIDENCE:'0'},encoding:'utf8'});assert.equal(run.status,0,run.stderr);const out=join(dir,'dist-release',`cajeerbots-${version}`);const m=await verifyReleaseFiles(out,`cajeerbots-${version}.manifest.json`,`cajeerbots-${version}.manifest.sig`,pub,version);assert.equal(m.files.length,1);const zip=spawnSync('unzip',['-Z1',join(out,m.files[0].path)],{encoding:'utf8'});assert.equal(zip.status,0);assert(zip.stdout.includes('packages/sdk/src/generated/contract.ts'));assert(!zip.stdout.includes('untracked.txt'));assert(!zip.stdout.includes('/.env.production\n'));assert(!zip.stdout.includes('test-key.pem'));
});

test('CycloneDX inventory includes transitive npm, workspace, Node and image OS dependencies', async t => {
  const {buildSbom}=await import('./release-sbom.mjs');
  const dir=await mkdtemp(join(tmpdir(),'rc6-sbom-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const workspace=join(dir,'packages','a');await mkdir(workspace,{recursive:true});await writeFile(join(workspace,'package.json'),JSON.stringify({name:'@cajeerbots/a',version}));
  const tree=[{name:'cajeerbots',version,dependencies:{'@cajeerbots/a':{version:'link:packages/a',path:workspace,dependencies:{'fastify':{version:'5.6.2',dependencies:{'foo':{version:'1.0.0'}}}}}}}];
  const bom=await buildSbom({tree,osPackages:'libc6\t2.36-9\tamd64\n',nodeVersion:'22.16.0',version,root:dir});
  assert.equal(bom.bomFormat,'CycloneDX');assert.equal(bom.specVersion,'1.5');assert(bom.components.some(c=>c.name==='@cajeerbots/a'));assert(bom.components.some(c=>c.name==='foo'));assert(bom.components.some(c=>c.name==='libc6'));assert(bom.components.some(c=>c.name==='nodejs'));assert(bom.dependencies.some(d=>d.ref==='pkg:npm/fastify@5.6.2'&&d.dependsOn.includes('pkg:npm/foo@1.0.0')));assert(!JSON.stringify(bom).includes(dir));
});

test('generated Nginx config forwards signed webhook path and rejects directive injection', async () => {
  const {renderNginxConfig}=await import('../packages/deployment/dist/nginx.js');
  const config=renderNginxConfig({serverName:'bots.example.org',enableTls:true});
  assert.match(config,/location \/runtime\/webhooks\/ \{\s+client_max_body_size 1m;\s+proxy_pass http:\/\/127\.0\.0\.1:3101;/);
  for(const options of [{serverName:'x; include /etc/passwd;'},{apiUpstream:'http://localhost; return 200'},{publicRoot:'/tmp/../etc'},{clientMaxBodySize:'1m; return 200'}])assert.throws(()=>renderNginxConfig({serverName:'bots.example.org',...options}));
});
