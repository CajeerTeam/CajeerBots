import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { generateKeyPairSync, sign, createHash } from 'node:crypto';
import { createServer } from 'node:http';
const compiler = await import(process.env.TS_COMPILER_PATH ? pathToFileURL(process.env.TS_COMPILER_PATH).href : 'typescript');
const ts = compiler.default;
const dir = await mkdtemp(join(tmpdir(), 'cajeer-rc4-'));
after(() => rm(dir, { recursive: true, force: true }));
async function compile(source, name, replacements = {}) {
  let text = await readFile(new URL(`../${source}`, import.meta.url), 'utf8');
  for (const [a,b] of Object.entries(replacements)) text = text.replaceAll(a,b);
  const file = join(dir, name + '.mjs');
  await writeFile(file, ts.transpileModule(text, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText);
  return import(pathToFileURL(file).href);
}
const { validateBotForm } = await compile('packages/message-core/src/forms.ts','forms');
const { dispatchProductInteraction, validateConversationAnswer, parseAnswer } = await compile('apps/bot-runtime/src/runtime/product-interactions.ts','interactions',{'@cajeerbots/message-core':'./forms.mjs'});
const { verifyArtifactIntegrity } = await compile('packages/registry/src/integrity.ts','integrity');
const { resolveExecutable, resolveExtensionEntry } = await compile('packages/extension-runtime/src/executable.ts','executable');
const { CajeerClient } = await compile('packages/sdk/src/client.ts','sdk');
const account = { id:'bot1', communityId:'c1', provider:'telegram' };
const cmd = { id:'cmd', communityId:'c1', name:'form', handler:'form.start', metadata:{code:'survey'}, scopes:[], cooldownSeconds:0, isEnabled:true };
const form = { id:'f1', code:'survey', communityId:'c1', updatedAt:new Date('2026-01-01'), fields:[{name:'age',label:'Возраст',type:'number',required:true,min:18},{name:'agree',label:'Согласие',type:'boolean',required:true}], isActive:true };
function harness(){
 const states=new Map(), replay=new Map(), submissions=[], events=[];
 const k=(x)=>JSON.stringify([x.communityId,x.provider,x.userId,x.flow]);
 const tx={ $queryRaw:async()=>[1], idempotencyKey:{findUnique:async({where})=>replay.get(where.key),create:async({data})=>{replay.set(data.key,data);return data;}}, conversationState:{findUnique:async({where})=>states.get(k(where.communityId_provider_userId_flow)),upsert:async({create,update,where})=>{const key=k(where.communityId_provider_userId_flow);states.set(key,structuredClone(states.has(key)?{...states.get(key),...update}:create));},deleteMany:async({where})=>states.delete(k(where))}, community:{findUnique:async()=>({language:'ru'})}, interactionCommand:{findFirst:async({where})=>where.id===cmd.id?cmd:null}, interactionCallbackAction:{findUnique:async({where})=>where.communityId_code.code==='menu'?{id:'cb',isActive:true,handler:'menu.open',scopes:[],payload:{code:'home'}}:null,findFirst:async()=>null}, interactionMenu:{findFirst:async()=>({title:'Home',buttons:[]})}, messageFormDefinition:{findFirst:async({where})=>where.communityId==='c1'?form:null}, messageFormSubmission:{create:async({data})=>submissions.push(data)}, conversationFlow:{findFirst:async()=>null}, outboxMessage:{create:async({data})=>events.push(data)}, user:{findUnique:async()=>null} };
 const db={$transaction:async(fn)=>fn(tx)};
 const run=(type,name,payload={},command=null,id=undefined,target='chat1',userId='u1')=>dispatchProductInteraction(db,account,{type,name,payload,userId,provider:account.provider},command,target,id);
 return {run,states,submissions,replay,tx};
}
test('typed form rejects invalid boolean and number',()=>{assert.equal(validateBotForm(form.fields,{age:' ',agree:'maybe'}).ok,false);assert.deepEqual(validateBotForm(form.fields,{age:'21',agree:'нет'}).normalized,{age:21,agree:false});});
test('form completes once, replays final response and retains typed data',async()=>{const h=harness();assert.match((await h.run('command','form',{},cmd,'a')).text,/Возраст/);assert.match((await h.run('message','message',{text:'15'},null,'b')).text,/Минимум/);assert.match((await h.run('message','message',{text:'20'},null,'c')).text,/Согласие/);assert.match((await h.run('message','message',{text:'да'},null,'d')).text,/принята/);await h.run('message','message',{text:'да'},null,'d');assert.equal(h.submissions.length,1);assert.deepEqual(h.submissions[0].normalized,{age:20,agree:true});});
test('conversation state isolated by user and chat, cancel clears state',async()=>{const h=harness();await h.run('command','form',{},cmd,'a');assert.equal(await h.run('message','message',{text:'20'},null,'b','other'),undefined);assert.equal(await h.run('message','message',{text:'20'},null,'c','chat1','u2'),undefined);await h.run('command','cancel',{},null,'d');assert.equal(h.states.size,0);});
test('registered callback opens menu; unknown callback cannot dispatch fallback',async()=>{const h=harness();assert.equal((await h.run('callback','menu')).text,'Home');assert.equal(await h.run('callback','debug'),null);});
test('revoked origin prevents resuming form',async()=>{const h=harness();await h.run('command','form',{},cmd);h.tx.interactionCommand.findFirst=async()=>null;assert.match((await h.run('message','message',{text:'20'})).text,/отозван/);assert.equal(h.states.size,0);});
test('conversation validation rejects invalid choices and boolean',()=>{assert.equal(validateConversationAnswer({id:'x',input:'boolean'},'maybe').ok,false);assert.equal(validateConversationAnswer({id:'x',input:'choice',choices:[{label:'A',value:'a'}]},'b').ok,false);assert.deepEqual(validateConversationAnswer({id:'x',input:'number'},'42'),{ok:true,value:42});});
test('Discord answer extracts actual slash option',()=>assert.equal(parseAnswer({provider:'discord',type:'slash_command',name:'answer',userId:'x',payload:{options:[{name:'value',value:'42'}]}}),'42'));
test('Ed25519 integrity rejects modified bytes and incorrect keys',()=>{const {privateKey,publicKey}=generateKeyPairSync('ed25519');const bytes=Buffer.from('bundle');const input={bytes,sha256:createHash('sha256').update(bytes).digest('hex'),signature:sign(null,bytes,privateKey).toString('base64'),signatureAlgorithm:'ed25519',publicKey:publicKey.export({type:'spki',format:'pem'})};assert.equal(verifyArtifactIntegrity(input).ok,true);assert.equal(verifyArtifactIntegrity({...input,bytes:Buffer.from('tampered')}).ok,false);assert.equal(verifyArtifactIntegrity({...input,publicKey:'not a key'}).ok,false);});
test('verified executable runs actual ESM and rejects corrupted code',async()=>{const source='export default { handleEvent: async () => 42 }';const artifactCodeSha256=createHash('sha256').update(source).digest('hex');const manifest={name:'example',entry:'./index.js'};const url=await resolveExecutable({artifactSource:source,artifactCodeSha256},manifest);assert.equal(await (await import(url)).default.handleEvent(),42);await assert.rejects(resolveExecutable({artifactSource:source+' ',artifactCodeSha256},manifest),/integrity/);assert.throws(()=>resolveExtensionEntry('/tmp',{name:'example',entry:'../../bad.js'}),/Unsafe/);});
test('SDK rollback makes authenticated API call with explicit discard policy',async()=>{const server=createServer(async(req,res)=>{let body='';for await(const c of req)body+=c;assert.equal(req.url,'/api/updates/plans/p1/rollback');assert.equal(req.headers['x-api-key'],'token');assert.deepEqual(JSON.parse(body),{discardPending:true});res.setHeader('content-type','application/json');res.end('{"status":"rolled_back"}');});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));try{const c=new CajeerClient({baseUrl:`http://127.0.0.1:${server.address().port}`,apiKey:'token'});assert.equal((await c.rollbackUpdatePlan('p1',true)).status,'rolled_back');}finally{await new Promise(resolve=>server.close(resolve));}});
