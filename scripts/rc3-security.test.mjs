import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { canAccessCommunity } from '../packages/security/dist/community-access.js';
import { signJwt, verifyJwt } from '../packages/security/dist/tokens.js';
import { isPublicAddress, validatePublicUrl, safeHttpRequest } from '../packages/security/dist/safe-http.js';
import { authenticateWebhook } from '../apps/bot-runtime/dist/runtime/webhook-auth.js';
import { InteractionRouter } from '../packages/interaction-core/dist/interaction-router.js';
import { subscriptionAllowsEvent } from '../packages/event-bus/dist/event-api.js';

test('tenant wildcard cannot access a different tenant or global routes', () => {
  const principal = { id: 'x', type: 'api_key', scopes: ['community:a', '*', 'platform:global'] };
  assert.equal(canAccessCommunity(principal, 'a'), true);
  assert.equal(canAccessCommunity(principal, 'b'), false);
  assert.equal(canAccessCommunity(principal), false);
  assert.equal(canAccessCommunity({ ...principal, scopes: ['communities:read'] }, 'a'), false);
  assert.equal(canAccessCommunity({ ...principal, scopes: ['*'] }, 'b'), true);
});
test('JWT rejects empty key, tampering, expiration and extra segments', () => {
  const secret = 'unit-test-key';
  const { token } = signJwt({ sub: 'a', typ: 'user', scopes: ['*'], sid: 'session' }, secret);
  assert.equal(verifyJwt(token, secret).sid, 'session');
  assert.equal(verifyJwt(token, ''), null);
  assert.equal(verifyJwt(token + '.extra', secret), null);
  assert.equal(verifyJwt(token, 'wrong'), null);
  assert.equal(verifyJwt(signJwt({ sub: 'a', typ: 'system', scopes: ['*'] }, secret, -1).token, secret), null);
});
test('SSRF blocks local/private and alternate IP encodings', async () => {
  for (const value of ['http://localhost', 'http://127.1', 'http://2130706433', 'http://0x7f000001', 'http://169.254.169.254', 'http://10.0.0.1', 'http://192.168.0.1', 'http://100.64.0.1', 'http://[::1]', 'http://user@8.8.8.8', 'file:///etc/passwd', 'http://8.8.8.8:8080']) assert.throws(() => validatePublicUrl(value));
  assert.equal(isPublicAddress('8.8.8.8'), true);
  await assert.rejects(safeHttpRequest('http://127.0.0.1', { method: 'GET' }), /Unsafe/);
  await assert.rejects(safeHttpRequest('http://8.8.8.8', { method: 'CONNECT' }), /Unsafe/);
});
test('provider webhook authentication binds exact account and raw signed bytes', () => {
  const old = process.env.BOT_WEBHOOK_CREDENTIALS;
  const { privateKey, publicKey } = generateKeyPairSync('ed25519');
  const key = publicKey.export({ format: 'der', type: 'spki' }).subarray(-32).toString('hex');
  process.env.BOT_WEBHOOK_CREDENTIALS = JSON.stringify({ t: { secret: 'telegram-secret-123' }, v: { secret: 'vk-secret-123456', groupId: '123' }, d: { publicKey: key } });
  const account = (id, provider) => ({ id, provider, communityId: 'a', mode: 'webhook', token: 'unused' });
  const raw = Buffer.from('{"type":1, "id":"x"}');
  try {
    assert.equal(authenticateWebhook(account('t', 'telegram'), {'x-telegram-bot-api-secret-token': 'telegram-secret-123'}, raw, {}), true);
    assert.equal(authenticateWebhook(account('other', 'telegram'), {'x-telegram-bot-api-secret-token': 'telegram-secret-123'}, raw, {}), false);
    assert.equal(authenticateWebhook(account('v', 'vk'), {}, raw, {secret:'vk-secret-123456', group_id:123}), true);
    assert.equal(authenticateWebhook(account('v', 'vk'), {}, raw, {secret:'vk-secret-123456', group_id:456}), false);
    const timestamp = String(Math.floor(Date.now()/1000));
    const signature = sign(null, Buffer.concat([Buffer.from(timestamp),raw]), privateKey).toString('hex');
    const headers = {'x-signature-ed25519':signature,'x-signature-timestamp':timestamp};
    assert.equal(authenticateWebhook(account('d','discord'),headers,raw,{}),true);
    assert.equal(authenticateWebhook(account('d','discord'),headers,Buffer.from('{"type":1,"id":"x"}'),{}),false);
    assert.equal(authenticateWebhook(account('d','discord'),headers,raw,{},Date.now()+600000),false);
  } finally { if (old === undefined) delete process.env.BOT_WEBHOOK_CREDENTIALS; else process.env.BOT_WEBHOOK_CREDENTIALS=old; }
});
test('command router and subscription filters do not leak between communities', async () => {
  const router = new InteractionRouter();
  router.loadCommands([{id:'1',communityId:'a',name:'secret',description:'',type:'command',aliases:[],scopes:[],handler:'template',templateKey:'private',cooldownSeconds:0,isEnabled:true}]);
  const input = {provider:'telegram',type:'command',name:'secret',userId:'1',payload:{communityId:'b'}};
  assert.equal(await router.dispatch(input),undefined);
  assert.equal((await router.dispatch({...input,payload:{communityId:'a'}})).templateKey,'private');
  assert.equal(subscriptionAllowsEvent({}, {communityId:'a'}),false);
  assert.equal(subscriptionAllowsEvent({communityId:'a'},{communityId:'b'}),false);
  assert.equal(subscriptionAllowsEvent({communityId:'a'},{communityId:'a'}),true);
  assert.equal(subscriptionAllowsEvent({}, {communityId:null}),true);
});
