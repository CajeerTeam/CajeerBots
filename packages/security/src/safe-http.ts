import { lookup } from 'node:dns/promises';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { isIP } from 'node:net';

// Conservative public IPv4 policy: IPv6-only destinations are intentionally unsupported.
export function isPublicAddress(address: string): boolean {
  if (isIP(address) !== 4) return false;
  const [a = 0, b = 0, c = 0] = address.split('.').map(Number);
  return !(a === 0 || a === 10 || a === 127 || a >= 224 || (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) || (a === 192 && (b === 168 || b === 0 || (b === 88 && c === 99))) ||
    (a === 198 && (b === 18 || b === 19 || (b === 51 && c === 100))) || (a === 203 && b === 0 && c === 113));
}
export function validatePublicUrl(value: string): URL {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || (url.port && url.port !== (url.protocol === 'https:' ? '443' : '80')) || url.hostname === 'localhost' || url.hostname.endsWith('.localhost') || url.hostname.startsWith('[') || (isIP(url.hostname) && !isPublicAddress(url.hostname))) throw new Error('Unsafe outbound URL');
  return url;
}
export async function safeHttpRequest(value: string, options: { method: string; headers?: Record<string, string>; body?: string }): Promise<{ ok: boolean; status: number; text(): Promise<string> }> {
  const url = validatePublicUrl(value);
  if (!['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'].includes(options.method.toUpperCase())) throw new Error('Unsafe HTTP method');
  const addresses = await resolvePublicHost(url.hostname);
  if (!addresses.length || addresses.some(({ address }) => !isPublicAddress(address))) throw new Error('Unsafe outbound DNS address');
  const address = addresses[0]!.address;
  const headers = Object.fromEntries(Object.entries(options.headers ?? {}).filter(([name]) => !['host', 'connection', 'transfer-encoding', 'content-length', 'upgrade', 'proxy-authorization', 'proxy-connection', 'expect', 'te', 'trailer'].includes(name.toLowerCase())));
  if (options.body !== undefined) headers['content-length'] = String(Buffer.byteLength(options.body));
  return new Promise((resolve, reject) => {
    const request = (url.protocol === 'https:' ? httpsRequest : httpRequest)(url, {
      method: options.method, headers, agent: false, family: 4,
      lookup: (_hostname, _options, callback) => callback(null, address, 4),
    }, (response) => {
      let body = Buffer.alloc(0);
      let received = 0;
      response.on('data', (chunk: Buffer) => {
        received += chunk.length;
        if (received > 65536) { request.destroy(new Error('Outbound response too large')); return; }
        if (body.length < 4096) body = Buffer.concat([body, chunk.subarray(0, 4096 - body.length)]);
      });
      response.on('error', reject);
      response.on('aborted', () => reject(new Error('Outbound response aborted')));
      response.on('end', () => { const status = response.statusCode ?? 0; resolve({ status, ok: status >= 200 && status < 300, text: async () => body.toString('utf8') }); });
    });
    const timer = setTimeout(() => request.destroy(new Error('Outbound HTTP timeout')), 10_000);
    request.once('close', () => clearTimeout(timer));
    request.on('error', reject);
    request.end(options.body);
  });
}

async function resolvePublicHost(hostname: string) {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([lookup(hostname, { all: true, family: 4 }), new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error('Outbound DNS timeout')), 3000); })]);
  } finally { if (timer) clearTimeout(timer); }
}
