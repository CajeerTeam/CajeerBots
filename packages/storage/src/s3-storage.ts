import { createHash, createHmac } from 'node:crypto';
import type { StorageDriver } from './storage.js';

export interface S3StorageOptions {
  endpoint: string;
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export class S3StorageDriver implements StorageDriver {
  constructor(private readonly options: S3StorageOptions) {}

  async put(key: string, data: Uint8Array): Promise<void> {
    await this.request('PUT', key, data);
  }

  async get(key: string): Promise<Uint8Array> {
    const response = await this.request('GET', key);
    return new Uint8Array(await response.arrayBuffer());
  }

  async delete(key: string): Promise<void> {
    await this.request('DELETE', key);
  }

  private async request(method: string, key: string, body?: Uint8Array): Promise<Response> {
    const url = new URL(`${this.options.endpoint.replace(/\/+$/, '')}/${encodeURIComponent(this.options.bucket)}/${key.split('/').map(encodeURIComponent).join('/')}`);
    const payloadHash = sha256Hex(body ?? new Uint8Array());
    const now = new Date();
    const amzDate = toAmzDate(now);
    const dateStamp = amzDate.slice(0, 8);
    const headers: Record<string, string> = {
      host: url.host,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate,
    };
    const signedHeaders = Object.keys(headers).sort().join(';');
    const canonicalHeaders = Object.keys(headers).sort().map((name) => `${name}:${headers[name]}\n`).join('');
    const canonicalRequest = [method, url.pathname, url.search.slice(1), canonicalHeaders, signedHeaders, payloadHash].join('\n');
    const scope = `${dateStamp}/${this.options.region}/s3/aws4_request`;
    const stringToSign = ['AWS4-HMAC-SHA256', amzDate, scope, sha256Hex(canonicalRequest)].join('\n');
    const signature = hmacHex(signingKey(this.options.secretAccessKey, dateStamp, this.options.region), stringToSign);
    headers['authorization'] = `AWS4-HMAC-SHA256 Credential=${this.options.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

    const response = await fetch(url, { method, headers, body });
    if (!response.ok) throw new Error(`S3 ${method} ${key} failed: ${response.status} ${await response.text()}`);
    return response;
  }
}

function toAmzDate(date: Date): string {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, '');
}

function sha256Hex(input: string | Uint8Array): string {
  return createHash('sha256').update(input).digest('hex');
}

function hmac(key: Buffer | string, input: string): Buffer {
  return createHmac('sha256', key).update(input).digest();
}

function hmacHex(key: Buffer | string, input: string): string {
  return createHmac('sha256', key).update(input).digest('hex');
}

function signingKey(secret: string, dateStamp: string, region: string): Buffer {
  const kDate = hmac(`AWS4${secret}`, dateStamp);
  const kRegion = hmac(kDate, region);
  const kService = hmac(kRegion, 's3');
  return hmac(kService, 'aws4_request');
}
