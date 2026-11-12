import { createHash, createHmac, createPublicKey, verify as cryptoVerify } from 'node:crypto';
import { readFile } from 'node:fs/promises';

export interface VerifyArtifactInput {
  bytes: Uint8Array | Buffer;
  sha256: string;
  signature?: string | null;
  signatureAlgorithm?: 'ed25519' | 'sha256-hmac' | 'none' | string | null;
  publicKey?: string | null;
  hmacSecret?: string | null;
}

export function sha256Hex(bytes: Uint8Array | Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

export async function sha256File(path: string): Promise<string> {
  return sha256Hex(await readFile(path));
}

export function verifyArtifactIntegrity(input: VerifyArtifactInput): { ok: boolean; sha256: string; signatureOk: boolean; reasons: string[] } {
  const sha256 = sha256Hex(input.bytes);
  const reasons: string[] = [];
  if (sha256.toLowerCase() !== input.sha256.toLowerCase()) reasons.push(`sha256 mismatch: expected ${input.sha256}, got ${sha256}`);
  const signatureOk = verifySignature(input.bytes, input.signature, input.signatureAlgorithm, input.publicKey, input.hmacSecret);
  if (!signatureOk) reasons.push('signature verification failed');
  return { ok: reasons.length === 0, sha256, signatureOk, reasons };
}

export function verifySignature(bytes: Uint8Array | Buffer, signature?: string | null, algorithm?: string | null, publicKey?: string | null, hmacSecret?: string | null): boolean {
  if (!signature || !algorithm || algorithm === 'none') return true;
  if (algorithm === 'sha256-hmac') {
    if (!hmacSecret) return false;
    const expected = createHmac('sha256', hmacSecret).update(bytes).digest('base64');
    return expected === signature;
  }
  if (algorithm === 'ed25519') {
    if (!publicKey) return false;
    try {
      const key = createPublicKey(publicKey);
      const decoded = Buffer.from(signature, 'base64');
      return key.asymmetricKeyType === 'ed25519' && decoded.length === 64 && cryptoVerify(null, Buffer.from(bytes), key, decoded);
    } catch { return false; }
  }
  return false;
}
