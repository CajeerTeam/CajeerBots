import { TextDecoder, TextEncoder } from 'node:util';
import { byteLength, sha256Hex, stableSerialize } from './checksum.js';

export interface BackupBundle<TPayload = unknown> {
  kind: 'cajeerbots.backup.bundle';
  version: 1;
  format: 'cajeerbots.backup.bundle.v1';
  createdAt: string;
  scope: 'community' | 'system';
  resourceId?: string;
  title: string;
  manifest: {
    payloadSha256: string;
    payloadBytes: number;
    includedResources: string[];
    metadata?: Record<string, unknown>;
  };
  payload: TPayload;
}

export function createBackupBundle<TPayload>(input: {
  scope: 'community' | 'system';
  resourceId?: string;
  title: string;
  payload: TPayload;
  includedResources: string[];
  metadata?: Record<string, unknown>;
  now?: Date;
}): BackupBundle<TPayload> {
  const payloadJson = stableSerialize(input.payload);
  const manifest: BackupBundle<TPayload>['manifest'] = {
    payloadSha256: sha256Hex(payloadJson),
    payloadBytes: byteLength(payloadJson),
    includedResources: [...new Set(input.includedResources)].sort(),
  };
  if (input.metadata !== undefined) manifest.metadata = input.metadata;
  const bundle: BackupBundle<TPayload> = {
    kind: 'cajeerbots.backup.bundle',
    version: 1,
    format: 'cajeerbots.backup.bundle.v1',
    createdAt: (input.now ?? new Date()).toISOString(),
    scope: input.scope,
    title: input.title,
    manifest,
    payload: input.payload,
  };
  if (input.resourceId !== undefined) bundle.resourceId = input.resourceId;
  return bundle;
}

export function encodeBackupBundle(bundle: BackupBundle): Uint8Array {
  return new TextEncoder().encode(`${JSON.stringify(bundle, null, 2)}
`);
}

export function decodeBackupBundle(data: Uint8Array | string): BackupBundle {
  const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
  const parsed = JSON.parse(text) as BackupBundle;
  assertBackupBundle(parsed);
  return parsed;
}

export function verifyBackupBundle(bundle: BackupBundle): { valid: boolean; expectedSha256: string; actualSha256: string; payloadBytes: number } {
  const payloadJson = stableSerialize(bundle.payload);
  const actualSha256 = sha256Hex(payloadJson);
  return {
    valid: bundle.kind === 'cajeerbots.backup.bundle' && bundle.version === 1 && actualSha256 === bundle.manifest.payloadSha256 && byteLength(payloadJson) === bundle.manifest.payloadBytes,
    expectedSha256: bundle.manifest.payloadSha256,
    actualSha256,
    payloadBytes: byteLength(payloadJson),
  };
}

function assertBackupBundle(value: BackupBundle): void {
  if (!value || value.kind !== 'cajeerbots.backup.bundle' || value.version !== 1 || value.format !== 'cajeerbots.backup.bundle.v1') {
    throw new Error('Файл не является backup bundle CajeerBots v1');
  }
  const verification = verifyBackupBundle(value);
  if (!verification.valid) throw new Error(`Backup bundle повреждён: expected=${verification.expectedSha256}, actual=${verification.actualSha256}`);
}
