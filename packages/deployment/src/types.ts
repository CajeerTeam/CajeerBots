export type DeploymentTargetKind = 'docker_compose' | 'systemd' | 'custom';
export type DeploymentCheckStatus = 'pass' | 'warn' | 'fail';

export interface DeploymentTargetInput {
  code: string;
  title: string;
  kind?: DeploymentTargetKind;
  environment?: string;
  baseUrl?: string | null;
  composeFiles?: string[];
  envFile?: string | null;
  metadata?: Record<string, unknown>;
}

export interface DeploymentCheckResult {
  code: string;
  status: DeploymentCheckStatus;
  severity: 'info' | 'warning' | 'critical';
  message: string;
  details?: Record<string, unknown>;
}

export interface ProductionEnvOptions {
  appUrl: string;
  databaseUrl: string;
  redisUrl: string;
  jwtSecret: string;
  adminToken: string;
  encryptionKey: string;
  botWebhookPublicUrl?: string;
  storageDriver?: 'local' | 's3';
  s3Endpoint?: string;
  s3Bucket?: string;
}

export interface NginxConfigOptions {
  serverName: string;
  publicRoot?: string;
  apiUpstream?: string;
  botWebhookUpstream?: string;
  enableTls?: boolean;
  certificatePath?: string;
  certificateKeyPath?: string;
  clientMaxBodySize?: string;
}

export interface SystemdUnitOptions {
  name: string;
  description: string;
  workingDirectory: string;
  command: string;
  user?: string;
  environmentFile?: string;
  restartSec?: number;
}

export interface ReleaseFileInput {
  path: string;
  sizeBytes: number;
  sha256: string;
  contentType?: string;
}

export interface ReleaseManifestInput {
  product: string;
  version: string;
  channel: 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security';
  commit?: string;
  createdBy?: string;
  files: ReleaseFileInput[];
}
