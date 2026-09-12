import { LocalStorageDriver, S3StorageDriver, type StorageDriver } from '@cajeerbots/storage';

export interface ConfiguredStorage {
  driverName: 'local' | 's3';
  bucket?: string;
  root?: string;
  driver: StorageDriver;
}

export function createConfiguredStorage(env: NodeJS.ProcessEnv = process.env): ConfiguredStorage {
  const driverName = (env.STORAGE_DRIVER ?? 'local') as 'local' | 's3';
  if (driverName === 's3') {
    const endpoint = required(env.S3_ENDPOINT, 'S3_ENDPOINT');
    const bucket = required(env.S3_BUCKET, 'S3_BUCKET');
    const accessKeyId = required(env.S3_ACCESS_KEY_ID, 'S3_ACCESS_KEY_ID');
    const secretAccessKey = required(env.S3_SECRET_ACCESS_KEY, 'S3_SECRET_ACCESS_KEY');
    return {
      driverName,
      bucket,
      driver: new S3StorageDriver({ endpoint, bucket, accessKeyId, secretAccessKey, region: env.S3_REGION ?? 'ru-1' }),
    };
  }
  const root = env.STORAGE_LOCAL_ROOT ?? './storage';
  return { driverName: 'local', root, driver: new LocalStorageDriver(root) };
}

function required(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} не задан для STORAGE_DRIVER=s3`);
  return value;
}
