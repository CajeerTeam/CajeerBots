import { decryptSecret } from '@cajeerbots/security';
import type { PrismaService } from '@cajeerbots/db';

export async function resolveTokenRef(prisma: PrismaService, tokenRef: string | null | undefined): Promise<string | undefined> {
  if (!tokenRef) return undefined;
  if (tokenRef.startsWith('env:')) return process.env[tokenRef.slice('env:'.length)];
  if (tokenRef.startsWith('secret:')) {
    const key = tokenRef.slice('secret:'.length);
    const secret = await prisma.secret.findUnique({ where: { key } });
    if (!secret) throw new Error(`Secret не найден для tokenRef=${tokenRef}`);
    const encryptionKey = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
    if (!encryptionKey) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан для secret tokenRef');
    return decryptSecret({ algorithm: secret.algorithm as 'aes-256-gcm', ciphertext: secret.ciphertext, iv: secret.iv, authTag: secret.authTag }, encryptionKey);
  }
  return tokenRef;
}
