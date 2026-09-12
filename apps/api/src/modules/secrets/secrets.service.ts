import { Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { decryptSecret, encryptSecret, maskSecret, type EncryptedSecretPayload, type Principal } from '@cajeerbots/security';

interface SecretInput {
  key: string;
  value: string;
  description?: string;
  reason?: string;
}

@Injectable()
export class SecretsService {
  constructor(private readonly prisma: PrismaService) {}

  async list() {
    const secrets = await this.prisma.secret.findMany({
      orderBy: { key: 'asc' },
      select: { id: true, key: true, description: true, version: true, createdAt: true, updatedAt: true, rotatedAt: true },
    });
    return secrets.map((secret) => ({ ...secret, ref: `secret:${secret.key}` }));
  }

  async get(key: string) {
    const secret = await this.prisma.secret.findUnique({
      where: { key },
      select: { id: true, key: true, description: true, version: true, createdAt: true, updatedAt: true, rotatedAt: true },
    });
    if (!secret) throw new NotFoundException('Secret не найден');
    return { ...secret, ref: `secret:${secret.key}` };
  }

  async reveal(key: string, actor: Principal) {
    const secret = await this.prisma.secret.findUnique({ where: { key } });
    if (!secret) throw new NotFoundException('Secret не найден');
    const value = this.decrypt({ algorithm: secret.algorithm as 'aes-256-gcm', ciphertext: secret.ciphertext, iv: secret.iv, authTag: secret.authTag });
    await this.audit(actor, 'secret.reveal', secret.id, { key, masked: maskSecret(value) });
    return { key, value, masked: maskSecret(value), version: secret.version };
  }

  async upsert(input: SecretInput, actor: Principal) {
    const encrypted = this.encrypt(input.value);
    const existing = await this.prisma.secret.findUnique({ where: { key: input.key } });
    if (!existing) {
      const created = await this.prisma.secret.create({
        data: { key: input.key, description: input.description, ...encrypted },
        select: { id: true, key: true, description: true, version: true, createdAt: true, updatedAt: true, rotatedAt: true },
      });
      await this.audit(actor, 'secret.create', created.id, { key: created.key, reason: input.reason });
      return { ...created, ref: `secret:${created.key}` };
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.secretHistory.create({
        data: {
          secretId: existing.id,
          version: existing.version,
          algorithm: existing.algorithm,
          ciphertext: existing.ciphertext,
          iv: existing.iv,
          authTag: existing.authTag,
          actorId: actor.id,
          reason: input.reason,
        },
      });
      await tx.secret.update({
        where: { id: existing.id },
        data: { description: input.description ?? existing.description, ...encrypted, version: { increment: 1 }, rotatedAt: new Date() },
      });
    });

    const updated = await this.get(input.key);
    await this.audit(actor, 'secret.update', existing.id, { key: input.key, reason: input.reason });
    return updated;
  }

  rotate(key: string, input: Omit<SecretInput, 'key'>, actor: Principal) {
    return this.upsert({ key, ...input }, actor);
  }

  async remove(key: string, actor: Principal) {
    const existing = await this.prisma.secret.findUnique({ where: { key }, select: { id: true, key: true } });
    if (!existing) throw new NotFoundException('Secret не найден');
    await this.prisma.secret.delete({ where: { key } });
    await this.audit(actor, 'secret.delete', existing.id, { key });
    return { deleted: true, key };
  }

  async history(key: string) {
    const secret = await this.prisma.secret.findUnique({ where: { key }, include: { history: { orderBy: { createdAt: 'desc' } } } });
    if (!secret) throw new NotFoundException('Secret не найден');
    return secret.history.map((entry) => ({ id: entry.id, version: entry.version, actorId: entry.actorId, reason: entry.reason, createdAt: entry.createdAt }));
  }

  private encrypt(value: string) {
    const key = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
    if (!key) throw new BadRequestException('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан');
    return encryptSecret(value, key);
  }

  private decrypt(payload: EncryptedSecretPayload) {
    const key = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
    if (!key) throw new BadRequestException('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан');
    return decryptSecret(payload, key);
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'secret', resourceId, metadata: metadata as never } });
  }
}
