import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { allPermissionScopes, permissionDescriptions, type Principal, type PermissionScope } from '@cajeerbots/security';

@Injectable()
export class RolesService {
  constructor(private readonly prisma: PrismaService) {}

  async listPermissions() {
    await this.ensureKnownPermissions();
    return this.prisma.permission.findMany({ orderBy: { scope: 'asc' } });
  }

  list(communityId: string) {
    return this.prisma.role.findMany({
      where: { communityId },
      orderBy: [{ priority: 'asc' }, { code: 'asc' }],
      include: { permissions: { include: { permission: true } }, users: true },
    });
  }

  async get(communityId: string, id: string) {
    const role = await this.prisma.role.findFirst({
      where: { id, communityId },
      include: { permissions: { include: { permission: true } } },
    });
    if (!role) throw new NotFoundException('Роль не найдена');
    return role;
  }

  async create(communityId: string, input: { code: string; title: string; priority: number; scopes: string[] }, actor: Principal) {
    await this.ensureKnownPermissions();
    try {
      const role = await this.prisma.role.create({ data: { communityId, code: input.code, title: input.title, priority: input.priority } });
      if (input.scopes.length > 0) await this.replacePermissions(role.id, input.scopes);
      await this.audit(actor, 'role.create', role.id, { communityId, code: input.code, scopes: input.scopes });
      return this.get(communityId, role.id);
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Роль с таким code уже существует');
      throw error;
    }
  }

  async setPermissions(communityId: string, roleId: string, scopes: string[], actor: Principal) {
    await this.get(communityId, roleId);
    await this.ensureKnownPermissions(scopes);
    await this.replacePermissions(roleId, scopes);
    await this.audit(actor, 'role.permissions.set', roleId, { communityId, scopes });
    return this.get(communityId, roleId);
  }

  private async ensureKnownPermissions(extraScopes: string[] = []) {
    const scopes = Array.from(new Set([...allPermissionScopes(), ...extraScopes]));
    for (const scope of scopes) {
      const description = permissionDescriptions[scope as PermissionScope] ?? `Пользовательский scope: ${scope}`;
      await this.prisma.permission.upsert({
        where: { scope },
        create: { scope, description },
        update: { description },
      });
    }
  }

  private async replacePermissions(roleId: string, scopes: string[]) {
    const permissions = await this.prisma.permission.findMany({ where: { scope: { in: scopes } } });
    await this.prisma.$transaction([
      this.prisma.rolePermission.deleteMany({ where: { roleId } }),
      ...permissions.map((permission) => this.prisma.rolePermission.create({ data: { roleId, permissionId: permission.id } })),
    ]);
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'role', resourceId, metadata: metadata as never } });
  }
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
