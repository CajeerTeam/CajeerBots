import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaClient } from '../../../prisma/generated/client/index.js';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  private connected = false;

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.close();
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    await this.$connect();
    this.connected = true;
  }

  async close(): Promise<void> {
    if (!this.connected) return;
    await this.$disconnect();
    this.connected = false;
  }

  async health(): Promise<'ok'> {
    await this.$queryRaw`SELECT 1`;
    return 'ok';
  }
}
