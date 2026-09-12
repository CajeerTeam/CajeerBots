export async function restoreCommunitySnapshot(prisma: any, communityId: string, data: any, mode: 'dry-run' | 'apply') {
  const plan = {
    mode,
    settings: data.settings?.length ?? 0,
    roles: data.roles?.length ?? 0,
    users: data.users?.length ?? 0,
    botAccounts: data.botAccounts?.length ?? 0,
    messageTemplates: data.messageTemplates?.length ?? 0,
    interactionCommands: data.interactionCommands?.length ?? 0,
    interactionMenus: data.interactionMenus?.length ?? 0,
    callbackActions: data.callbackActions?.length ?? 0,
    conversationFlows: data.conversationFlows?.length ?? 0,
    localeCatalogs: data.localeCatalogs?.length ?? 0,
    buttonLayouts: data.buttonLayouts?.length ?? 0,
    messageForms: data.messageForms?.length ?? 0,
    eventTopics: data.eventTopics?.length ?? 0,
    eventSubscriptions: data.eventSubscriptions?.length ?? 0,
    botScenarios: data.botScenarios?.length ?? 0,
  };
  if (mode === 'dry-run') return { applied: false, plan };

  const community = await prisma.community.findUnique({ where: { id: communityId } });
  if (!community) throw new Error('Community не найдена');

  await prisma.$transaction(async (tx: any) => {
    if (data.community) await tx.community.update({ where: { id: communityId }, data: { title: data.community.title, language: data.community.language } });

    for (const item of data.settings ?? []) {
      await tx.setting.upsert({ where: { communityId_key: { communityId, key: item.key } }, create: { communityId, key: item.key, value: item.value }, update: { value: item.value } });
    }

    for (const item of data.roles ?? []) {
      const role = await tx.role.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, priority: item.priority }, update: { title: item.title, priority: item.priority } });
      for (const scope of item.permissions ?? []) {
        const permission = await tx.permission.upsert({ where: { scope }, create: { scope }, update: {} });
        await tx.rolePermission.upsert({ where: { roleId_permissionId: { roleId: role.id, permissionId: permission.id } }, create: { roleId: role.id, permissionId: permission.id }, update: {} });
      }
    }

    for (const item of data.users ?? []) {
      const user = await tx.user.upsert({ where: { communityId_provider_externalId: { communityId, provider: item.provider, externalId: item.externalId } }, create: { communityId, provider: item.provider, externalId: item.externalId, username: item.username, displayName: item.displayName, locale: item.locale ?? 'ru' }, update: { username: item.username, displayName: item.displayName, locale: item.locale ?? 'ru' } });
      for (const roleCode of item.roles ?? []) {
        const role = await tx.role.findUnique({ where: { communityId_code: { communityId, code: roleCode } } });
        if (role) await tx.userRole.upsert({ where: { userId_roleId: { userId: user.id, roleId: role.id } }, create: { userId: user.id, roleId: role.id }, update: {} });
      }
    }

    for (const item of data.botAccounts ?? []) {
      const existing = await tx.botAccount.findFirst({ where: { communityId, provider: item.provider, externalId: item.externalId ?? null } });
      if (existing) await tx.botAccount.update({ where: { id: existing.id }, data: { mode: item.mode, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive ?? true } });
      else await tx.botAccount.create({ data: { communityId, provider: item.provider, mode: item.mode, externalId: item.externalId, username: item.username, tokenRef: item.tokenRef, isActive: item.isActive ?? true } });
    }

    for (const item of data.messageTemplates ?? []) {
      await tx.messageTemplate.upsert({ where: { key_locale: { key: item.key, locale: item.locale } }, create: { key: item.key, locale: item.locale, body: item.body, variables: item.variables, buttons: item.buttons }, update: { body: item.body, variables: item.variables, buttons: item.buttons } });
    }

    for (const item of data.interactionCommands ?? []) {
      await tx.interactionCommand.upsert({ where: { communityId_provider_name: { communityId, provider: item.provider ?? null, name: item.name } }, create: { communityId, name: item.name, description: item.description, provider: item.provider ?? null, type: item.type ?? 'command', aliases: item.aliases ?? [], scopes: item.scopes ?? [], handler: item.handler ?? 'template', templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds ?? 0, isEnabled: item.isEnabled ?? true, metadata: item.metadata }, update: { description: item.description, type: item.type ?? 'command', aliases: item.aliases ?? [], scopes: item.scopes ?? [], handler: item.handler ?? 'template', templateKey: item.templateKey, conversationCode: item.conversationCode, cooldownSeconds: item.cooldownSeconds ?? 0, isEnabled: item.isEnabled ?? true, metadata: item.metadata } });
    }

    for (const item of data.interactionMenus ?? []) {
      await tx.interactionMenu.upsert({ where: { communityId_code_locale: { communityId, code: item.code, locale: item.locale ?? 'ru' } }, create: { communityId, code: item.code, title: item.title, locale: item.locale ?? 'ru', templateKey: item.templateKey, buttons: item.buttons, isActive: item.isActive ?? true }, update: { title: item.title, templateKey: item.templateKey, buttons: item.buttons, isActive: item.isActive ?? true } });
    }

    for (const item of data.callbackActions ?? []) {
      await tx.interactionCallbackAction.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, handler: item.handler ?? 'menu.open', scopes: item.scopes ?? [], payload: item.payload, isActive: item.isActive ?? true }, update: { title: item.title, handler: item.handler ?? 'menu.open', scopes: item.scopes ?? [], payload: item.payload, isActive: item.isActive ?? true } });
    }

    for (const item of data.conversationFlows ?? []) {
      await tx.conversationFlow.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, entryCommand: item.entryCommand, steps: item.steps, isActive: item.isActive ?? true }, update: { title: item.title, entryCommand: item.entryCommand, steps: item.steps, isActive: item.isActive ?? true } });
    }

    for (const item of data.localeCatalogs ?? []) {
      await tx.localeCatalog.upsert({ where: { communityId_namespace_locale: { communityId, namespace: item.namespace, locale: item.locale } }, create: { communityId, namespace: item.namespace, locale: item.locale, fallbackLocale: item.fallbackLocale, messages: item.messages }, update: { fallbackLocale: item.fallbackLocale, messages: item.messages, version: { increment: 1 } } });
    }

    for (const item of data.buttonLayouts ?? []) {
      await tx.messageButtonLayout.upsert({ where: { communityId_code_provider_locale: { communityId, code: item.code, provider: item.provider ?? null, locale: item.locale } }, create: { communityId, code: item.code, provider: item.provider ?? null, locale: item.locale, rows: item.rows }, update: { rows: item.rows } });
    }

    for (const item of data.messageForms ?? []) {
      await tx.messageFormDefinition.upsert({ where: { communityId_code_locale: { communityId, code: item.code, locale: item.locale } }, create: { communityId, code: item.code, title: item.title, locale: item.locale, fields: item.fields, submitTemplateKey: item.submitTemplateKey, successTemplateKey: item.successTemplateKey, isActive: item.isActive ?? true }, update: { title: item.title, fields: item.fields, submitTemplateKey: item.submitTemplateKey, successTemplateKey: item.successTemplateKey, isActive: item.isActive ?? true } });
    }

    for (const item of data.eventTopics ?? []) {
      await tx.eventTopic.upsert({ where: { name: item.name }, create: { name: item.name, description: item.description, schema: item.schema, isActive: item.isActive ?? true, retentionDays: item.retentionDays ?? 30 }, update: { description: item.description, schema: item.schema, isActive: item.isActive ?? true, retentionDays: item.retentionDays ?? 30 } });
    }

    for (const item of data.eventSubscriptions ?? []) {
      await tx.eventSubscription.upsert({ where: { name: item.name }, create: { name: item.name, topicPattern: item.topicPattern, targetType: item.targetType, targetRef: item.targetRef, filters: item.filters, isActive: item.isActive ?? true, maxAttempts: item.maxAttempts ?? 8 }, update: { topicPattern: item.topicPattern, targetType: item.targetType, targetRef: item.targetRef, filters: item.filters, isActive: item.isActive ?? true, maxAttempts: item.maxAttempts ?? 8, disabledAt: item.isActive === false ? new Date() : null } });
    }

    for (const item of data.botScenarios ?? []) {
      await tx.botScenario.upsert({ where: { communityId_code: { communityId, code: item.code } }, create: { communityId, code: item.code, title: item.title, provider: item.provider ?? null, accountId: item.accountId, definition: item.definition, isActive: item.isActive ?? true }, update: { title: item.title, provider: item.provider ?? null, accountId: item.accountId, definition: item.definition, isActive: item.isActive ?? true } });
    }
  });

  return { applied: true, plan };
}
