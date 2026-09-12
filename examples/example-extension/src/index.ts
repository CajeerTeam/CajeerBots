import type { CajeerExtension, ExtensionContext, ExtensionEventEnvelope } from '@cajeerbots/extension-sdk';

const extension: CajeerExtension = {
  activate(context: ExtensionContext) {
    context.logger.info('Example extension activated', { extension: context.manifest.name });
  },

  async handleEvent(context: ExtensionContext, event: ExtensionEventEnvelope) {
    const counter = await context.storage.get<number>('runtime', 'handledEvents') ?? 0;
    await context.storage.set('runtime', 'handledEvents', counter + 1);
    context.logger.info('Example extension handled event', { hook: event.hook, total: counter + 1 });

    if (event.hook === 'webhook.received') {
      await context.events.publish('extension.example.webhook_handled', {
        extension: context.manifest.name,
        sourceHook: event.hook,
        handledEvents: counter + 1,
      });
    }

    return { handled: true, hook: event.hook, handledEvents: counter + 1 };
  },
};

export default extension;
