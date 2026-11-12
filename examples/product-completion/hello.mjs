export default {
  async handleEvent(context, event) {
    const previous = await context.storage.get('demo', 'count');
    const count = (typeof previous === 'number' ? previous : 0) + 1;
    await context.storage.set('demo', 'count', count);
    return { count, hook: event.hook };
  }
};
