export type MessageProvider = 'telegram' | 'vk' | 'discord';

export interface MessageButton {
  id: string;
  label: string;
  action: string;
  payload?: Record<string, unknown>;
  url?: string;
}

export type ButtonRows = MessageButton[][];

export interface ButtonLayout {
  rows: ButtonRows;
}

export function normalizeButtonRows(input: unknown): ButtonRows {
  const rows = Array.isArray(input) ? input : isRecord(input) && Array.isArray(input.rows) ? input.rows : [];
  return rows.map((row) => Array.isArray(row) ? row.map(normalizeButton).filter(Boolean) as MessageButton[] : []).filter((row) => row.length > 0);
}

export function renderProviderButtons(provider: MessageProvider, input: unknown): unknown {
  const rows = normalizeButtonRows(input);
  if (rows.length === 0) return undefined;
  if (provider === 'telegram') {
    return {
      inline_keyboard: rows.map((row) => row.map((button) => button.url
        ? { text: button.label, url: button.url }
        : { text: button.label, callback_data: button.action })),
    };
  }
  if (provider === 'vk') {
    return {
      one_time: false,
      inline: true,
      buttons: rows.map((row) => row.map((button) => ({
        action: button.url
          ? { type: 'open_link', label: button.label, link: button.url }
          : { type: 'callback', label: button.label, payload: JSON.stringify({ action: button.action, payload: button.payload ?? {} }) },
        color: 'secondary',
      }))),
    };
  }
  return rows.map((row) => ({
    type: 1,
    components: row.map((button) => button.url
      ? { type: 2, style: 5, label: button.label, url: button.url }
      : { type: 2, style: 2, label: button.label, custom_id: button.action }),
  }));
}

export function appendButtonLabels(text: string, input: unknown): string {
  const rows = normalizeButtonRows(input);
  if (rows.length === 0) return text;
  const labels = rows.map((row) => row.map((button) => `[${button.label}]`).join(' ')).join('\n');
  return `${text}\n\n${labels}`;
}

function normalizeButton(input: unknown): MessageButton | undefined {
  if (!isRecord(input)) return undefined;
  const id = String(input.id ?? input.action ?? input.label ?? '').trim();
  const label = String(input.label ?? '').trim();
  const action = String(input.action ?? id).trim();
  if (!id || !label || !action) return undefined;
  return { id, label, action, payload: isRecord(input.payload) ? input.payload : undefined, url: typeof input.url === 'string' ? input.url : undefined };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
