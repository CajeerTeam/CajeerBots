import { describe, expect, it } from 'vitest';
import { Localizer } from './localization.js';

describe('Localizer', () => {
  it('renders russian message with variables', () => {
    const localizer = new Localizer([{ locale: 'ru', messages: { hello: 'Привет, {{name}}' } }]);
    expect(localizer.t('ru', 'hello', { name: 'Cajeer' })).toBe('Привет, Cajeer');
  });
});
