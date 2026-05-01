const output = document.querySelector('#output');
const title = document.querySelector('#title');
const cards = document.querySelector('#cards');
const token = document.querySelector('#token');

const pages = {
  dashboard: { title: 'Панель', endpoints: ['/readyz', '/version', '/status/dependencies'] },
  adapters: { title: 'Адаптеры', endpoints: ['/adapters', '/adapter-status'] },
  modules: { title: 'Модули', endpoints: ['/modules', '/commands'] },
  plugins: { title: 'Плагины', endpoints: ['/plugins'] },
  rbac: { title: 'RBAC', endpoints: ['/commands'] },
  audit: { title: 'Audit', endpoints: ['/audit'] },
  delivery: { title: 'Доставка', endpoints: ['/worker-status', '/delivery/enqueue'] },
  'dead-letters': { title: 'Dead letters', endpoints: ['/dead-letters'] },
  events: { title: 'События', endpoints: ['/events', '/routes'] },
  settings: { title: 'Настройки', endpoints: ['/config/summary', '/openapi.json'] },
  updates: { title: 'Обновления', endpoints: ['/updates/status', '/updates/plan', '/updates/history'] },
};

function headers() {
  const value = token.value.trim();
  return value ? { Authorization: `Bearer ${value}` } : {};
}

async function getJson(path) {
  const response = await fetch(path, { headers: headers(), cache: 'no-store' });
  const text = await response.text();
  let body;
  try { body = JSON.parse(text); } catch { body = text; }
  return { path, status: response.status, body };
}

async function load(page = 'dashboard') {
  const spec = pages[page] || pages.dashboard;
  title.textContent = spec.title;
  output.textContent = 'Загрузка...';
  cards.innerHTML = '';
  const results = [];
  for (const endpoint of spec.endpoints.filter((item) => !item.includes('/delivery/enqueue'))) {
    try { results.push(await getJson(endpoint)); }
    catch (error) { results.push({ path: endpoint, status: 0, body: String(error) }); }
  }
  for (const item of results) {
    const card = document.createElement('article');
    card.innerHTML = `<strong>${item.path}</strong><span>${item.status}</span>`;
    cards.appendChild(card);
  }
  output.textContent = JSON.stringify(results, null, 2);
}

document.querySelectorAll('#nav button').forEach((button) => {
  button.addEventListener('click', () => load(button.dataset.page));
});
token.addEventListener('change', () => load());
load();
