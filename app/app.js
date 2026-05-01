const out = document.querySelector('#output');
const token = document.querySelector('#token');
async function load(path, asText=false){
  out.textContent = 'Загрузка...';
  const headers = {};
  if (token.value.trim()) headers.Authorization = `Bearer ${token.value.trim()}`;
  const res = await fetch(path, {headers});
  const payload = asText ? await res.text() : await res.json();
  out.textContent = asText ? payload : JSON.stringify(payload, null, 2);
}
document.querySelectorAll('button[data-path]').forEach(btn => {
  btn.addEventListener('click', () => load(btn.dataset.path, btn.dataset.text === '1').catch(err => out.textContent = err.message));
});
