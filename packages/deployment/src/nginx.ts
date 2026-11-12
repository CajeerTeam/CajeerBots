import type { NginxConfigOptions } from './types.js';

function upstream(value: string): string {
  if (!/^https?:\/\/[A-Za-z0-9.:[\]-]+$/.test(value)) throw new Error('Invalid Nginx upstream');
  const url = new URL(value);
  if (url.username || url.password || url.pathname !== '/' || url.search || url.hash) throw new Error('Invalid Nginx upstream');
  return url.origin;
}
function filePath(value: string): string {
  if (!/^\/[A-Za-z0-9_./-]+$/.test(value) || value.split('/').includes('..')) throw new Error('Invalid Nginx path');
  return value;
}

export function renderNginxConfig(options: NginxConfigOptions): string {
  if (!/^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$/.test(options.serverName)) throw new Error('Invalid Nginx server name');
  const apiUpstream = upstream(options.apiUpstream ?? 'http://127.0.0.1:3000');
  const botUpstream = upstream(options.botWebhookUpstream ?? 'http://127.0.0.1:3101');
  const clientMaxBodySize = options.clientMaxBodySize ?? '20m';
  if (!/^[1-9]\d{0,4}[kKmM]?$/.test(clientMaxBodySize)) throw new Error('Invalid Nginx body size');
  const publicRoot = filePath(options.publicRoot ?? '/var/www/cajeerbots/public');
  const tls = options.enableTls ? `\n  listen 443 ssl http2;\n  ssl_certificate ${filePath(options.certificatePath ?? '/etc/letsencrypt/live/cajeerbots/fullchain.pem')};\n  ssl_certificate_key ${filePath(options.certificateKeyPath ?? '/etc/letsencrypt/live/cajeerbots/privkey.pem')};\n` : '\n  listen 80;\n';

  return `server {${tls}  server_name ${options.serverName};\n  root ${publicRoot};\n  client_max_body_size ${clientMaxBodySize};\n\n  add_header X-Content-Type-Options nosniff always;\n  add_header X-Frame-Options DENY always;\n  add_header Referrer-Policy no-referrer-when-downgrade always;\n\n  location /health {\n    proxy_pass ${apiUpstream}/api/health;\n    proxy_set_header Host $host;\n    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n    proxy_set_header X-Forwarded-Proto $scheme;\n  }\n\n  location /api/ {\n    proxy_pass ${apiUpstream}/api/;\n    proxy_http_version 1.1;\n    proxy_set_header Host $host;\n    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n    proxy_set_header X-Forwarded-Proto $scheme;\n  }\n\n  # Publish behind TLS with configured per-account webhook credentials.\n  location /runtime/webhooks/ {\n    client_max_body_size 1m;\n    proxy_pass ${botUpstream};\n    proxy_http_version 1.1;\n    proxy_set_header Host $host;\n    proxy_read_timeout 20s;\n  }\n}\n`;
}
