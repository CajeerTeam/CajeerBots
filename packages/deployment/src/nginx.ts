import type { NginxConfigOptions } from './types.js';

export function renderNginxConfig(options: NginxConfigOptions): string {
  const apiUpstream = options.apiUpstream ?? 'http://127.0.0.1:3000';
  const clientMaxBodySize = options.clientMaxBodySize ?? '20m';
  const publicRoot = options.publicRoot ?? '/var/www/cajeerbots/public';
  const tls = options.enableTls ? `\n  listen 443 ssl http2;\n  ssl_certificate ${options.certificatePath ?? '/etc/letsencrypt/live/cajeerbots/fullchain.pem'};\n  ssl_certificate_key ${options.certificateKeyPath ?? '/etc/letsencrypt/live/cajeerbots/privkey.pem'};\n` : '\n  listen 80;\n';

  return `server {${tls}  server_name ${options.serverName};\n  root ${publicRoot};\n  client_max_body_size ${clientMaxBodySize};\n\n  add_header X-Content-Type-Options nosniff always;\n  add_header X-Frame-Options DENY always;\n  add_header Referrer-Policy no-referrer-when-downgrade always;\n\n  location /health {\n    proxy_pass ${apiUpstream}/api/health;\n    proxy_set_header Host $host;\n    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n    proxy_set_header X-Forwarded-Proto $scheme;\n  }\n\n  location /api/ {\n    proxy_pass ${apiUpstream}/api/;\n    proxy_http_version 1.1;\n    proxy_set_header Host $host;\n    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n    proxy_set_header X-Forwarded-Proto $scheme;\n  }\n\n  # RC2: public provider webhook verification is pending RC3.\n  location /runtime/webhooks/ { return 404; }\n}\n`;
}
