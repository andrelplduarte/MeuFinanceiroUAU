# Deploy na Render

Esta aplicacao foi preparada para deploy web na Render com Flask + Gunicorn.

Existem dois blueprints no repositorio:

- `render.yaml`: versao gratis para teste
- `render-paid.yaml`: versao com disco persistente para uso mais estavel

## O que esta configurado

- `gunicorn` em `requirements.txt`
- `render.yaml` com:
  - runtime Python
  - start command para `app:app`
  - timeout longo para processamento de lote
  - healthcheck em `/healthz`
  - modo gratis sem disco persistente
- `APP_DATA_ROOT` no backend para gravar:
  - uploads
  - outputs
  - progresso

## Publicacao pela Render

1. Entre na Render e conecte o repositorio GitHub.
2. Escolha `Blueprint` ou `Web Service`.
3. Se usar o blueprint do repo:
   - para teste gratis, use `render.yaml`
   - para ambiente pago com persistencia, use `render-paid.yaml`
   - confirme a branch desejada
4. Aguarde o build e o primeiro deploy.

## URL publica

Depois do deploy, a Render gera uma URL `onrender.com` para acesso web.

## Persistencia de arquivos

### Blueprint gratis (`render.yaml`)

Os arquivos operacionais ficam em:

- `/tmp/meu-financeiro-uau/uploads`
- `/tmp/meu-financeiro-uau/outputs`
- `/tmp/meu-financeiro-uau/outputs/_progress`

Importante:

- como o deploy gratis usa filesystem efemero, uploads e arquivos gerados podem sumir quando o servico reiniciar, redeployar ou ficar ocioso e voltar
- use este modo apenas para teste e demonstracao

### Blueprint pago (`render-paid.yaml`)

Os arquivos operacionais ficam em:

- `/var/data/meu-financeiro-uau/uploads`
- `/var/data/meu-financeiro-uau/outputs`
- `/var/data/meu-financeiro-uau/outputs/_progress`

## Observacoes importantes

- O deploy gratis nao oferece disco persistente.
- O blueprint pago exige plano compativel com persistent disk.
- Como o sistema processa lotes longos, o `gunicorn` foi configurado com `timeout 3600`.
- O start command usa 2 workers e 2 threads para evitar travar monitoramento e polling durante execucoes longas.

## Healthcheck

Endpoint:

- `/healthz`

Resposta esperada:

- `ok: true`
- `app_env: production`
- `data_root: /tmp/meu-financeiro-uau` no modo gratis
- `data_root: /var/data/meu-financeiro-uau` no modo pago
