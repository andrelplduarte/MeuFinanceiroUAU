# Validação Rápida (sem esperar lote completo)

Use esta estrutura para testar ajustes em minutos.

## 1) Onde colocar arquivos

- `tests_rapidos/entrada/receber/*.txt`
- `tests_rapidos/entrada/recebidos/*.txt`
- `tests_rapidos/entrada/estoque/*.txt` (opcional)

Sugestão: use 1 par pequeno por empreendimento para validar layout/regra.

## 2) Comando para rodar

No diretório do projeto:

```powershell
python scripts/rodar_validacao_rapida.py
```

## 3) Saída

Cada execução grava em:

- `tests_rapidos/saida/YYYYMMDD_HHMMSS/`

Com:

- `CARTEIRAS GERAL.xlsx`
- `CARTEIRAS BANCO DE DADOS.xlsx` (quando aplicável)

## 4) Fluxo recomendado

1. Ajustou código
2. Rodou validação rápida
3. Conferiu arquivo de saída
4. Só depois roda lote completo
