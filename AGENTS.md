# Regras para o agente (Codemax/Codex)

- Não execute `Servidor.py` (loop infinito/GUI). Faça análise estática e refactors seguros.
- Não acessar BigQuery/serviços externos; use mocks.
- Valide sempre com:
  - `python -m compileall -q .`
  - `ruff check .`
  - `pytest -q` (se houver testes)
- Não remover logs; padronizar/melhorar mensagens quando necessário.
- Evitar mudanças de comportamento produtivo sem justificativa clara.
