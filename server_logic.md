# Lógica de Funcionamento do Servidor (C6 RPA)

Este documento detalha o comportamento exato do `Servidor.py` em relação ao agendamento, execução e recuperação de atrasos.

## 1. Inicialização e Ciclo de Vida
1.  **Start Importante**: Ao abrir, o servidor tenta conectar ao BigQuery imediatamente para baixar o histórico de hoje.
    *   *Objetivo*: Saber o que já rodou hoje para não rodar duplicado.
    *   *Arquivo de Cache*: Ele salva esse histórico em `%TEMP%\C6_RPA_EXEC\automacoes_exec.xlsx`.
2.  **Descoberta de Scripts**: A cada 60 segundos, ele varre a pasta `graciliano/automacoes` procurando novos arquivos `.py` e lendo o Excel central (`registro_automacoes.xlsx`) para atualizar as regras de horário (`CRON`).

## 2. Lógica de Agendamento (O Coração do Cron)
O servidor verifica a cada **1 segundo** se precisa disparar algo. A lógica é baseada em "Alvo vs Realizado".

### Como ele decide se deve rodar?
Ele calcula quantas vezes o script **DEVERIA** ter rodado até agora (`target_runs`) e subtrai quantas vezes ele **REALMENTE** rodou hoje (`actual_runs`).

#### Cenário A: Agendamento Fixo (Ex: "10, 14, 16")
Se são **10:15 da manhã**:
*   Horas de alvo que já passaram: `10`. (Total: 1 execução alvo).
*   Se o histórico diz que rodou 0 vezes:
    *   `Diferença = 1 - 0 = 1`. **Fila: Adiciona 1 execução.**
*   Se já rodou 1 vez:
    *   `Diferença = 1 - 1 = 0`. **Nada acontece.**

#### Cenário B: Agendamento "ALL" (Rodar Toda Hora)
Se são **10:15 da manhã**:
*   A regra "ALL" define que o alvo é `Hora Atual + 1`.
*   Ou seja, deveria ter rodado às 00h, 01h, 02h ... 09h, 10h. (Total: 11 execuções alvo).

### Exemplo Prático de Atraso (O que você perguntou)
**Situação**: O servidor estava desligado. Você abre ele às **10:00**. O script é **ALL**.
1.  **10:00:00**: O servidor liga e vê no BigQuery que rodou **4 vezes** hoje (digamos, às 00h, 01h, 02h, 03h).
2.  **Cálculo**:
    *   Alvo (até as 10h): 11 execuções.
    *   Realizado: 4 execuções.
    *   **Pendente**: `11 - 4 = 7` execuções atrasadas.
3.  **Execução**:
    *   Ele **NÃO** roda as 7 de uma vez (para não travar o PC).
    *   Ele coloca **1** na fila.
    *   Assim que essa acabar, o contador sobe para 5.
    *   Na próxima verificação (1 seg depois), ele vê: `Alvo 11 - Realizado 5 = 6`. Coloca mais **1** na fila.
    *   Ele vai repetindo isso ("Catch-up") até empatar.

## 3. Prioridade e Concorrência
*   **Limite**: O servidor roda no máximo **5 scripts** ao mesmo tempo (`self.max_concurrent = 5`).
*   **Fila**: Se tiver 10 scripts atrasados, ele roda os 5 primeiros e deixa os outros 5 esperando alguém terminar.

## 4. BigQuery e Segurança
*   **Verificação de Saúde**: Antes de disparar qualquer coisa, ele checa se o BigQuery está respondendo. Se a conexão cair, ele **PAUSA** novos agendamentos para evitar rodar algo e não conseguir registrar o log depois.
*   **Sync**: A cada 10 minutos ele baixa o BigQuery de novo para garantir que se outro computador rodou um script, este servidor aqui saiba e não rode duplicado.

---
**Resumo da verificação**: O servidor está seguindo estritamente esta lógica no código atual (`Servidor.py` -> `check_schedule_logic`).
