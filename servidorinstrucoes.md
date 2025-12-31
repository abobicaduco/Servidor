Especificação Técnica Completa: Servidor RPA (servidor.py)
Este documento detalha a lógica de execução, regras de negócio e fluxo de dados do orquestrador de automações servidor.py.
Estrutura de Pastas:
- servidor.py (Raiz)
- frontend/ (Pasta contendo index.html, CSS e assets da interface web)

1. Definição de Ambiente e Caminhos (Inicialização)
Ao iniciar, o script define os diretórios raiz e de configuração seguindo esta ordem de prioridade exata:
1.1. Detecção do Drive Raiz (ROOT_DRIVE)
O servidor itera sobre a lista POSSIBLE_ROOTS e define como ROOT_DRIVE o primeiro caminho que existir no sistema de arquivos:
# Define Root Path
HOME = Path.home()
POSSIBLE_ROOTS = [
    HOME / "C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A",
    HOME / "Meu Drive/C6 CTVM",
    HOME / "C6 CTVM",
]
ROOT_DRIVE = next((p for p in POSSIBLE_ROOTS if p.exists()), HOME / "C6 CTVM")
1.2. Diretório de Automações (BASE_PATH)
Dentro do ROOT_DRIVE definido acima, o servidor busca a pasta de automações:
Tenta: ROOT_DRIVE + Mensageria e Cargas Operacionais - 11.CelulaPython/graciliano/automacoes
Se não existir, usa: ROOT_DRIVE + graciliano/automacoes
1.3. Arquivos de Configuração e Cache
O servidor utiliza o diretório temporário do Windows para armazenar estados voláteis:
Diretório de Config: %TEMP%/C6_RPA_EXEC
Cache de Histórico (Excel): %TEMP%/C6_RPA_EXEC/automacoes_exec.xlsx
Arquivo Mestre de Registro (Leitura): O script busca um arquivo Excel chamado registro_automacoes.xlsx (ou nome definido na env EXCEL_FILENAME) localizado na mesma pasta onde o script servidor.py está salvo.
1.4. Credenciais Google Cloud
O script configura automaticamente a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS se ela não existir, buscando o primeiro arquivo .json encontrado no diretório:
%APPDATA%/Roaming/CELPY/*.json
2. Motor de Processamento (Classe EngineWorker)
O EngineWorker opera em uma Thread separada (QThread) para não travar a interface gráfica. O método run() executa um loop infinito com verificações a cada 1 segundo.
2.1. Verificação de Virada de Dia (Midnight Reset)
Frequência: Executado a cada iteração do loop (1 segundo). Lógica:
O servidor armazena a data de processamento atual em memória (self.current_processing_date formato YYYY-MM-DD).
Obtém a data atual do sistema (datetime.now()).
Condição: Se a Data Atual for diferente da Data de Processamento Armazenada:
Limpa completamente o dicionário de contagem de execuções (self.daily_execution_cache.clear()).
Reseta o DataFrame de histórico (self.history_df = pd.DataFrame()).
Define o timestamp da última sincronização do BigQuery (self.last_bq_sync) para 0 (forçando uma sincronização imediata na próxima etapa).
Atualiza self.current_processing_date para a nova data.
2.2. Descoberta de Scripts (Discovery)
Frequência: Executado a cada 60 segundos. Processo:
Scan de Arquivos: Percorre recursivamente (os.walk) o diretório BASE_PATH. Identifica arquivos .py que estão dentro de pastas chamadas metodos.
Leitura do Excel Mestre: Lê o arquivo registro_automacoes.xlsx.
Normaliza nomes de colunas para minúsculas.
Busca colunas chaves: script_name (ou similar), area, cron (schedule), active.
Cruzamento de Dados:
Filtra apenas linhas onde a coluna active contém: "true", "1", "verdadeiro", "sim", "s" ou "on".
Associa o caminho do arquivo físico ao nome lógico do script.
Parse do Cron: Lê a coluna cron_schedule:
Se valor for "ALL": Define agendamento horário toda do em todas as horas do dia.
Se valor for lista (ex: "8, 12, 18"): Converte para um conjunto de inteiros {8, 12, 18}. E nesse caso vai rodar nas horas estabelecidas do dia, ex: 1,13,14. Vai rodar o python quando for uma das manhã, treze da tarde e quatorze da tarde.
Se valor for "MANUAL", "SEM", ou vazio: Define como agendamento manual, ou seja, não roda automaticamente pelo servidor.
2.3. Sincronização com BigQuery (Source of Truth)
Frequência: Executado a cada 300 segundos (5 minutos), ou imediatamente após o Reset Diário. Query Executada:
SELECT script_name, status, start_time, duration_seconds, date
FROM `datalab-pagamentos.ADMINISTRACAO_CELULA_PYTHON.automacoes_exec`
WHERE DATE(start_time, 'America/Sao_Paulo') = CURRENT_DATE('America/Sao_Paulo')


Regra de Consolidação (Merge):
O servidor baixa os dados de execução apenas do dia atual.
Salva uma cópia de backup em %TEMP%/C6_RPA_EXEC/automacoes_exec.xlsx.
Atualização do Cache Local: Para cada script, o contador de execuções do dia (daily_execution_cache) é atualizado para o maior valor entre:
O valor que o servidor já tem em memória (contado localmente).
O valor retornado pelo BigQuery (contagem de linhas retornadas para aquele script).
Objetivo: Garantir consistência mesmo se o BigQuery tiver delay na inserção de dados.
2.4. Lógica de Agendamento (Scheduler)
Frequência: Executado a cada iteração do loop (1 segundo). Lógica de Decisão: Para cada script descoberto:
Verifica a hora atual (datetime.now().hour).
Calcula a Meta de Execuções (Target Runs) baseada no Cron:
Se Cron == "ALL": Meta = Hora Atual + 1.
Se Cron == Lista de Horas: Meta = Quantidade de horas na lista que são menores ou iguais à hora atual.
Obtém as Execuções Realizadas (Actual Runs) do cache daily_execution_cache.
Calcula Backlog = Target Runs - Actual Runs.
Ação: Se Backlog > 0:
Verifica se o BigQuery foi lido com sucesso pelo menos uma vez no dia (self.bq_verified). Se não, aborta para segurança.
Verifica se o script já está rodando (self.running_tasks).
Verifica se o script já está na fila (self.execution_queue).
Se livre, adiciona o script à fila de execução com prioridade 0.
2.5. Processamento da Fila (Queue Processor)
Frequência: Executado a cada iteração do loop. Lógica:
Limpeza: Verifica processos ativos (subprocess.Popen). Se algum terminou (poll() is not None):
Remove da lista de tarefas ativas.
Incrementa daily_execution_cache em +1 imediatamente.
Início de Novos Processos:
Verifica se a quantidade de tarefas rodando é menor que max_concurrent (5).
Retira o próximo item da fila.
Inicia o processo via subprocess.Popen usando o mesmo executável Python que está rodando o servidor (sys.executable).
Injeção de Variável: Define a variável de ambiente ENV_EXEC_MODE = "AGENDAMENTO" para o processo filho.
3. Comportamento da Interface Gráfica (UI)
3.1. Cards de Automação
Os cards exibem o estado e bordas consolidado combinando dados locais e históricos:
RUNNING (Azul): Se o script estiver na lista running_tasks do servidor.
SUCCESS (Verde): Se não estiver rodando e o último status vindo do BigQuery contiver "SUCCESS" ou "SUCESSO".
ERROR (Vermelho): Se não estiver rodando e o último status vindo do BigQuery contiver "ERROR" ou "ERRO".
NO_DATA (Amarelo): Se o status for "NO_DATA".
SCHEDULED (Cinza): Se o script estiver na fila de espera (execution_queue).
IDLE (Padrão): Se nenhuma das condições acima for atendida.
3.2. Ações de Controle
Botão Run: Invoca worker.run_script(path), que inicia o subprocesso imediatamente (respeitando a verificação de duplicidade, mas furando a fila do agendador).
Botão Stop: Invoca worker.kill_script(path).
Envia sinal SIGTERM (Solicitação de término gracioso).
Agenda uma verificação para 2 segundos depois.
Se o processo ainda estiver vivo após 2 segundos, envia SIGKILL (Matar forçadamente). 
3.3. Prevenção de Suspensão de Energia
O servidor executa ctypes.windll.kernel32.SetThreadExecutionState(0x80000003) na inicialização.
Flag 0x80000003: Combinação de ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED.
Efeito: Informa ao Windows que uma operação crítica está em andamento, impedindo que o sistema entre em modo de suspensão (Sleep) ou desligue o monitor enquanto a janela estiver aberta.

4. Os caras de cada script python deve ser criado em 300x300, cada card deverá conter a informação  de última execução que aquele script python deve pegando da tabela automacoes_exec, lembrando que só deve procurar execuções do dia atual, current date. As datas na coluna start_time estão no formato assim, exemplo: 2025-12-12T12:04:05

Os caras devem conter também que horas irá acontecer a próxima execução do script pelo servidor se ele possuir agendamento, caso não possua mais agendamentos cron para o dia vigente, apenas coloque a informação ‘Próxima execução: só amanhã’.

Deverá conter o status também da última execução, se for SUCCESS, SUCESSO o card precisa ficar com borda Verde, se for Error ou erro, fique vermelho, se for no_data, fique amarelo. O código deverá segregar os caras dos Scripts usando os valores da coluna area_name da registro_automacoes. As abas com os caras deverão ficar em modo lista na lateral do servidor, a esquerda.
