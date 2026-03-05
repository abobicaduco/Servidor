<div align="center">

# ⚡ C6 Cron Server

**Servidor orquestrador de automações Python com monitoramento em tempo real.**

![Python](https://img.shields.io/badge/python-3.10+-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/flask-3.0+-green?logo=flask)
![License](https://img.shields.io/badge/license-internal-lightgrey)
![LGPD](https://img.shields.io/badge/LGPD-compliant-brightgreen)

</div>

---

## 📋 Sobre

O **C6 Cron Server** é um servidor Python que gerencia a execução automatizada de scripts Python da Célula Python Monitoração. Ele opera 100% em **localhost**, sem exposição de rede.

### Funcionalidades

| Feature | Descrição |
|---------|-----------|
| 🕐 **Cron por Hora** | Agendamento por horários (0-23h), lidos de planilha Excel |
| 🔄 **Catch-up Inteligente** | Se o servidor iniciar atrasado, executa scripts atrasados 1x sem spam |
| 📊 **Dashboard Real-time** | Interface HTML com WebSocket, sem necessidade de refresh |
| ⏱️ **Timeout Automático** | Scripts rodando >2h são encerrados automaticamente |
| 🔀 **Workflows** | Execução sequencial de múltiplos scripts com controle de passos |
| 📝 **Logs por Script** | Cada script tem seu próprio arquivo de log |
| 🗄️ **SQLite Tracking** | Execuções registradas em banco local (WAL mode) |
| 🛡️ **LGPD Compliant** | Zero dados pessoais, headers de segurança, localhost only |
| 🔧 **Portátil (.env)** | Funciona em qualquer PC via configuração `.env` |

---

## 🚀 Instalação

### Pré-requisitos
- **Python 3.10+**

### Passos

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/c6-cron-server.git
cd c6-cron-server

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Configure o .env
cp .env.example .env
# Edite o .env com os caminhos da sua máquina (veja seção abaixo)

# 4. Inicie o servidor
python server.py
```

O servidor estará disponível em **http://127.0.0.1:3000**

---

## ⚙️ Configuração (.env)

Crie um arquivo `.env` na raiz do projeto. Use o `.env.example` como base:

```env
# Diretório raiz (contém /automacoes e /novo_servidor)
BASE_PATH=C:\Users\SEU_USUARIO\C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A\Mensageria e Cargas Operacionais - 11.CelulaPython\graciliano

# Diretório com os scripts .py
SCRIPTS_DIR=C:\Users\SEU_USUARIO\...\graciliano\automacoes

# Diretório do servidor
NEW_SERVER_DIR=C:\Users\SEU_USUARIO\...\graciliano\novo_servidor

# Planilha de registro
EXCEL_PATH=C:\Users\SEU_USUARIO\...\graciliano\novo_servidor\registro_automacoes.xlsx

# Servidor (não altere a menos que necessário)
SERVER_HOST=127.0.0.1
SERVER_PORT=3000

# Máximo de scripts simultâneos
MAX_CONCURRENT=10

# Timeout em segundos (7200 = 2 horas)
SCRIPT_TIMEOUT_SECONDS=7200

# Timezone
TIMEZONE=America/Sao_Paulo
```

### PC da Empresa vs PC Pessoal

| Parâmetro | PC Empresa (OneDrive) | PC Pessoal |
|-----------|----------------------|------------|
| `BASE_PATH` | `C:\Users\carlos.lsilva\C6 CTVM LTDA...\graciliano` | Qualquer pasta local |
| `SCRIPTS_DIR` | `{BASE_PATH}\automacoes` | Pasta com seus scripts .py |
| `EXCEL_PATH` | `{NEW_SERVER_DIR}\registro_automacoes.xlsx` | Planilha local de teste |

---

## 📊 Planilha de Registro

O servidor lê a planilha `registro_automacoes.xlsx` com as seguintes colunas:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `script_name` | texto | Nome do script (sem `.py`) |
| `area_name` | texto | Área responsável (para agrupar no dashboard) |
| `cron_schedule` | texto | Horários separados por `,` (ex: `2,7,13`) |
| `is_active` | texto | `true` ou `false` |
| `inactivated_at` | data/NaT | Data de inativação (`NaT` = ativo) |
| `emails_principal` | texto | Email responsável |
| `emails_cc` | texto | Emails CC (separados por `;`) |
| `move_file` | texto | `true`/`false` |
| `created_at` | data | Data de criação |

---

## 🔒 LGPD & Segurança

- **100% Localhost** — nenhuma porta é exposta na rede
- **Zero dados pessoais** — apenas metadados operacionais (nomes de scripts, horários)
- **Security Headers** — CSP, X-Frame-Options, X-XSS-Protection
- **SQLite local** — WAL mode, sem transmissão externa
- **Sem autenticação externa** — acesso somente na máquina local

---

## 🏗️ Arquitetura

```
c6-cron-server/
├── server.py              # Backend Python (Flask + SocketIO)
├── static/
│   └── index.html         # Frontend HTML (dashboard)
├── .env                   # Configuração local (não commitado)
├── .env.example           # Template de configuração
├── requirements.txt       # Dependências Python
└── README.md              # Este arquivo
```

### Componentes Internos

```
server.py
├── Bootstrap          → Instala dependências automaticamente
├── SQLite DB          → Single source of truth para execuções
├── Orchestrator       → Classe principal
│   ├── Scheduler      → Thread: checa slots a cada 15s
│   ├── Queue Processor → Thread: executa processos a cada 2s
│   ├── Timeout Watchdog → Encerra scripts >2h
│   └── Workflow Engine → Execução sequencial com bloqueio
├── Flask Routes       → API REST endpoints
└── SocketIO           → WebSocket para push real-time
```

---

## 📜 API Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/` | Dashboard HTML |
| `GET` | `/api/status` | Estado completo (JSON) |
| `POST` | `/api/run` | Executar script manualmente |
| `POST` | `/api/kill` | Matar processo |
| `POST` | `/api/kill-all` | Matar todos |
| `POST` | `/api/reload` | Recarregar Excel |
| `GET` | `/api/history/<name>` | Histórico de execuções |
| `GET` | `/api/script-log/<name>` | Log do script |
| `GET` | `/api/about` | Info + LGPD |
| `GET/POST/PUT/DELETE` | `/api/workflows` | CRUD de workflows |
| `POST` | `/api/workflows/<id>/trigger` | Disparar workflow |
| `POST` | `/api/workflows/stop` | Parar workflow |

---

## 👨‍💻 Autor

**Célula Python Monitoração** — C6 Bank
