# 🤖 C6 RPA Dashboard (Python Automation Server)

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white) 
![BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white) 
![PySide6](https://img.shields.io/badge/PySide6-Qt-41CD52?style=for-the-badge&logo=qt&logoColor=white)
![Security](https://img.shields.io/badge/Enterprise_Security-Ready-red?style=for-the-badge&logo=cisco&logoColor=white)

---

### 🇧🇷 Português | [🇺🇸 English](#-english) | [🇪🇸 Español](#-español)

## 📌 Visão Geral
Este projeto é um **Orquestrador de Automação Corporativa** desenvolvido para gerenciar e executar scripts Python críticos para operações financeiras no **C6 Bank**. 

Ele foi desenhado para rodar em ambientes com **altas restrições de segurança** (Firewalls, Proxy, Sem Admin), garantindo estabilidade e auditoria completa via **Google BigQuery**.

### ✨ Funcionalidades Principais
*   **🏢 Agendamento Inteligente**: Gerencia filas de execução, prioridades e "catch-up" (recupera atrasos automaticamente se o PC for desligado).
*   **🛡️ Segurança Enterprise**: 
    *   Não exige privilégios de Administrador.
    *   Credenciais seguras via variáveis de ambiente (`.env`).
    *   Bypass seguro de SSL corporativo apenas onde necessário.
*   **📊 Monitoramento em Tempo Real**: Interface GUI moderna (PySide6) com tema escuro, busca em tempo real e status de cada robô via BigQuery.
*   **🧠 Lógica Autônoma**: Detecta novos scripts na rede automaticamente sem precisar reiniciar o servidor.

### 🚀 Como Rodar
1.  **Clone o Repositório**:
    ```bash
    git clone https://github.com/abobicaduco/Servidor.git
    ```
2.  **Configure o Ambiente**:
    Crie um arquivo `.env` com suas credenciais (veja `.env.example`).
3.  **Execute**:
    ```bash
    python Servidor.py
    ```

---

<a name="-english"></a>
## 🇺🇸 English

## 📌 Overview
This project is an **Enterprise Automation Orchestrator** designed to manage and execute critical Python scripts for financial operations at **C6 Bank**.

It is engineered to operate within **strictly restricted environments** (Firewalls, Proxies, No Admin Rights), ensuring stability and full auditability via **Google BigQuery**.

### ✨ Key Features
*   **🏢 Smart Scheduling**: Manages execution queues, priorities, and "catch-up" logic (automatically recovers missed runs if the PC was offline).
*   **🛡️ Enterprise Security**: 
    *   Zero Admin privileges required.
    *   Secure credential management via environment variables (`.env`).
    *   Safe corporate SSL bypass only where strictly necessary.
*   **📊 Real-Time Monitoring**: Modern GUI interface (PySide6) with dark mode, real-time search, and BigQuery execution status for every bot.
*   **🧠 Autonomous Logic**: Automatically detects new scripts on the network without needing a server restart.

### 🚀 How to Run
1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/abobicaduco/Servidor.git
    ```
2.  **Setup Environment**:
    Create a `.env` file with your credentials (see `.env.example`).
3.  **Execute**:
    ```bash
    python Servidor.py
    ```

---

<a name="-español"></a>
## 🇪🇸 Español

## 📌 Visión General
Este proyecto es un **Orquestador de Automatización Empresarial** desarrollado para gestionar y ejecutar scripts de Python críticos para las operaciones financieras en **C6 Bank**.

Está diseñado para funcionar en entornos con **altas restricciones de seguridad** (Firewalls, Proxies, Sin Admin), garantizando estabilidad y auditoría completa a través de **Google BigQuery**.

### ✨ Características Principales
*   **🏢 Programación Inteligente**: Gestiona colas de ejecución, prioridades y lógica de "catch-up" (recupera automáticamente ejecuciones perdidas si la PC estaba apagada).
*   **🛡️ Seguridad Empresarial**: 
    *   No requiere privilegios de Administrador.
    *   Gestión credenciales segura vía variables de entorno (`.env`).
    *   Bypass seguro de SSL corporativo solo donde es estrictamente necesario.
*   **📊 Monitoreo en Tiempo Real**: Interfaz GUI moderna (PySide6) con modo oscuro, búsqueda en tiempo real y estado de ejecución de cada bot vía BigQuery.
*   **🧠 Lógica Autónoma**: Detecta automáticamente nuevos scripts en la red sin necesidad de reiniciar el servidor.

### 🚀 Como Ejecutar
1.  **Clonar el Repositorio**:
    ```bash
    git clone https://github.com/abobicaduco/Servidor.git
    ```
2.  **Configurar Entorno**:
    Crea un archivo `.env` con tus credenciales (ver `.env.example`).
3.  **Ejecutar**:
    ```bash
    python Servidor.py
    ```
