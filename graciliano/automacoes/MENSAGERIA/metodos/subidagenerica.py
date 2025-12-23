import pandas as pd
from google.cloud import bigquery
from google.auth import default
import pythoncom
import win32com.client as win32
import re
import unicodedata

CAMINHO_ARQUIVO = r"C:\Users\carlos.lsilva\Downloads\base_navigli.xlsx"

PROJECT_ID = "datalab-pagamentos"
# Corrigido de "00-temp" para "00_temp" conforme sua indicação
DATASET = "00_temp"
TABELA = "BASE_CONTESTACAO_0512"

EMAIL_DESTINO = "joao.vitoras@c6bank.com"

def enviar_email(assunto, corpo):
    pythoncom.CoInitialize()
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.To = EMAIL_DESTINO
    mail.Subject = assunto
    mail.Body = corpo
    mail.Send()
    pythoncom.CoUninitialize()

def normalizar_coluna(coluna):
    """
    Normaliza o nome da coluna para o padrão do BigQuery:
    - Remove acentos
    - Substitui espaços e caracteres especiais por _
    - Converte para maiúsculo
    """
    s = str(coluna)
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    s = re.sub(r'[^a-zA-Z0-9]', '_', s)
    return s.lower()

def main():
    try:
        # Lê o arquivo forçando tudo como string
        df = pd.read_excel(CAMINHO_ARQUIVO, dtype=str)
        
        # Normaliza colunas
        df.columns = [normalizar_coluna(col) for col in df.columns]

        # Tratamento de nulos/strings
        df = df.astype(str)
        df.replace("nan", None, inplace=True)

        creds, _ = default()
        client = bigquery.Client(credentials=creds, project=PROJECT_ID)

        # --- NOVO BLOCO: GARANTIA DO DATASET ---
        # Define a referência do dataset
        dataset_id = f"{PROJECT_ID}.{DATASET}"
        
        # Tenta criar o dataset (se já existir, não faz nada e não dá erro graças ao exists_ok=True)
        # Isso evita o erro 404 se o dataset 00_temp ainda não existir
        try:
            dataset_ref = bigquery.Dataset(dataset_id)
            dataset_ref.location = "US" # Ajuste para "southamerica-east1" se seu projeto for BR
            client.create_dataset(dataset_ref, exists_ok=True)
            print(f"Dataset {dataset_id} verificado/criado com sucesso.")
        except Exception as e:
            print(f"Aviso ao verificar dataset: {e}")
            # Continua, pois o erro pode ser de permissão de leitura, mas a escrita pode funcionar
        # ---------------------------------------

        tabela_id = f"{PROJECT_ID}.{DATASET}.{TABELA}"

        # Schema dinâmico (tudo STRING)
        schema = []
        for col in df.columns:
            schema.append(bigquery.SchemaField(col, "STRING"))

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Cria ou substitui a tabela
            schema=schema,
            autodetect=False,
        )

        job = client.load_table_from_dataframe(df, tabela_id, job_config=job_config)
        job.result()

        enviar_email(
            assunto="Carga BQ – SUCESSO",
            corpo=f"Carga concluída com sucesso na tabela:\n{tabela_id}\n\nTotal de linhas: {len(df)}"
        )
        print("Processo finalizado com sucesso.")

    except Exception as e:
        print(f"Erro fatal: {e}")
        enviar_email(
            assunto="Carga BQ – FALHA",
            corpo=f"Erro durante a carga:\n{str(e)}"
        )

if __name__ == "__main__":
    main()