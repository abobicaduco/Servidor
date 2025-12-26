import os
import sys
import zipfile
import shutil
import requests
from pathlib import Path

# NOME DO ARQUIVO (V20)
FILENAME = "node-v20.10.0-win-x64.zip"

# LISTA EXTENSIVA DE MIRRORS (TENTATIVA POR FORCA BRUTA)
MIRRORS = [
    # 1. Official (Tentativa padrao)
    f"https://nodejs.org/dist/v20.10.0/{FILENAME}", 
    
    # 2. NPM Mirror (China - robusto)
    f"https://npmmirror.com/mirrors/node/v20.10.0/{FILENAME}",
    
    # 3. Huawei Cloud
    f"https://mirrors.huaweicloud.com/nodejs/v20.10.0/{FILENAME}",
    
    # 4. Tsinghua University (China - muito rapido e geralmente desbloqueado)
    f"https://mirrors.tuna.tsinghua.edu.cn/nodejs-release/v20.10.0/{FILENAME}",
    
    # 5. USTC (University of Science and Technology of China)
    f"https://mirrors.ustc.edu.cn/node/v20.10.0/{FILENAME}",
    
    # 6. Aliyun (Alibaba Cloud)
    f"https://mirrors.aliyun.com/nodejs-release/v20.10.0/{FILENAME}",
    
    # 7. Unofficial Builds (Geralmente nao bloqueado pois é outro dominio)
    f"https://unofficial-builds.nodejs.org/download/release/v20.10.0/{FILENAME}",

    # 8. CNode (Community Mirror)
    f"https://cnodejs.org/dist/v20.10.0/{FILENAME}",
    
    # 9. ISCAS (Institute of Software Chinese Academy of Sciences)
    f"https://mirror.iscas.ac.cn/node/v20.10.0/{FILENAME}",
    
    # 10. Nanjing University
    f"https://mirrors.nju.edu.cn/nodejs/v20.10.0/{FILENAME}",

    # 11. Sahil (India Mirror - as vezes funciona)
    f"https://nodejs.org/download/release/v20.10.0/{FILENAME}", # Alternativo oficial
    
    # TENTATIVA COM VERSAO ANTERIOR (V18 LTS) CASO A V20 ESTEJA OFF EM ALGUNS
    "https://nodejs.org/dist/v18.19.0/node-v18.19.0-win-x64.zip",
    "https://npmmirror.com/mirrors/node/v18.19.0/node-v18.19.0-win-x64.zip",
    "https://mirrors.huaweicloud.com/nodejs/v18.19.0/node-v18.19.0-win-x64.zip",
]

ZIP_NAME = "node_bin.zip"
EXTRACT_DIR = Path("node_bin")

def download_file():
    print(f"Iniciando tentativa de download (BRUTE FORCE)...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*'
    }

    for url in MIRRORS:
        print(f"Tentando: {url}")
        try:
            # timeout curto para pular rapido
            response = requests.get(url, stream=True, verify=False, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"FALHA HTTP: {response.status_code}")
                continue
            
            # Detecção de Bloqueio (Firewall C6 redireciona para pagina html)
            if "c6bank" in response.url.lower() or "block" in response.url.lower():
                print("BLOQUEADO PELO FIREWALL (Redirect detectado).")
                continue

            content_type = response.headers.get('Content-Type', '').lower()
            if 'zip' not in content_type and 'octet-stream' not in content_type and 'application/x-zip-compressed' not in content_type:
                 print(f"CONTEUDO INVALIDO ({content_type}). Provavelmente HTML de bloqueio.")
                 continue

            total_length = response.headers.get('content-length')
            # Se for muito pequeno (< 1MB), é bloqueio disfarçado
            if total_length and int(total_length) < 1000000:
                print("ARQUIVO MUITO PEQUENO (Provavelmente bloqueio).")
                continue

            print("Conexão estabelecida! Baixando...")
            downloaded = 0
            
            with open(ZIP_NAME, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_length:
                            percent = int((downloaded / int(total_length)) * 100)
                            if percent % 10 == 0:
                                print(f"Download: {percent}%", end="\r")
                                
            print("\nDownload concluido com SUCESSO!")
            return True
        except Exception as e:
            print(f"Erro: {e}")
            continue
            
    print("\nTODOS OS MIRRORS FALHARAM.")
    return False

def extract_file():
    print("Extraindo arquivos...")
    try:
        if EXTRACT_DIR.exists():
            shutil.rmtree(EXTRACT_DIR)
            
        with zipfile.ZipFile(ZIP_NAME, 'r') as zip_ref:
            zip_ref.extractall(".")
            
            # Procura pasta extraida (pode ser v20 ou v18)
            extracted_folder = None
            for item in os.listdir("."):
                if item.startswith("node-v") and os.path.isdir(item) and item != "node_modules":
                    extracted_folder = item
                    break
            
            if extracted_folder:
                shutil.move(extracted_folder, EXTRACT_DIR)
                print(f"Extraido para: {EXTRACT_DIR.absolute()}")
                return True
            else:
                print("Nao consegui encontrar a pasta extraida.")
                return False
                
    except Exception as e:
        print(f"Erro na extracao: {e}")
        return False

def configure_npm_ssl():
    print("Configurando NPM para ignorar SSL...")
    if EXTRACT_DIR.exists():
        with open(".npmrc", "w") as f:
            f.write("strict-ssl=false\n")
            f.write("registry=http://registry.npmjs.org/\n")
            f.write("ca=null\n")
            f.write("fetch-retry-maxtimeout=60000\n")
            f.write("fetch-retry-mintimeout=10000\n")
    else:
        print("Pasta node_bin nao encontrada.")

if __name__ == "__main__":
    if download_file():
        if extract_file():
            configure_npm_ssl()
            print("\n[SUCESSO] INSTALACAO DO NODE PORTATIL CONCLUIDA!")
            try: os.remove(ZIP_NAME)
            except: pass
        else:
            print("Falha na extracao.")
    else:
        print("Falha no download.")
