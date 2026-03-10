from fastapi import FastAPI, HTTPException
import pandas as pd
import os
import kaggle
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv('.env')

app = FastAPI(title="Meu primeiro ETL")

## Configuração (Variáveis carregadas do .env)
DATASET_NAME = os.getenv('DATASET_NAME')
DATASET_PATH = os.getenv('DATASET_PATH', './')
DATASET_FILE = os.getenv('DATASET_FILE')

df = None


def download_dataset():
    """Função para baixar os dados direto do Kaggle"""
    try:
        print(f"Iniciando o download do dataset: {DATASET_NAME}...")
        kaggle.api.dataset_download_files(DATASET_NAME, path=DATASET_PATH, unzip=True)
        print(f'Dataset baixado e descompactado com sucesso em {DATASET_PATH}')
    except Exception as e:
        print(f'Erro ao efetuar o download via Kaggle: {e}')


# Camada de Extração e Transformação
def load_data():
    '''Função responsável por ler o CSV e fazer limpezas básicas'''
    global df

    # 1. Tenta baixar o arquivo do Kaggle antes de ler
    if DATASET_NAME:
        download_dataset()

    # Monta o caminho completo do arquivo
    file_path = os.path.join(DATASET_PATH, DATASET_FILE)

    if not os.path.exists(file_path):
        print(f"Erro: Arquivo {file_path} não encontrado.")
        return

    # Extract
    print("Carregando DataSet...")
    temp_df = pd.read_csv(file_path)

    # Transform (remover nulos e renomear colunas)
    temp_df.dropna(inplace=True)
    temp_df.columns = [c.lower().replace(' ', '_') for c in temp_df.columns]

    # Load (Para a memória da API)
    df = temp_df
    print("Dados carregados com sucesso.")


# Evento de inicialização da API
@app.on_event("startup")
async def startup_event():
    load_data()


# EndPoints (mantidos como originais)
@app.get("/")
def home():
    return {"mensagem": "API de Análise de Dados Online", "status": "Ativo"}


@app.get("/dados")
def get_all_data(limit: int = 10, offset: int = 0):
    """
    Retorna os dados brutos com paginação
    """
    if df is None:
        raise HTTPException(status_code=503, detail="Dataset não carregado")

    # Converte fatia do dataframe para dicionário (JSON)
    data = df.iloc[offset:offset + limit].to_dict(orient='records')
    return {
        "total_registros": len(df),
        "limite": limit,
        "offset": offset,
        "dados": data
    }


@app.get("/analise/resumo")
def get_summary():
    """
    Endpoint de análise: Retorna estatísticas descritivas das colunas numéricas.
    """
    if df is None:
        raise HTTPException(status_code=503, detail="Dataset não carregado")

    # TRANSFORM on-the-fly: Gera estatísticas
    resumo = df.describe().to_dict()
    return resumo


@app.get("/analise/coluna/{coluna}")
def get_column_stats(coluna: str):
    """
    Retorna contagem de valores únicos para uma coluna categórica específica.
    """
    if df is None:
        raise HTTPException(status_code=503, detail="Dataset não carregado")

    if coluna not in df.columns:
        raise HTTPException(status_code=404, detail="Coluna não encontrada")

    contagem = df[coluna].value_counts().head(10).to_dict()
    return {"coluna": coluna, "top_10_valores": contagem}