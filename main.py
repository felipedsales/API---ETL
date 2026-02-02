from fastapi import FastAPI, HTTPException
import pandas as pd
import os
app = FastAPI(title="Meu primeiro ETL")

## Configuração (Variáveis Globais)
DATASET_PATH = "daily_gym_attendance_workout_data.csv"
df = None

# Camada de Extração e Transformação
def load_data():
    '''Função responsável por ler o CSV e fazer limpezas básicas'''
    global df
    if not os.path.exists(DATASET_PATH):
        print(f"Erro: Arquivo {DATASET_PATH} não encontrado.")
        return
    # Extract
    print("Carregando DataSet...")
    temp_df = pd.read_csv(DATASET_PATH)

    # Transform (Exemplo: remover nulos e renomear colunas)
    # Aqui você aplica sua lógica de negócio/limpeza
    temp_df.dropna(inplace=True)
    temp_df.columns = [c.lower().replace(' ', '_') for c in temp_df.columns]

    #Load (Para a memória da API)
    df = temp_df
    print("Dados carregados com sucesso.")

# Evento de inicialização da API
@app.on_event("startup")
async def startup_event():
    load_data()

# EndPoints
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

    #Converte fatia do dataframe para dicionário (JSON)
    data = df.iloc[offset:offset+limit].to_dict(orient='records')
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