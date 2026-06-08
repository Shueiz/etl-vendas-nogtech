from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.sensors.filesystem import FileSensor # type: ignore
from datetime import datetime, timedelta
import pandas as pd # type: ignore
import re
import requests
import sqlite3
import os
import shutil

# Caminho baseado no volume mapeado no Docker
DATA_DIR = '/opt/airflow/data'

default_args = {
    'owner': 'engenharia_dados',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

def padronizar_cpf(cpf):
    """Garante que o CPF tenha 11 dígitos numéricos e aplica a máscara tradicional."""
    if pd.isna(cpf):
        return None
    # Remove qualquer caractere que não seja número
    cpf_limpo = ''.join(filter(str.isdigit, str(cpf)))
    
    # Preenche com zeros à esquerda caso tenha vindo incompleto
    cpf_limpo = cpf_limpo.zfill(11)
    
    # Aplica a máscara padrão
    return f"{cpf_limpo[0:3]}.{cpf_limpo[3:6]}.{cpf_limpo[6:9]}-{cpf_limpo[9:11]}"

def anonimizar_cpf(cpf_mascarado):
    """Transforma 123.456.789-00 em ***.456.789-** conforme requisito LGPD."""
    if not cpf_mascarado or len(cpf_mascarado) != 14:
        return "***.***.***-**"
    return f"***.{cpf_mascarado[4:7]}.{cpf_mascarado[8:11]}-**"


def extract_data():
    """Extração ajustada para o novo formato de mês (Ano-Mês) e nomes de colunas do JSON."""
    caminho_csv = os.path.join(DATA_DIR, 'transacoes_nogtech.csv')
    caminho_json = os.path.join(DATA_DIR, 'engajamento_alunos.json')
    
    df_vendas = pd.read_csv(caminho_csv, encoding='latin-1', sep=';')
    df_engajamento = pd.read_json(caminho_json, encoding='utf-8')
    
    # 1. Padronização inicial das chaves de CPF em ambas as fontes ANTES do merge
    if 'cpf_aluno' in df_vendas.columns:
        df_vendas['cpf_aluno'] = df_vendas['cpf_aluno'].apply(padronizar_cpf)
    if 'cpf_aluno' in df_engajamento.columns:
        df_engajamento['cpf_aluno'] = df_engajamento['cpf_aluno'].apply(padronizar_cpf)
            
    # 2. CORREÇÃO CRÍTICA DO MÊS: Cria o formato 'YYYY-MM' baseado na data da transação
    if 'data_transacao' in df_vendas.columns:
        df_vendas['data_transacao_dt'] = pd.to_datetime(df_vendas['data_transacao'], format='mixed', dayfirst=True, errors='coerce')
        
        # Extrai o ano e o mês garantindo dois dígitos com zfill (ex: '2024-01')
        df_vendas['ano_str'] = df_vendas['data_transacao_dt'].dt.year.fillna(2024).astype(int).astype(str)
        df_vendas['mes_str'] = df_vendas['data_transacao_dt'].dt.month.fillna(1).astype(int).astype(str).str.zfill(2)
        df_vendas['mes_referencia'] = df_vendas['ano_str'] + '-' + df_vendas['mes_str']
        
        # Limpa colunas auxiliares
        df_vendas.drop(columns=['ano_str', 'mes_str'], inplace=True)
    
    if 'mes_referencia' in df_engajamento.columns:
        df_engajamento['mes_referencia'] = df_engajamento['mes_referencia'].astype(str).str.strip()

    # 3. Cruzamento perfeito usando CPF E o novo formato de Mês de Referência (LEFT JOIN)
    if 'cpf_aluno' in df_vendas.columns and 'mes_referencia' in df_vendas.columns:
        df_final = pd.merge(df_vendas, df_engajamento, on=['cpf_aluno', 'mes_referencia'], how='left')
    else:
        df_final = pd.merge(df_vendas, df_engajamento, on='cpf_aluno', how='left')
        
    if 'data_transacao_dt' in df_final.columns:
        df_final.drop(columns=['data_transacao_dt'], inplace=True)
    
    df_final.to_parquet(os.path.join(DATA_DIR, 'stage_extract.parquet'))
    print("Extração e Merge com novo Schema de datas concluídos.")


def transform_data():
    """Transformação ajustada para os novos nomes de colunas de engajamento do professor."""
    df = pd.read_parquet(os.path.join(DATA_DIR, 'stage_extract.parquet'))
    
    # LGPD: Anonimização do CPF e Remoção de Nome
    if 'cpf_aluno' in df.columns:
        df['cpf_aluno'] = df['cpf_aluno'].apply(anonimizar_cpf)
    if 'nome_aluno' in df.columns:
        df.drop(columns=['nome_aluno'], inplace=True)
        
    # Limpeza de Planos com Regex
    if 'plano_adquirido' in df.columns:
        df['plano_adquirido'] = df['plano_adquirido'].fillna('Não informado').astype(str).str.strip()
        df['plano_adquirido'] = df['plano_adquirido'].replace(r'(?i)^(?:nan|none|)$', 'Não informado', regex=True)
        df['plano_adquirido'] = df['plano_adquirido'].str.replace(
            r'B[\W_]*[aáàâãäÁÃÂÀ]*sico', 'Básico', regex=True, flags=re.IGNORECASE
        )

    # Correção de Tipagem do faturamento
    if 'valor_brl' in df.columns:
        df['valor_brl'] = df['valor_brl'].astype(str).str.replace(',', '.', regex=False).str.strip()
        df['valor_brl'] = pd.to_numeric(df['valor_brl'], errors='coerce').fillna(0.0)

    # Enriquecimento com BrasilAPI (CEP)
    if 'cep_cobranca' in df.columns:
        df['cep_limpo'] = df['cep_cobranca'].astype(str).str.split('.').str[0].str.replace(r'\D', '', regex=True)
        df['cep_limpo'] = df['cep_limpo'].apply(lambda x: x.zfill(8) if 0 < len(x) < 8 else x)
        
        ceps_validos = df.loc[df['cep_limpo'].str.len() == 8, 'cep_limpo'].unique()
        dict_ceps = {}
        
        for cep in ceps_validos:
            try:
                resp = requests.get(f"https://brasilapi.com.br/api/cep/v2/{cep}", timeout=10)
                if resp.status_code == 200:
                    dados = resp.json()
                    dict_ceps[cep] = {
                        'cidade': dados.get('city'), 'estado': dados.get('state'), 'bairro': dados.get('neighborhood')
                    }
                else:
                    dict_ceps[cep] = {'cidade': 'Não Localizado', 'estado': 'NI', 'bairro': 'Não Localizado'}
            except:
                dict_ceps[cep] = {'cidade': 'Não Localizado', 'estado': 'NI', 'bairro': 'Não Localizado'}
        
        df['cidade'] = df['cep_limpo'].map(lambda x: dict_ceps.get(x, {}).get('cidade', 'Sem CEP'))
        df['estado'] = df['cep_limpo'].map(lambda x: dict_ceps.get(x, {}).get('estado', 'NI'))
        df['bairro'] = df['cep_limpo'].map(lambda x: dict_ceps.get(x, {}).get('bairro', 'Sem CEP'))
        df.drop(columns=['cep_limpo'], inplace=True)

    # Análise de Calendário Feriados
    if 'data_transacao' in df.columns:
        df['data_transacao_conv'] = pd.to_datetime(df['data_transacao'], format='mixed', dayfirst=True, errors='coerce')
        anos_unicos = df['data_transacao_conv'].dt.year.dropna().unique()
        feriados_cache = []
        for ano in anos_unicos:
            try:
                resp = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{int(ano)}", timeout=10)
                if resp.status_code == 200:
                    feriados_cache.extend([f['date'] for f in resp.json()])
            except:
                pass
        feriados_datas = pd.to_datetime(feriados_cache).date
        df['venda_em_feriado'] = df['data_transacao_conv'].dt.date.isin(feriados_datas)
        df.drop(columns=['data_transacao_conv'], inplace=True)

    # CORREÇÃO CRÍTICA: Mapeia os novos nomes reais das colunas de engajamento do JSON
    colunas_eng = ['horas_assistidas', 'tickets_suporte', 'nps_score']
    for col in colunas_eng:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df.to_parquet(os.path.join(DATA_DIR, 'stage_transform.parquet'))
    print("Transformação concluída com os novos campos mapeados.")


def load_data(ds, **kwargs):
    """Garante a Idempotência limpando partições antigas e poluídas antes de gravar o novo lote."""
    df = pd.read_parquet(os.path.join(DATA_DIR, 'stage_transform.parquet'))
    
    datas_convertidas = pd.to_datetime(df['data_transacao'], format='mixed', dayfirst=True, errors='coerce')
    df['ano_particao'] = datas_convertidas.dt.year.fillna(2026).astype(int)
    df['mes_particao'] = datas_convertidas.dt.month.fillna(5).astype(int)
    
    caminho_datalake = os.path.join(DATA_DIR, 'datalake_fato_vendas')
    
    # TRATATIVA ANTE-POLUIÇÃO: Remove a pasta antiga completamente antes de criar a nova estrutura limpa
    if os.path.exists(caminho_datalake):
        shutil.rmtree(caminho_datalake)
        
    df.to_parquet(caminho_datalake, partition_cols=['ano_particao', 'mes_particao'], index=False)
    print("Carga higienizada no Data Lake concluída.")


def aggregate_metrics(ds, **kwargs):
    """Lê o Data Lake transformado e alimenta o Banco de Dados Relacional SQLite para visualização no Metabase BI."""
    caminho_datalake = os.path.join(DATA_DIR, 'datalake_fato_vendas')
    
    if os.path.exists(caminho_datalake):
        df = pd.read_parquet(caminho_datalake)
        
        caminho_banco = os.path.join(DATA_DIR, 'banco_metabase.db')
        conn = sqlite3.connect(caminho_banco)
        
        # Salva tabelas tratadas para o BI consumir sem esforço computacional
        df.to_sql('fato_vendas_completas', conn, if_exists='replace', index=False)
        
        if 'valor_brl' in df.columns and 'estado' in df.columns:
            resumo = df.groupby('estado', as_index=False)['valor_brl'].sum().sort_values(by='valor_brl', ascending=False)
            resumo.to_sql('resumo_por_estado', conn, if_exists='replace', index=False)
            
        conn.close()
        print("Modelagem Relacional carregada no SQLite para consumo do Metabase BI.")

# ==========================================
# GRAFO DE EXECUÇÃO E DEPENDÊNCIAS DO AIRFLOW
# ==========================================
with DAG(
    'etl_vendas_diarias',
    default_args=default_args,
    description='Pipeline de Vendas Resiliente NogTech com BrasilAPI',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 26),
    catchup=False,
    tags=['nogtech', 'etl', 'lgpd', 'brasilapi'],
) as dag:

    # Requisito 4: Resiliência contra ausência de arquivos na pasta mapeada
    aguardar_arquivo = FileSensor(
        task_id='aguardar_arquivo_csv',
        filepath=os.path.join(DATA_DIR, 'transacoes_nogtech.csv'),
        poke_interval=15, 
        timeout=600,      
        mode='poke'
    )

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    task_aggregate = PythonOperator(
        task_id='aggregate_metrics',
        python_callable=aggregate_metrics,
    )

    # Fluxo do Grafo de Tarefas (Observabilidade clara na Interface Web)
    aguardar_arquivo >> task_extract >> task_transform >> task_load >> task_aggregate