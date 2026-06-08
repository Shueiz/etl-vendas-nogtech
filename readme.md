# Pipeline de ETL e Dashboard Analítico de Vendas (NogTech) 🚀

![Python](https://img.shields.io/badge/python-3.12-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-teal.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg)
![Metabase](https://img.shields.io/badge/Metabase-0.49-royalpurple.svg)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg)

Este projeto apresenta uma solução completa de Engenharia de Dados de ponta a ponta, desenvolvida para processar, enriquecer e analisar os dados de vendas e engajamento da plataforma educacional **NogTech**. A arquitetura foi totalmente containerizada e orquestrada de forma profissional, garantindo robustez, idempotência e governança de dados (LGPD).

---

## 🏗️ Arquitetura do Ecossistema

O ecossistema é baseado em microserviços isolados e integrados via **Docker Compose**, minimizando o consumo de recursos locais e garantindo portabilidade absoluta:

> **[Fontes Brutas]** (CSV / JSON) 
> ➡️ **[Apache Airflow]** (Orquestração / Extração / Transformação / Carga) 
> ➡️ **[Data Lake Local]** (Parquet Particionado + SQLite) 
> ➡️ **[Metabase]** (Dashboard C-Level)

### Componentes Utilizados:
* **Apache Airflow (v2.9.1):** Responsável pelo agendamento, monitoramento e execução acíclica direta (DAG) das etapas do pipeline.
* **Pandas & PyArrow:** Motores de processamento paralelo e transformação de dados altamente tipados.
* **SQLite (Camada Analítica):** Banco de dados leve embarcado mapeado em disco para servir como o repositório final de consumo.
* **Metabase (v0.49):** Ferramenta de Business Intelligence conectada à camada analítica para visualização dinâmica de dados.

---

## ⚡ Detalhes Técnicos do Pipeline (ETL)

O pipeline implementado na DAG `etl_vendas_diarias` resolveu dores complexas de qualidade de dados:

1.  **Extração Corretiva:** Reconstrução cronológica uniforme de datas e padronização prévia de chaves (CPFs) antes do cruzamento (`LEFT JOIN`), evitando perdas de métricas de engajamento.
2.  **Transformação e Governança:**
    * **Anonimização (LGPD):** Remoção de dados sensíveis e mascaramento de CPF (`***.XXX.XXX-**`).
    * **Enriquecimento com BrasilAPI:** Consulta assíncrona para tradução de CEPs em Estados reais e descoberta de vendas em Feriados Nacionais.
    * **Tipagem Estrita:** Limpeza de caracteres monetários e strings malformadas, garantindo integridade para o formato Parquet.
3.  **Carga Inteligente (Idempotência):** Mecanismo de auto-higienização (`shutil.rmtree`) que limpa partições antigas/poluídas antes de persistir o novo lote particionado fisicamente por **Ano** e **Mês**.

---

## 📊 Insights de Negócio Revelados

Ao cruzar os dados de vendas com o engajamento na camada analítica, o dashboard gerou descobertas críticas para a tomada de decisão executiva:

* **O Paradoxo do Suporte:** A empresa registrou cerca de 4,1 mil vendas totais, mas gerou uma volumetria de **4,7 mil Tickets de Suporte**. Há mais de um chamado por cliente cadastrado.
* **Depreciação do NPS:** Essa sobrecarga operacional reflete perfeitamente no **NPS Geral crítico de 2.11** (Zona de Detração máxima).
* **Alerta de Churn (Cancelamento):** Alunos que avaliaram o curso com nota zero assistiram, em média, a apenas **8.7 horas**, enquanto o restante dos alunos manteve consumo estável acima de 65 horas, provando que o atrito tecnológico inicial está afastando os clientes.

---

## 🛠️ Como Executar o Projeto Localmente

### Passo 1: Inicializar a Infraestrutura
Na raiz do projeto (onde está o `docker-compose.yml`), execute:
```bash
docker-compose up -d
```

### Passo 2: Permissões de Disco
Como o Docker roda em ambiente isolado, garanta que ele pode escrever os arquivos Parquet na sua máquina:
```bash
sudo chmod -R 777 data/
```

### Passo 3: Executar a DAG
1. Acesse o Airflow em `http://localhost:8080` (credenciais padrão do compose).
2. Ative o toggle da DAG `etl_vendas_diarias` e clique em **Trigger DAG** (Play). Aguarde todas as tarefas ficarem verdes.

### Passo 4: Visualizar no Metabase
1. Acesse o Metabase em `http://localhost:3000`.
2. Conecte ao banco SQLite apontando para o mapeamento do contêiner: `/opt/airflow/data/data_lake.db` (ou o nome do seu arquivo `.db` configurado).
3. (Opcional) Adicione o GeoJSON dos estados do Brasil nas configurações de Admin para habilitar o Mapa de Calor regional.

---
*Projeto desenvolvido como portfólio de Engenharia de Dados e Business Intelligence.*