# Pipeline de ETL e Dashboard Analítico de Vendas (NogTech) 🚀

![Python](https://img.shields.io/badge/python-3.12-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-teal.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg)
![Metabase](https://img.shields.io/badge/Metabase-0.49-royalpurple.svg)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg)

Este projeto apresenta uma solução completa de Engenharia de Dados de ponta a ponta, desenvolvida para processar, enriquecer e analisar os dados de vendas e engajamento da plataforma educacional **NogTech**. A arquitetura foi totalmente containerizada e orquestrada de forma profissional com **Apache Airflow**, garantindo robustez, idempotência e governança de dados (LGPD).

---

## 🏗️ Arquitetura do Ecossistema

O ecossistema é baseado em microserviços isolados e integrados via **Docker Compose**, minimizando o consumo de recursos locais e garantindo portabilidade absoluta:

> **[Fontes Brutas]** (CSV / JSON) 
> ➡️ **[Apache Airflow]** (Orquestração / Extração / Transformação / Carga) 
> ➡️ **[Data Lake Local]** (Parquet Particionado + SQLite) 
> ➡️ **[Metabase]** (Dashboard C-Level)

### Componentes Utilizados:
* **Apache Airflow (v2.9.1):** Responsável pelo agendamento, monitoramento, interface de observabilidade (DAGs) e gestão de dependências.
* **Pandas & PyArrow:** Motores de processamento em memória e transformação de dados altamente tipados.
* **SQLite (Camada Analítica):** Banco de dados leve embarcado mapeado em disco para servir como repositório final de consumo.
* **Metabase (v0.49):** Ferramenta de Business Intelligence para visualização dinâmica de dados.

---

## ⚡ Estratégias Técnicas do Pipeline (ETL)

O pipeline implementado na DAG `etl_vendas_diarias` atende a todos os requisitos de negócio, focando em engenharia de software de alta performance:

### 1. Extração (Extract) e Cruzamento (Join)
* A ingestão das fontes locais (CSV `latin-1` e JSON `utf-8`) é feita de forma automatizada.
* **Padronização prévia:** Foi implementada a reconstrução cronológica do mês de referência (formato `YYYY-MM`) e a limpeza dos CPFs antes do cruzamento (`LEFT JOIN`). Isso garantiu que nenhuma transação perdesse os dados de engajamento por falha de formatação.

### 2. Transformação (Transform) e Resiliência
* **Anonimização (LGPD):** Remoção definitiva da coluna de identificação direta (`nome_aluno`) e mascaramento rigoroso do CPF mantendo os 6 dígitos centrais (`***.XXX.XXX-**`).
* **Enriquecimento via BrasilAPI com Cache:** Integração para tradução de CEPs (Bairro, Cidade, Estado) e análise do calendário de Feriados Nacionais.
* **🛡️ Tratamento de Erros e Uso de Cache (Requisito Crítico):** Para evitar sobrecarga de rede e respeitar o limite da BrasilAPI pública, foram implementados **Caches em Memória** (Dicionários Python e Listas). A API é chamada apenas uma vez por CEP único ou Ano único. Além disso, foi aplicada a lógica de **Resiliência (Try/Except)**: caso a API caia, sofra *timeout* ou o CEP seja inválido, o pipeline não quebra. O algoritmo desvia o fluxo e cataloga os campos como `NI` (Não Informado), preservando os dados financeiros da transação.

### 3. Carga (Load) e Idempotência
* **Estratégia de Idempotência Adotada:** A opção escolhida foi o **Particionamento por data com Overwrite (Sobrescrita Limpa)**.
* **Justificativa:** Diferente de um UPSERT em banco de dados que exige constante verificação de chaves linha a linha (custoso para Big Data), a sobrescrita de partições (`shutil.rmtree`) garante que o diretório legado do respectivo Ano/Mês seja completamente higienizado antes de salvar os novos arquivos `.parquet`. Isso permite que o pipeline rode infinitas vezes no mesmo lote mantendo a consistência do *Data Lake* livre de poluição ou duplicidades, sendo a estratégia mais performática para processamento de arquivos distribuídos.

---

## 📊 Insights de Negócio Revelados

Ao plugar o Metabase na camada de consumo, o dashboard gerou descobertas críticas:
* **O Paradoxo do Suporte:** A empresa registrou cerca de 4,1 mil vendas totais, mas gerou uma volumetria de **4,7 mil Tickets de Suporte**. Há mais de um chamado por cliente cadastrado.
* **Depreciação do NPS:** Essa sobrecarga operacional reflete perfeitamente no **NPS Geral crítico de 2.11**.
* **Alerta de Churn (Cancelamento):** Alunos que avaliaram o curso com nota zero assistiram, em média, a apenas **8.7 horas**, enquanto o restante dos alunos manteve consumo acima de 65 horas. O atrito inicial com a plataforma está gerando cancelamentos rápidos.

---

## 🛠️ Como Executar o Projeto Localmente

### Passo 1: Inicializar a Infraestrutura
Certifique-se de que o Docker está em execução. Na raiz do projeto (onde está o arquivo `docker-compose.yml`), abra o terminal e inicialize os contêineres:

    docker-compose up -d

### Passo 2: Permissões de Disco (Acesso a Volumes)
Para que o ambiente isolado do Docker consiga escrever os arquivos Parquet na sua máquina física, as permissões variam de acordo com o Sistema Operacional:
* **Linux / macOS:** É necessário conceder permissão de escrita local. No terminal, execute:

    sudo chmod -R 777 data/

* **Windows:** O *Docker Desktop* gerencia as permissões de pastas automaticamente nativo ou via WSL 2. Nenhuma configuração extra de linha de comando é necessária nesta etapa.

### Passo 3: Executar a DAG e Observabilidade
1. Acesse a interface visual do Airflow em `http://localhost:8080` (credenciais padrões configuradas no docker-compose).
2. Ative o *toggle* da DAG `etl_vendas_diarias` e clique em **Trigger DAG** (ícone de Play). Acompanhe o grafo de execução até a conclusão dos nós (status verde).

### Passo 4: Visualizar no Metabase
1. Acesse o Metabase em `http://localhost:3000`.
2. Conecte ao banco de dados escolhendo a opção SQLite e apontando para o arquivo físico gerado no contêiner: `/opt/airflow/data/data_lake.db`.
3. (Opcional) Adicione o GeoJSON dos estados do Brasil nas configurações de *Admin* para habilitar o Mapa de Calor regional.

---
*Projeto desenvolvido como portfólio de Engenharia de Dados e Business Intelligence.*