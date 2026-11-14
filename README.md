# Engenharia de Dados aplicada ao Setor de Telecom: Construindo um Data Lake para Cálculo de PDD em Telecom

Compartilho o resultado da primeira fase de um projeto prático focado em resolver um desafio crítico no setor de Telecomunicações: o cálculo automatizado e a mensuração da **Provisão para Devedores Duvidosos (PDD)**.

### 📌 O que é PDD e qual seu impacto em Telecom?

A **PDD** é uma reserva contábil obrigatória que previne perdas financeiras decorrentes da inadimplência de clientes. Em Telecom, empresa com milhões de assinaturas recorrentes e alto volume transacional diário, gerenciar o risco de crédito exige um controle preciso da **Aging List** (faixas de atraso dos títulos em aberto). Sem dados limpos e pipelines performáticos, o risco de distorção na DRE contábil é elevado.

### ☁️ Arquitetura da Solução na AWS

Para processar esses volumes de forma escalável, adotei a **Arquitetura Medallion (Bronze, Silver e Gold)** com serviços gerenciados da AWS:

```
[ Fontes de Dados ] (Sistemas de Billing, CRM, Legacy)
        │
        ▼ (S3 Put)
[ S3 Bucket Raw / Bronze ] (CSVs brutos)
        │
        ▼ (Leitura & Tipagem via PySpark)
[ Amazon EMR ]
        │
        ▼ (Escrita Parquet Particionado por Ano/Mês)
[ S3 Bucket Silver / Trusted ]
        │
        ▼ (Spark SQL & Left Join Faturas x Pagamentos)
[ Amazon EMR ]
        │
        ▼ (Aging List & Matriz de Provisão)
[ S3 Bucket Gold / Business ]
        │
        ▼
[ AWS Glue Data Catalog ] ──► [ Amazon Athena ] ──► [ Consumer / BI / Analytics ]

```

* **Ingestão & Camada Bronze (Raw):** Armazenamento em arquivos CSV brutos no **Amazon S3**.

* **Camada Silver (Trusted):** Leitura, limpeza de schema, validação e tipagem forte (`to_date`, `decimal`, etc.) via **PySpark** no **Amazon EMR**, persistidos em formato **Apache Parquet** particionado por `ano` e `mes`.

* **Camada Gold (Business):** Processamento analítico em **Spark SQL** executando cruzamentos (`LEFT JOIN`) entre faturas e pagamentos para classificar o atraso em faixas de risco (1-30, 31-60, 61-90 e 90+ dias) e aplicar as taxas de provisão gerenciais.

* **Governança & Consumo:** Catálogo unificado com **AWS Glue Data Catalog** e consultas interativas via **Amazon Athena**.

### 📊 Resultado Obtido e Aplicações Práticas

Executando os scripts no **Amazon EMR**, chegamos ao Datamart executivo consolidado da camada Gold:

```
+----------------+-----------+------------------+---------------+---------------------+
|   faixa_atraso |qtd_faturas|      valor_aberto|  taxa_provisao|  provisao_necessaria|
+----------------+-----------+------------------+---------------+---------------------+
|    1 a 30 dias |          1|            129.90|             2%|                 2.60|
|   31 a 60 dias |          3|            289.70|            10%|                28.97|
|   61 a 90 dias |         22|          2,067.80|            50%|             1,033.90|
|Mais de 90 dias |       1476|        151,052.40|           100%|           151,052.40|
+----------------+-----------+------------------+---------------+---------------------+

```

**Aplicações:**

1. **Contabilidade & DRE:** Suporte direto ao fechamento mensal financeiro com provisão calculada de forma auditável.

2. **Gestão de Risco:** Visibilidade clara de onde está concentrada a inadimplência (faixa 90+ dias representando o maior volume de provisionamento).

### 🔮 Próximos Passos (Ciência & Análise de Dados)

Este projeto continua! Nas próximas fases pretendo evoluir a solução explorando:

* 📈 **Analytics & BI:** Criação de dashboards executivos interativos via **Amazon QuickSight** / Power BI para visualização de safra e evolução do aging.

* 🤖 **Ciência de Dados & ML:** Construção de modelos preditivos no **Amazon SageMaker** para identificação precoce de clientes com propensão ao *Default* (inadimplência) antes do vencimento da fatura.

Seguimos aprendendo e evoluindo a cada pipeline! Sugestões e feedbacks de arquitetura são sempre muito bem-vindos. 💡

#EngenhariaDeDados #DataEngineering #AWS #PySpark #SparkSQL #AmazonEMR #AmazonS3 #DataLake #PDD #Telecom #MedallionArchitecture #BigData #LearningInPublic
