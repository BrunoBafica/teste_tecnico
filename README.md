# Teste Técnico — Analista de Dados / Business Intelligence

## Descrição geral

Este repositório contém a solução do teste técnico proposto para análise de dados no contexto da Secretaria Nacional de Habitação (SNH), contemplando:

- consultas SQL para responder às questões obrigatórias da Etapa 1;
- tratamento e validação dos dados com Python e pandas;
- geração de indicadores analíticos da Etapa 3;
- exportação dos datasets tratados e dos resultados analíticos para a pasta `outputs/`.

A estrutura foi organizada para separar claramente os insumos, as consultas SQL, os scripts Python e os arquivos gerados ao longo do processo.

---

## Estrutura do projeto

```bash
teste_tecnico/
├── dados/
│   ├── beneficiarios.csv
│   ├── municipios.csv
│   ├── pagamentos.csv
│   └── projetos.csv
├── outputs/
│   ├── beneficiarios_tratados.csv
│   ├── municipios_tratados.csv
│   ├── pagamentos_tratados.csv
│   ├── projetos_tratados.csv
│   ├── indicador_a_eficiencia_uf.csv
│   ├── indicador_b_modalidade.csv
│   ├── indicador_b_resumo.csv
│   ├── indicador_b_top10_municipios.csv
│   ├── indicador_c_deficit_vs_entregas.csv
│   ├── indicador_d_risco_projetos.csv
│   └── relatorio_final.csv
├── python/
│   ├── analise.py
│   └── tratamento.py
├── sql/
│   └── consultas.sql
└── requirements.txt
```

---

## Tecnologias utilizadas

- Python 3.x
- pandas
- SQLite

---

## Pré-requisitos

Antes de executar o projeto, tenha instalado:

- Python 3.10+
- pip

---

## Instalação das dependências

Instale as dependências com:

```bash
pip install -r requirements.txt
```

---

## Como executar

### 1) Tratamento dos dados

Esse script:

- carrega os 4 arquivos CSV da pasta `dados/`;
- aplica regras de validação e tratamento;
- gera os arquivos tratados na pasta `outputs/`;
- imprime um log resumido das inconsistências encontradas.

```bash
python python/tratamento.py
```

### 2) Análise e geração dos indicadores

Esse script utiliza os arquivos tratados gerados na etapa anterior para calcular os indicadores analíticos e exportar os resultados para a pasta `outputs/`.

```bash
python python/analise.py
```

### Ordem correta de execução

```bash
python python/tratamento.py
python python/analise.py
```

---

## Ferramenta SQL utilizada

As consultas da Etapa 1 foram escritas em sintaxe compatível com **SQLite**, utilizando funções como `julianday()` e `date('now')`.

Arquivo:

```bash
sql/consultas.sql
```

### Como executar no SQLite

Depois de carregar as tabelas no banco, execute:

```bash
sqlite3 habitacao.db
.read sql/consultas.sql
```

---

## Decisões tomadas no tratamento de dados

### 1. Datas inválidas ou inconsistentes
As colunas de data foram convertidas com `errors="coerce"` para evitar falhas na execução e permitir tratamento posterior de valores inválidos.

### 2. Inconsistência entre `dt_conclusao_real` e `situacao`
Projetos com `dt_conclusao_real` preenchida, mas situação diferente de `Concluído`, foram sinalizados na coluna `flag_inconsistencia`.

### 3. `valor_repassado` maior que `valor_total`
Quando identificado repasse superior ao valor contratado, o caso foi registrado como inconsistência e o valor repassado foi ajustado para o limite de `valor_total`.

### 4. Validação de CPF
Os CPFs foram validados com base na máscara `XXX.XXX.XXX-XX`, gerando a coluna `flag_cpf_invalido`.

### 5. Faixa de renda
A `faixa_renda` foi recalculada com base na coluna `renda_mensal`, preservando a informação original em `faixa_renda_original` e sinalizando divergências em `flag_faixa_renda_inconsistente`.

### 6. Pagamentos com data anterior ao início do projeto
Pagamentos com `dt_pagamento` anterior à `dt_inicio` do projeto foram sinalizados na coluna `flag_data_invalida`.

### 7. Campos-chave nulos
Os registros com valores nulos em campos-chave não foram removidos. Apenas foi registrada a contagem no log, conforme solicitado no teste.

---

## Critérios de risco definidos no Indicador D

A classificação de risco foi aplicada aos projetos ativos (`Em execução` ou `Contratado`) com base em:

1. percentual de repasse já realizado;
2. atraso em relação à data prevista de conclusão;
3. valor total do projeto.

### Regras utilizadas

**Risco Alto**
- projeto atrasado com percentual de repasse inferior a 50%; ou
- projeto atrasado e com valor total elevado.

**Risco Médio**
- projeto com pelo menos um sinal relevante de atenção:
  - atraso; ou
  - repasse abaixo de 50%; ou
  - valor total elevado.

**Risco Baixo**
- projeto sem atraso, com repasse adequado e sem criticidade relevante pelo valor total.

### Regra prática para valor elevado

```python
valor_total >= 10_000_000
```

---

## Limitações identificadas

- o dataset não traz documentação completa de todas as colunas, o que exige inferência em parte das regras;
- a qualidade dos dados pode impactar os indicadores, especialmente em casos de campos nulos, datas inválidas e classificações textuais inconsistentes;
- os resultados dependem da consistência dos relacionamentos por `id_projeto` e `id_municipio`;
- o repositório não inclui etapa opcional de dashboard ou automação em Excel.

---

## Saídas geradas

Após a execução, são gerados:

### Dados tratados
- `municipios_tratados.csv`
- `projetos_tratados.csv`
- `beneficiarios_tratados.csv`
- `pagamentos_tratados.csv`

### Indicadores analíticos
- `indicador_a_eficiencia_uf.csv`
- `indicador_b_top10_municipios.csv`
- `indicador_b_modalidade.csv`
- `indicador_b_resumo.csv`
- `indicador_c_deficit_vs_entregas.csv`
- `indicador_d_risco_projetos.csv`

### Relatório consolidado
- `relatorio_final.csv`
