# BigQuery CSV Uploader

CLI para criar tabelas no BigQuery a partir de arquivos CSV e fazer upload de dados. Todas as colunas são criadas como tipo STRING.

## 📋 Requisitos

- Python 3.6+
- Bibliotecas:
  ```bash
  pip install pandas google-cloud-bigquery
  ```
- Credenciais do Google Cloud Platform (JSON)
- Permissões necessárias no BigQuery:
  - `bigquery.tables.create`
  - `bigquery.tables.get`
  - `bigquery.tables.updateData`

## 🚀 Instalação

```bash
# Clone ou baixe o script
chmod +x script.py

# Instale as dependências
pip install pandas google-cloud-bigquery
```

## 📖 Uso Básico

### Sintaxe Geral

```bash
python script.py [opções]
```

### Principais Argumentos

| Argumento | Descrição | Obrigatório |
|-----------|-----------|-------------|
| `-c`, `--csv` | Caminho para o arquivo CSV | Sim (exceto com `--print-sql`) |
| `-t`, `--table-id` | Identificador da tabela no formato `[project.]dataset.table` | Condicional* |
| `--project-id` | ID do projeto GCP | Não** |
| `--dataset` | Nome do dataset | Condicional* |
| `--table-name` | Nome da tabela | Condicional* |
| `--mode` | Ação: `create`, `upload` ou `both` (padrão: `both`) | Não |
| `--sep` | Separador do CSV (padrão: `;`) | Não |
| `--encoding` | Encoding do CSV (padrão: `utf-8-sig`) | Não |
| `--credentials` | Caminho para o JSON de credenciais | Não** |
| `--replace` | Usa `CREATE OR REPLACE TABLE` | Não |
| `--print-sql` | Apenas imprime o SQL sem executar | Não |

\* Você pode usar `--table-id` OU a combinação `--dataset` + `--table-name`  
\*\* Se não informado, usa as credenciais padrão do ambiente

## 💡 Exemplos de Uso

### 1. Criar tabela e fazer upload (modo padrão)

```bash
python script.py \
  --csv dados.csv \
  --table-id meu-projeto.meu_dataset.minha_tabela \
  --credentials credenciais.json
```

### 2. Apenas criar a tabela (sem upload)

```bash
python script.py \
  --csv dados.csv \
  --table-id meu-projeto.meu_dataset.minha_tabela \
  --mode create \
  --credentials credenciais.json
```

### 3. Apenas fazer upload (tabela já existe)

```bash
python script.py \
  --csv dados.csv \
  --table-id meu-projeto.meu_dataset.minha_tabela \
  --mode upload \
  --credentials credenciais.json
```

### 4. Criar/substituir tabela existente

```bash
python script.py \
  --csv dados.csv \
  --table-id meu-projeto.meu_dataset.minha_tabela \
  --replace \
  --credentials credenciais.json
```

### 5. Usando formato de table-id simplificado

```bash
# Apenas dataset.tabela (usa project-id informado)
python script.py \
  --csv dados.csv \
  --table-id meu_dataset.minha_tabela \
  --project-id meu-projeto

# Apenas nome da tabela (usa project-id e dataset informados)
python script.py \
  --csv dados.csv \
  --table-name minha_tabela \
  --project-id meu-projeto \
  --dataset meu_dataset
```

### 6. CSV com vírgula como separador

```bash
python script.py \
  --csv dados.csv \
  --table-id dataset.tabela \
  --sep "," \
  --encoding utf-8
```

### 7. Visualizar SQL sem executar

```bash
python script.py \
  --csv dados.csv \
  --table-id dataset.tabela \
  --print-sql
```

## 🔧 Configuração de Credenciais

### Opção 1: Via argumento

```bash
python script.py --credentials /caminho/para/credenciais.json ...
```

### Opção 2: Variável de ambiente

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/caminho/para/credenciais.json"
python script.py ...
```

### Opção 3: Credenciais padrão do gcloud

```bash
gcloud auth application-default login
python script.py ...
```

## 📊 Comportamento do Script

### Criação de Tabela (`mode=create`)

- Lê o cabeçalho do CSV
- Cria todas as colunas como tipo STRING
- Por padrão, falha se a tabela já existir (use `--replace` para sobrescrever)

### Upload de Dados (`mode=upload`)

- Verifica se a tabela existe
- Adiciona colunas faltantes no DataFrame (preenchidas com NULL)
- Remove colunas do DataFrame que não existem na tabela
- Converte todos os valores para STRING
- Normaliza valores nulos (`nan`, `NaN`, `None`) para NULL
- Faz APPEND dos dados (não substitui dados existentes)

### Modo Both (`mode=both`)

- Executa criação seguida de upload
- Útil para processar tudo de uma vez

## ⚠️ Notas Importantes

1. **Todas as colunas são STRING**: O script cria todas as colunas como tipo STRING para máxima compatibilidade
2. **Append mode**: O upload sempre adiciona dados, nunca substitui
3. **Normalização de nulos**: Valores como "nan", "NaN", "None" são convertidos para NULL
4. **Ordem das colunas**: O script garante que a ordem das colunas do CSV corresponda à tabela
5. **Encoding padrão**: O padrão é `utf-8-sig` para lidar com BOM em arquivos Excel

## 🐛 Resolução de Problemas

### Erro: "Quando usar 'dataset.table', informe --project-id"

**Solução**: Forneça o `--project-id` ou use o formato completo `project.dataset.table`

### Erro: "A tabela não existe"

**Solução**: Use `--mode both` ou crie a tabela primeiro com `--mode create`

### Erro ao ler CSV

**Solução**: Verifique o separador (`--sep`) e encoding (`--encoding`)

### Erro de permissão

**Solução**: Verifique se as credenciais têm as permissões necessárias no BigQuery

## 📝 Códigos de Saída

| Código | Descrição |
|--------|-----------|
| 0 | Sucesso |
| 1 | Erro ao criar cliente BigQuery |
| 2 | Formato inválido de table-id |
| 3 | Erro ao ler cabeçalho do CSV |
| 4 | Erro de SQL ao criar tabela |
| 5 | Erro na API ao criar tabela |
| 6 | Tabela não existe (modo upload) |
| 7 | Erro ao ler CSV completo |
| 8 | Erro ao enviar dados (BadRequest) |
| 9 | Erro na API ao enviar dados |

## 📄 Licença

Este script é fornecido como está, sem garantias.