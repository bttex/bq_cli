#!/usr/bin/env python3
import typer
from typing_extensions import Annotated
import os
import sys
from typing import List, Tuple, Optional

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError


def parse_table_id_using_defaults(
    table_id: str,
    default_project: Optional[str] = None,
    default_dataset: Optional[str] = None,
) -> Tuple[str, str, str]:
    """
    Aceita:
      - project.dataset.table
      - dataset.table   (usa default_project)
      - table           (usa default_project e default_dataset)
    """
    parts = table_id.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        if not default_project:
            raise ValueError("Quando usar 'dataset.table', informe --project-id.")
        return default_project, parts[0], parts[1]
    if len(parts) == 1:
        if not default_project or not default_dataset:
            raise ValueError(
                "Quando usar apenas 'table', informe --project-id e --dataset."
            )
        return default_project, default_dataset, parts[0]
    raise ValueError(f"Formato inválido de table-id: {table_id}")


def get_columns_from_csv(csv_path: str, sep: str, encoding: str) -> List[str]:
    df = pd.read_csv(csv_path, nrows=0, sep=sep, encoding=encoding)
    return df.columns.tolist()


def generate_create_table_sql(
    project_id: str,
    dataset_id: str,
    table_name: str,
    columns: List[str],
    replace: bool = False,
) -> str:
    fields = ",\n  ".join([f"`{c}` STRING" for c in columns])
    verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    sql = f"""{verb} `{project_id}.{dataset_id}.{table_name}` (
  {fields}
);"""
    return sql


def run_query(client: bigquery.Client, sql: str):
    job = client.query(sql)
    return job.result()


def table_exists(client: bigquery.Client, full_table_id: str) -> bool:
    try:
        client.get_table(full_table_id)
        return True
    except NotFound:
        return False


def upload_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    full_table_id: str,
) -> int:
    """
    - Garante que todas as colunas do df existam e na mesma ordem da tabela no BQ
    - Converte todos os valores para string e normaliza 'nan'/'None' -> NULL
    - Faz append na tabela
    """
    table = client.get_table(full_table_id)
    bq_columns = [field.name for field in table.schema]

    # Adiciona colunas faltantes
    for col in bq_columns:
        if col not in df.columns:
            df[col] = None

    # Mantém apenas as colunas da tabela e na mesma ordem
    df_filtered = df[bq_columns].copy()

    # Converte tudo para string e normaliza nulos
    df_filtered = df_filtered.astype(str)
    df_filtered = df_filtered.replace(["Nan", "NaN", "nan", "None"], None)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    load_job = client.load_table_from_dataframe(
        df_filtered, full_table_id, job_config=job_config
    )
    load_job.result()  # espera terminar

    # Tenta obter linhas carregadas (fallback para len do df)
    rows = getattr(load_job, "output_rows", None)
    return int(rows) if rows is not None else int(len(df_filtered))


def main(
    csv: Annotated[
        Optional[str], typer.Option("-c", "--csv", help="Caminho para o CSV")
    ] = None,
    table_id: Annotated[
        Optional[str],
        typer.Option(
            "-t", "--table-id", help="Tabela no formato [project.]dataset.table"
        ),
    ] = None,
    project_id: Annotated[
        Optional[str],
        typer.Option(help="Project ID (usado se table-id não inclui o project)"),
    ] = None,
    dataset: Annotated[
        Optional[str], typer.Option(help="Dataset (caso não use --table-id completo)")
    ] = None,
    table_name: Annotated[
        Optional[str], typer.Option(help="Nome da tabela (caso não use --table-id)")
    ] = None,
    mode: Annotated[
        str, typer.Option(help="Ação a realizar: create, upload ou both (padrão: both)")
    ] = "both",
    sep: Annotated[str, typer.Option(help="Separador do CSV (padrão: ;)")] = ";",
    encoding: Annotated[
        str, typer.Option(help="Encoding do CSV (padrão: utf-8-sig)")
    ] = "utf-8-sig",
    credentials: Annotated[
        Optional[str], typer.Option(help="Caminho para o JSON de credenciais do GCP")
    ] = None,
    replace: Annotated[
        bool,
        typer.Option("--replace", help="Usa CREATE OR REPLACE TABLE ao criar a tabela"),
    ] = False,
    print_sql: Annotated[
        bool, typer.Option(help="Apenas imprime o SQL de criação (não executa)")
    ] = False,
):
    """
    CLI para criar tabela no BigQuery a partir de um CSV (colunas STRING) e/ou enviar dados.
    """

    # Agora, em vez de 'args.csv', usamos a variável 'csv'
    # Em vez de 'args.credentials', usamos a variável 'credentials'
    # etc.

    if credentials:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials

    # Validação do CSV conforme o modo
    # A validação de 'choices' do argparse se foi, então podemos adicionar uma.
    # E a validação de obrigatoriedade do CSV.

    valid_modes = ["create", "upload", "both"]
    if mode not in valid_modes:
        print(f"❌ Erro: --mode deve ser um de {valid_modes}")
        raise typer.Exit(code=1)

    if mode in ("create", "upload", "both") and not csv:
        print("❌ Erro: --csv é obrigatório para o modo selecionado.")
        # Usamos typer.Exit() em vez de parser.error()
        raise typer.Exit(code=1)

    # Cria cliente (usa project passado ou o padrão do ambiente)
    try:
        # Usamos a variável 'project_id'
        client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Erro ao criar cliente BigQuery: {e}")
        sys.exit(1)

    # Normaliza project/dataset/table
    default_project = project_id or client.project
    try:
        # Usamos as variáveis 'table_id', 'dataset', 'table_name'
        if table_id:
            project_id, dataset_id, table_name = parse_table_id_using_defaults(
                table_id,
                default_project=default_project,
                default_dataset=dataset,
            )
        else:
            if not dataset or not table_name:
                print("❌ Erro: Informe --table-id OU (--dataset e --table-name).")
                raise typer.Exit(code=1)  # <- Substitui parser.error
            project_id, dataset_id, table_name = (
                default_project,
                dataset,
                table_name,
            )
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(2)

    full_table_id = f"{project_id}.{dataset_id}.{table_name}"

    # CREATE
    if mode in ("create", "both"):
        try:
            # Usamos as variáveis 'csv', 'sep', 'encoding'
            columns = get_columns_from_csv(csv, sep=sep, encoding=encoding)
        except Exception as e:
            print(f"❌ Erro ao ler cabeçalho do CSV: {e}")
            sys.exit(3)

        # Usamos a variável 'replace'
        sql = generate_create_table_sql(
            project_id, dataset_id, table_name, columns, replace=replace
        )
        print("🧱 SQL de criação da tabela:")
        print(sql)
        print()

        if not print_sql:  # Usamos a variável 'print_sql'
            try:
                run_query(client, sql)
                print(f"✅ Tabela `{full_table_id}` criada com sucesso.")
            except BadRequest as e:
                print(f"❌ Erro de SQL ao criar tabela: {e}")
                sys.exit(4)
            except GoogleAPICallError as e:
                print(f"❌ Erro na API do BigQuery ao criar tabela: {e}")
                sys.exit(5)

    # UPLOAD
    if mode in ("upload", "both"):
        if not table_exists(client, full_table_id):
            print(f"❌ A tabela `{full_table_id}` não existe.")
            if mode == "both":
                print(
                    "Observação: era esperado que a criação tivesse ocorrido antes do upload."
                )
            else:
                print("Dica: rode com --mode both para criar e enviar de uma vez.")
            sys.exit(6)

        try:
            # Usamos 'csv', 'sep', 'encoding'
            df = pd.read_csv(csv, sep=sep, encoding=encoding)
        except Exception as e:
            print(f"❌ Erro ao ler o CSV: {e}")
            sys.exit(7)

        try:
            rows = upload_to_bigquery(client, df, full_table_id)
            print(f"🚀 {rows} linhas enviadas para `{full_table_id}` com sucesso!")
        except BadRequest as e:
            print(f"❌ Erro ao enviar dados (BadRequest): {e}")
            sys.exit(8)
        except GoogleAPICallError as e:
            print(f"❌ Erro na API do BigQuery ao enviar dados: {e}")
            sys.exit(9)


if __name__ == "__main__":
    typer.run(main)
