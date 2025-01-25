import json
import pandas as pd
from clickhouse_driver import Client
import pyarrow.parquet as pq


def create_clickhouse_table(config_file):
    # Read the config file
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Extract ClickHouse table configuration
    clickhouse_table_name = config['clickhouse_table']['name']
    clickhouse_engine = config['clickhouse_table']['engine']
    clickhouse_order_by = config['clickhouse_table']['order_by']
    clickhouse_fields = config['clickhouse_table']['fields']

    # Add 'dna_id' field as the first column
    clickhouse_fields = {"dna_id": "Int32", **clickhouse_fields}

    # Create ClickHouse client
    client = Client('localhost')

    # Construct the SQL query to create the table
    fields = ', '.join([f"{column} {data_type}" for column, data_type in clickhouse_fields.items()])
    query = f"""
    CREATE TABLE IF NOT EXISTS `{clickhouse_table_name}` (
        {fields}
    ) ENGINE = {clickhouse_engine}
    ORDER BY {clickhouse_order_by}
    """

    # Execute the query to create the table
    client.execute(query)
    print(f"ClickHouse table '{clickhouse_table_name}' created successfully.")


def insert_data_into_clickhouse(parquet_file_path, config_file):
    # Read the Parquet file using pandas
    df = pd.read_parquet(parquet_file_path)

    # Add a 'dna_id' column with sequential integers
    df['dna_id'] = range(1, len(df) + 1)

    # Read the config file
    with open(config_file, 'r') as f:
        config = json.load(f)

    # Extract ClickHouse table configuration
    clickhouse_table_name = config['clickhouse_table']['name']
    clickhouse_fields = config['clickhouse_table']['fields']

    # Add 'dna_id' to the columns in config
    columns = ['dna_id'] + list(clickhouse_fields.keys())  # Include 'dna_id' as the first column

    # Prepare data for insertion into ClickHouse
    values = df[columns].values.tolist()  # Extract the rows corresponding to the columns

    # Create ClickHouse client
    client = Client('localhost')

    # Insert data into ClickHouse
    insert_query = f"INSERT INTO `{clickhouse_table_name}` ({', '.join(columns)}) VALUES"
    client.execute(insert_query, values)
    print(f"Data from Parquet file '{parquet_file_path}' inserted into '{clickhouse_table_name}'.")


def main(config_file, parquet_file_path):
    create_clickhouse_table(config_file)
    insert_data_into_clickhouse(parquet_file_path, config_file)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python create_clickhouse_table.py <config_file> <parquet_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    parquet_file_path = sys.argv[2]

    main(config_file, parquet_file_path)
