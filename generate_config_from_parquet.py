import json
import os
import sys
import pandas as pd

def generate_config_from_parquet(file_path, output_file_path):
    # Extract endpoint and ClickHouse table name from the file name
    file_name = os.path.basename(file_path)
    endpoint = os.path.splitext(file_name)[0]
    clickhouse_table_name = endpoint.lower()

    # Read the Parquet file
    df = pd.read_parquet(file_path)

    # Extract fields and their types from the DataFrame
    model_fields = {}
    clickhouse_fields = {}
    for column, dtype in df.dtypes.items():
        model_field = map_dtype_to_django_field(column, dtype)
        clickhouse_field = map_dtype_to_clickhouse_field(column, dtype)
        model_fields[column] = model_field
        clickhouse_fields[column] = clickhouse_field

    # Define the configuration
    config = {
        "app_name": "myapp",
        "model_name": "ComplicatedModel",
        "model_fields": model_fields,
        "serializer_name": "ComplicatedModelSerializer",
        "view_name": "ComplicatedModelViewSet",
        "url_path": endpoint,
        "clickhouse_table": {
            "name": clickhouse_table_name,
            "engine": "MergeTree",
            "order_by": "id",
            "fields": clickhouse_fields
        }
    }

    # Save the configuration to the provided output file path
    with open(output_file_path, 'w') as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Configuration file generated: {output_file_path}")

def map_dtype_to_django_field(column, dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return f"models.IntegerField()"
    elif pd.api.types.is_float_dtype(dtype):
        return f"models.FloatField()"
    elif pd.api.types.is_bool_dtype(dtype):
        return f"models.BooleanField()"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return f"models.DateTimeField()"
    elif pd.api.types.is_string_dtype(dtype):
        return f"models.CharField(max_length=255)"
    else:
        return f"models.TextField()"

def map_dtype_to_clickhouse_field(column, dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "UInt32"
    elif pd.api.types.is_float_dtype(dtype):
        return "Float32"
    elif pd.api.types.is_bool_dtype(dtype):
        return "UInt8"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DateTime"
    elif pd.api.types.is_string_dtype(dtype):
        return "String"
    else:
        return "String"

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_config_from_parquet.py <parquet_file_path> <output_file_path>")
        sys.exit(1)

    parquet_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    generate_config_from_parquet(parquet_file_path, output_file_path)
