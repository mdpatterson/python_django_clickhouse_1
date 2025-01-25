import os
import subprocess
import sys


def list_parquet_files(directory):
    """List all Parquet files in the specified directory."""
    parquet_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            parquet_files.append(os.path.join(directory, filename))
    return parquet_files


def run_generate_config_script(parquet_file, config_file):
    """Run the generate_config_from_parquet.py script for each Parquet file."""
    command = [
        sys.executable,  # Use the current Python interpreter
        "generate_config_from_parquet.py",  # The script name
        parquet_file,  # The Parquet file path
        config_file  # The output config file path
    ]
    subprocess.run(command, check=True)


def run_write_clickhouse_table_script(config_file, parquet_file):
    """Run the write_clickhouse_table_2.py script with the config file and parquet file."""
    command = [
        sys.executable,  # Use the current Python interpreter
        "write_clickhouse_table_2.py",  # The script name
        config_file,  # The config file path
        parquet_file  # The Parquet file path
    ]
    subprocess.run(command, check=True)


def run_append_endpoints_script():
    """Run the append_endpoints_using_inspectdb_1.py script."""
    command = [
        sys.executable,  # Use the current Python interpreter
        "append_endpoints_using_inspectdb_1.py"  # The script name
    ]
    subprocess.run(command, check=True)


def main(parquet_directory, config_directory):
    """List Parquet files and run the generate_config_from_parquet and write_clickhouse_table_2 scripts."""
    if not os.path.exists(config_directory):
        os.makedirs(config_directory)  # Create the config directory if it doesn't exist

    parquet_files = list_parquet_files(parquet_directory)

    for parquet_file in parquet_files:
        # Extract the file name without extension
        file_name = os.path.basename(parquet_file)
        output_config_file = os.path.join(config_directory, f"{os.path.splitext(file_name)[0]}_config.json")

        # Generate the config file
        print(f"Generating config for: {file_name}")
        run_generate_config_script(parquet_file, output_config_file)

        # Run the write_clickhouse_table_2 script
        print(f"Running write_clickhouse_table_2 for: {file_name}")
        run_write_clickhouse_table_script(output_config_file, parquet_file)

    # Run the append_endpoints_using_inspectdb_1.py script after all the Parquet files are processed
    print("Running append_endpoints_using_inspectdb_1.py at the end of the process.")
    run_append_endpoints_script()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_configs_and_write_to_clickhouse.py <parquet_directory> <config_directory>")
        sys.exit(1)

    parquet_directory = sys.argv[1]
    config_directory = sys.argv[2]
    main(parquet_directory, config_directory)
