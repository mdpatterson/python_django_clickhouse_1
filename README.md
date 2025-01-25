    mkdir python_django_clickhouse_
    cd python_django_clickhouse_
    ./setup_django_project_1.sh
    mkdir my_parquet_files
    mkdir my_config_files
    python generate_stock_data_parquet.py 15 1000 ../my_parquet_files/stock_data_15_1000.parquet
    python generate_stock_data_parquet.py 25 1000 ../my_parquet_files/stock_data_25_1000.parquet
    python generate_stock_data_parquet.py 35 1000 ../my_parquet_files/stock_data_35_1000.parquet
    python generate_stock_data_parquet.py 45 1000 ../my_parquet_files/stock_data_45_1000.parquet
    python my_wrapper.py ../my_parquet_files/ ../my_config_files/
    cd ../myproject/
    python manage.py runserver
    http://127.0.0.1:8000/api/
    curl -o response.json http://127.0.0.1:8000/api/stockdata/