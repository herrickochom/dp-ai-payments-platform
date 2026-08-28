{% macro bronze_valid_events() %}
    read_parquet(
        's3://dp-ai-payment/bronze/valid/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
{% endmacro %}
