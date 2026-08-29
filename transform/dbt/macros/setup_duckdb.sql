-- macros/setup_duckdb.sql
{% macro setup_duckdb() %}
    INSTALL httpfs;
    LOAD httpfs;
    INSTALL iceberg;
    LOAD iceberg;

    SET s3_region='us-east-1';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_endpoint='localhost:9000';
    SET s3_use_ssl='false';
    SET s3_url_style='path';
{% endmacro %}
