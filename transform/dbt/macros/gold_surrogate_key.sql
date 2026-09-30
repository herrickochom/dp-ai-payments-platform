{% macro gold_surrogate_key(columns) -%}
to_hex(md5(to_utf8(concat_ws('||'
    {%- for column in columns -%}
    , coalesce(cast({{ column }} as varchar), '__UNKNOWN__')
    {%- endfor -%}
))))
{%- endmacro %}
