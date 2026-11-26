{% macro gold_surrogate_key(columns) -%}
to_hex(md5(array_to_string([
    {%- for column in columns -%}
    coalesce(cast({{ column }} as string), '__UNKNOWN__'){% if not loop.last %}, {% endif %}
    {%- endfor -%}
], '||')))
{%- endmacro %}
