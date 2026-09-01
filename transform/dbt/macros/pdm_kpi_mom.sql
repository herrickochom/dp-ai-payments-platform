{# Reusable month-on-month helpers for PDM executive KPIs. #}

{% macro pdm_mom_pct_change(current_value, previous_value) -%}
case
    when {{ previous_value }} is null or {{ previous_value }} = 0 then null
    else (
        ({{ current_value }} - {{ previous_value }})
        / abs({{ previous_value }})
    ) * 100.0
end
{%- endmacro %}


{% macro pdm_mom_direction(current_value, previous_value) -%}
case
    when {{ previous_value }} is null then 'NO_PRIOR'
    when {{ current_value }} > {{ previous_value }} then 'UP'
    when {{ current_value }} < {{ previous_value }} then 'DOWN'
    else 'FLAT'
end
{%- endmacro %}


{% macro pdm_mom_arrow(current_value, previous_value) -%}
case
    when {{ previous_value }} is null then '—'
    when {{ current_value }} > {{ previous_value }} then '↑'
    when {{ current_value }} < {{ previous_value }} then '↓'
    else '→'
end
{%- endmacro %}


{% macro pdm_mom_status(current_value, previous_value, increase_is_good=true) -%}
case
    when {{ previous_value }} is null then 'NO_PRIOR'
    when {{ current_value }} = {{ previous_value }} then 'NEUTRAL'
    {% if increase_is_good %}
    when {{ current_value }} > {{ previous_value }} then 'FAVOURABLE'
    else 'UNFAVOURABLE'
    {% else %}
    when {{ current_value }} < {{ previous_value }} then 'FAVOURABLE'
    else 'UNFAVOURABLE'
    {% endif %}
end
{%- endmacro %}
