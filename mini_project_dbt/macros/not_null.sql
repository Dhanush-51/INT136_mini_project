{% macro not_null(columns) %}
    {% for column in columns %}
        {{ column }} IS NOT NULL
        {% if not loop.last %} AND {% endif %}
    {% endfor %}
{% endmacro %}