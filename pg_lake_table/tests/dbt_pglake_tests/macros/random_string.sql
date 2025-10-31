{% macro random_string(length=15) %}
    {% set characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789' %}
    {% set result = '' %}
    {% for i in range(length) %}
        {% set result = result + characters | random %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}
