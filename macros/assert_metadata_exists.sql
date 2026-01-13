{% macro assert_metadata_exists() %}
  {% if execute %}
    {% set model_node = graph.nodes.values() | selectattr('unique_id', 'equalto', model.unique_id) | first %}
    {% if not model_node.description %}
      {{ exceptions.raise_compiler_error("FATAL: Metadata description is missing for model '" ~ model.name ~ "'. The corresponding .yml file entry is required.") }}
    {% endif %}
  {% endif %}
{% endmacro %}
