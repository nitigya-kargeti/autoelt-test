
{{ assert_metadata_exists() }}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        merge_exclude_columns = ['DW_INSERT_DATE'],
        schema='DWH_SI',
        tags=['l2'],
        unique_key=['RECORD_NO']
    , post_hook=[
        "DELETE FROM {{ this }} WHERE IS_DELETED = TRUE"
    ]

    )
}}

-- This model incrementally builds the L2 table (SCD Type 1)
-- It merges new records and updates existing ones based on the unique key.

WITH filtered_source AS (
    SELECT
        *, {{ generate_audit_column('DW_INSERT_DATE') }}
         ,{{ generate_audit_column('DW_UPDATE_DATE') }}
    FROM {{ ref('V_STG_SI_GLACCOUNT_L1') }}

    {% if is_incremental() %}
    -- This filter is applied *before* deduplication
    WHERE LAST_MODIFIED_DATE > (SELECT MAX(LAST_MODIFIED_DATE) FROM {{ this }})
    {% endif %}
)

SELECT
    *
FROM filtered_source

-- Deduplicate *after* filtering for new records
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY RECORD_NO
  ORDER BY LAST_MODIFIED_DATE DESC
) = 1
