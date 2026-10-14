{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge'
) }}

-- Flatten the JSON payload into typed columns and remove duplicates.
-- Kafka delivery is at-least-once, so the same event_id can arrive more than
-- once; QUALIFY keeps the first copy that was loaded.

select
    payload:event_id::string                        as event_id,
    payload:trans_num::string                       as trans_num,
    payload:card_id::string                         as card_id,
    payload:event_time::timestamp_tz                as event_time,
    payload:original_txn_time::timestamp_ntz        as original_txn_time,
    payload:merchant::string                        as merchant,
    payload:category::string                        as category,
    payload:amount::number(12, 2)                   as amount,
    payload:customer_state::string                  as customer_state,
    payload:customer_city::string                   as customer_city,
    payload:customer_lat::float                     as customer_lat,
    payload:customer_long::float                    as customer_long,
    payload:merchant_lat::float                     as merchant_lat,
    payload:merchant_long::float                    as merchant_long,
    payload:is_fraud::boolean                       as is_fraud,
    payload:kafka_partition::int                    as kafka_partition,
    payload:kafka_offset::int                       as kafka_offset,
    source_file,
    loaded_at
from {{ source('raw', 'card_transactions') }}
{% if is_incremental() %}
where loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }})
{% endif %}
qualify row_number() over (partition by payload:event_id::string order by loaded_at, file_row_number) = 1
