-- Rejected messages with their validation errors, for monitoring data quality.
select
    payload:kafka_partition::int                as kafka_partition,
    payload:kafka_offset::int                   as kafka_offset,
    payload:received_at::timestamp_tz           as received_at,
    payload:raw_value::string                   as raw_value,
    e.value::string                             as error,
    source_file,
    loaded_at
from {{ source('raw', 'card_transactions_dlq') }},
     lateral flatten(input => payload:errors) e
