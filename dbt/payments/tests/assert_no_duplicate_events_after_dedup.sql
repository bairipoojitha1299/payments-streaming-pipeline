-- Every raw event_id appears exactly once in staging, even though Kafka may
-- deliver duplicates (at-least-once) and the producer injects some on purpose.
with raw_ids as (
    select count(distinct payload:event_id::string) as n from {{ source('raw', 'card_transactions') }}
),
stg as (
    select count(*) as n from {{ ref('stg_card_transactions') }}
)
select raw_ids.n as distinct_raw_events, stg.n as staged_events
from raw_ids, stg
where raw_ids.n <> stg.n
