-- Operational view of the pipeline itself: one row per landed micro-batch.
-- End-to-end latency = when the batch was queryable in Snowflake minus when
-- its newest event was produced to Kafka.
with valid as (
    select source_file,
           count(*)          as events,
           min(event_time)   as first_event_time,
           max(event_time)   as last_event_time,
           max(loaded_at)    as loaded_at
    from {{ ref('stg_card_transactions') }}
    group by 1
),
rejected as (
    select replace(source_file, 'dlq_', 'batch_') as source_file,
           count(distinct kafka_partition || '-' || kafka_offset) as rejected_events
    from {{ ref('stg_dead_letters') }}
    group by 1
)
select
    v.source_file,
    v.events,
    coalesce(r.rejected_events, 0)                                      as rejected_events,
    round(100 * coalesce(r.rejected_events, 0)
          / nullif(v.events + coalesce(r.rejected_events, 0), 0), 2)    as rejected_pct,
    v.first_event_time,
    v.last_event_time,
    v.loaded_at,
    datediff('second', v.last_event_time, v.loaded_at)                  as latency_seconds
from valid v
left join rejected r using (source_file)
