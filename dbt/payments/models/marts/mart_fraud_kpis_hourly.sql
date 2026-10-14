-- Hourly business KPIs for a live fraud dashboard.
select
    date_trunc('hour', event_time)                         as event_hour,
    count(*)                                               as transactions,
    count(distinct card_id)                                as active_cards,
    sum(amount)                                            as total_amount,
    round(avg(amount), 2)                                  as avg_amount,
    count_if(is_fraud)                                     as fraud_transactions,
    sum(iff(is_fraud, amount, 0))                          as fraud_amount,
    round(100 * count_if(is_fraud) / nullif(count(*), 0), 3) as fraud_rate_pct
from {{ ref('stg_card_transactions') }}
group by 1
