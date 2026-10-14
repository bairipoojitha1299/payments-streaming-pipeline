-- Rule-based alerts on the stream, with each alert's reason and whether it
-- turned out to be fraud (so rule precision can be tracked over time).
with history as (
    select
        *,
        avg(amount) over (partition by card_id order by event_time
                          rows between unbounded preceding and 1 preceding)       as card_avg_prior,
        count(*) over (partition by card_id order by event_time
                       rows between unbounded preceding and 1 preceding)          as card_txns_prior,
        datediff('second',
                 lag(event_time, 2) over (partition by card_id order by event_time),
                 event_time)                                                      as secs_since_2_txns_ago,
        hour(original_txn_time)                                                   as txn_hour
    from {{ ref('stg_card_transactions') }}
),
flagged as (
    select
        *,
        (card_txns_prior >= 5 and amount >= 5 * card_avg_prior and amount >= 200) as rule_spend_spike,
        (secs_since_2_txns_ago <= 600)                                             as rule_rapid_fire,
        ((txn_hour >= 22 or txn_hour < 4) and amount >= 300)                       as rule_night_high_amount
    from history
)
select
    event_id,
    card_id,
    event_time,
    category,
    merchant,
    amount,
    round(card_avg_prior, 2)                         as card_avg_prior,
    array_to_string(array_construct_compact(
        iff(rule_spend_spike, '>=5x card average', null),
        iff(rule_rapid_fire, '3 txns within 10 min', null),
        iff(rule_night_high_amount, 'night-time >= $300', null)
    ), '; ')                                         as alert_reasons,
    is_fraud                                         as confirmed_fraud
from flagged
where rule_spend_spike or rule_rapid_fire or rule_night_high_amount
