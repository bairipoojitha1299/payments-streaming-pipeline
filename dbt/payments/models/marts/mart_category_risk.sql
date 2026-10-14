-- Which merchant categories carry the most fraud risk.
select
    category,
    count(*)                                                  as transactions,
    count_if(is_fraud)                                        as fraud_transactions,
    round(100 * count_if(is_fraud) / nullif(count(*), 0), 3)  as fraud_rate_pct,
    sum(iff(is_fraud, amount, 0))                             as fraud_amount,
    round(avg(iff(is_fraud, amount, null)), 2)                as avg_fraud_amount,
    round(avg(iff(not is_fraud, amount, null)), 2)            as avg_normal_amount
from {{ ref('stg_card_transactions') }}
group by 1
