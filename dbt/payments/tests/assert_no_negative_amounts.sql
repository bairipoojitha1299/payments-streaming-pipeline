-- Negative amounts must be stopped by the consumer's contract check (DLQ),
-- never reach the warehouse.
select event_id, amount
from {{ ref('stg_card_transactions') }}
where amount < 0
