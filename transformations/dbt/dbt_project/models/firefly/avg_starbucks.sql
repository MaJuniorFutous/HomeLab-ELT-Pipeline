WITH monthly_sum AS (SELECT
        DATE_TRUNC('month', tj.date) AS month,
        --AVG(t.amount) AS avg_amount,
        Sum(t.amount) as monthly_sum
    -- FROM main.tags tags
    FROM {{ source('firefly_db', 'tags') }}
    INNER JOIN {{ source('firefly_db', 'tag_transaction_journal') }} ttj ON ttj.tag_id=tags.id
    INNER JOIN {{ source('firefly_db', 'transaction_journals') }} tj ON tj.id=ttj.transaction_journal_id
    INNER JOIN {{ source('firefly_db', 'transactions') }} t ON t.transaction_journal_id=tj.id
    INNER JOIN {{ source('firefly_db', 'accounts') }} a ON a.id=t.account_id
    WHERE tags.tag = 'Starbucks' AND t.account_id = 29
    GROUP BY month
)

SELECT Avg(monthly_sum) as avg_sum FROM monthly_sum