WITH monthly_sum_groceries AS (SELECT 
	DATE_TRUNC('month', tj.date) AS month,
	Sum(t.amount) as monthly_sum
FROM main.categories c
INNER JOIN main.category_transaction_journal ctj ON ctj.category_id=c.id
INNER JOIN main.transaction_journals tj ON ctj.transaction_journal_id=tj.id
INNER JOIN main.transactions t ON t.transaction_journal_id=tj.id
INNER JOIN main.accounts a ON a.id=t.account_id

WHERE c.id = 6 AND t.account_id = 29
GROUP BY month)

SELECT AVG(monthly_sum) FROM monthly_sum_groceries