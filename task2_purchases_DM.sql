SELECT
orders.*,
transactions.*,
verification.*
FROM orders
INNER JOIN transactions on orders.id=transactions.order_id
INNER JOIN verification on transactions.id=verification.transaction_id
WHERE 
  NOT EXISTS (
    SELECT 1
    FROM purchases p
    WHERE p.order_id = orders.id
  )
  OR EXISTS (
    SELECT 1
    FROM purchases p
    WHERE p.order_id = orders.id
    AND (
      orders.updated_at > p.uploaded_at
      OR transactions.updated_at > p.uploaded_at
      OR verification.updated_at > p.uploaded_at
    )
  );
