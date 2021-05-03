

--DROP FUNCTION warehouse.upsert_inv_transactions()

CREATE FUNCTION warehouse.upsert_inv_transactions()  RETURNS void AS $$
begin
	
	-- Delete
	delete from warehouse.inv_transactions
	where "transaction_number" in (
		select "transaction_number" 
		from warehouse.inv_transactions_INCREMENTAL
	);
	
	-- Reinsert 
	insert into warehouse.inv_transactions
	select
        "location_id", "location_name", "bin", "transaction_number", "quantity", "date_created", "inv_mast_uid", "item_id", "item_desc", "po_number"
    from warehouse.inv_transactions_INCREMENTAL;

	
END ;
$$
LANGUAGE plpgsql ;
