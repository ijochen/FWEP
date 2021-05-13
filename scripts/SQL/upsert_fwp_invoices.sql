CREATE FUNCTION sales.upsert_fwp_invoices()  RETURNS void AS $$
begin
	
	-- Delete
	delete from sales.fwp_invoice_data
	where invoice_no in (
		select invoice_no
		from sales.fwp_invoice_data_incremental
	);
	
	-- Reinsert 
	insert into sales.fwp_invoice_data 
	select * from sales.fwp_invoice_data_incremental;
	
END ;
$$
LANGUAGE plpgsql ;