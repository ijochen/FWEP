
--select version()
--select sales.upsert_pep_invoices()

--drop function sales.upsert_pep_invoices()

CREATE FUNCTION sales.upsert_pep_invoices()  RETURNS void AS $$
begin
	
	-- Delete
	delete from sales.pep_invoices
	where "INV_NUM" in (
		select "INV_NUM" 
		from sales.pep_invoices_incremental
	);
	
	-- Reinsert 
	insert into sales.pep_invoices
	select "ORD_NUM", "INV_NUM", "INV_DATE", "SEL_WHSE", "Branch", "ORD_DATE", "INV_AMT","MERCH_AMT", "TOT_COST", "CUST_NUM", "CUST_DESC", "CT_DESC"
	from (
		select *, 
			row_number() over(partition by "INV_NUM" order by "INV_NUM") as row_num
		from sales.pep_invoices_incremental
	) a
	where row_num = 1;

	
END ;
$$
LANGUAGE plpgsql ;

/*delete 
-- Have all rows from prev?
	
	-- 1048394
	select 
		count(distinct "INV_NUM") 
	from sales.pep_invoice_data_HISTORICAL
	where "INV_NUM" in (
		select "INV_NUM" from sales.pep_invoice_data
	)
	
	select 
		count(distinct "INV_NUM") 
	from sales.pep_invoice_data_HISTORICAL
	

-- Did they get inserted?
	select count(*) from sales.pep_invoice_data
	where "INV_NUM" in (select "INV_NUM" from sales.pep_invoice_data_INCREMENTAL)
	
	select count(*) from sales.pep_invoice_data_INCREMENTAL

*/
	
