
--select version()
--select sales.load_invoices()

--drop function sales.load_invoices

CREATE FUNCTION sales.load_invoices()  RETURNS void AS $$
begin
	DROP TABLE IF EXISTS sales.invoice_data_merged;
	
    create table sales.invoice_data_merged as 
    select *
	from (
		(select
			null as order_no,
			"INV_NUM" as invoice_no,
			cast("INV_DATE" as timestamp) as invoice_date, 
			case --when "Branch" = '01-Anaheim' then 'Anaheim' 
                when "Branch" = '11-San Antonio' then 'San Antonio (PEP)'
                    else "Branch" end branch,
			cast(null as timestamp) as order_date,
			cast("MERCH_AMT" as float) as total_sales,
			cast("TOT_COST" as float) as total_cost,
			c."CT_DESC" as channel,
			'PEP' as company
		from sales.pep_invoice_data i
		left join sales.pep_customer c
			on i."CUST_NUM" = c."CUST_NUM")
		
		union 
		(select 
			order_no,
			invoice_no,
			cast(invoice_date as timestamp) as invoice_date,
			case when branch_description = 'San Antonio' then 'San Antonio (FWP)'
                    else branch_description end branch_description,
			cast(order_date as timestamp) as order_date,
			max(total_sales)-max(freight)-max(tax_amount) as total_sales,
			sum(cogs_amount) as cogs_amount,
			class_1id as channel,
			'FWP' as company
		from sales.fwp_invoice_data 
		group by order_no,
			invoice_no,
			invoice_date,
			branch_description,
			order_date,
			class_1id
		)
	) a;
END ;
$$
LANGUAGE plpgsql ;