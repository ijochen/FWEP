--delete all rows to avoid inserting duplicates
drop table sales.fwp_invoice_data 


--insert all into the table 
select * into sales.fwp_invoice_data 
from 
(
select * from sales.fwp_invoice_data_2021 
union 
select * from sales.fwp_invoice_data_2020
union 
select * from sales.fwp_invoice_data_2019
) as fwp_invoice_data

--execute stored procedure to merge all the data
select sales.load_invoices()


--insert fwp invoices into merged invoice table
insert into sales.invoice_data_merged (order_no, invoice_no, invoice_date, branch, order_date, total_sales, total_cost, channel, company)
from 
(
select 
			order_no,
			invoice_no,
			cast(invoice_date as timestamp) as invoice_date,
			branch_description,
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
) as fwp_invoice_data


--insert didn't work so drop then create
DROP TABLE IF EXISTS sales.invoice_data_merged;


--create table with merged data
create table sales.invoice_data_merged as 
    select *
	from (
		(select
			null as order_no,
			"INV_NUM" as invoice_no,
			cast("INV_DATE" as timestamp) as invoice_date, 
			"Branch" as branch,
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
			branch_description,
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
