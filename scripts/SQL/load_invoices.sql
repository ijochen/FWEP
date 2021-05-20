
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
			case 
                when "Branch" = '01-Anaheim' then 'Anaheim'
                when "Branch" = '02-Indio' then 'Indio'
                when "Branch" = '03-El Cajon' then 'El Cajon'
                when "Branch" = '04-Murrieta' then 'Murrieta'
                when "Branch" = '05-Livermore' then 'Livermore'
                when "Branch" = '06-Ontario' then 'Ontario'
                when "Branch" = '07-San Dimas' then 'San Dimas'
                when "Branch" = '08-Cathedral City' then 'Cathedral City'
                when "Branch" = '09-San Fernando' then 'San Fernando'
                when "Branch" = '10-Visalia' then 'Visalia'
                when "Branch" = '11-San Antonio' then 'San Antonio (PEP)'
                when "Branch" = '12-Vista' then 'Vista'
                when "Branch" = '13-Austin' then 'San Antonio (PEP)'
                when "Branch" = '14-Palm Springs' then 'Austin (PEP)'
                when "Branch" = '15-Corona' then 'Corona'
                when "Branch" = '16-Bakersfield' then 'Bakersfield'
                when "Branch" = '17-Houston' then 'Houston'
                when "Branch" = '18-Lake Forest' then 'Lake Forest'
                when "Branch" = '19-Oxnard' then 'Oxnard'
                when "Branch" = '20-North Austin' then 'North Austin'
                when "Branch" = '21-Duarte' then 'Duarte'
                when "Branch" = '22-Yucaipa' then 'Yucaipa'
                when "Branch" = '23-Riverside' then 'Riverside'
                when "Branch" = '24-Long Beach' then 'Long Beach'
                when "Branch" = '25-Palm Desert' then 'Palm Desert'
                when "Branch" = '26-Los Angeles' then 'Los Angeles'
                when "Branch" = '27-Tempe' then 'Tempe'
                when "Branch" = '28-Phoenix' then 'Phoenix'
                when "Branch" = '29-Santa Ana' then 'Santa Ana'
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
			case 
                when branch_description = 'APS - HENDERSON NV' then 'Henderson'
                when branch_description = 'FWP - SAN ANTONIO TX' then 'San Antonio (FWP)'
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