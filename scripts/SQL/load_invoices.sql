
--select version()
--select sales.load_invoices()

--drop function sales.load_invoices

CREATE FUNCTION sales.load_invoices()  RETURNS void AS $$
begin
	DROP TABLE IF EXISTS sales.invoice_data_merged;
	
    create table sales.invoice_data_merged as 
    select distinct *
	from (
		(select distinct
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
                when "Branch" = '13-Austin' then 'Austin (PEP)'
                when "Branch" = '14-Palm Springs' then 'Palm Springs'
                when "Branch" = '15-Corona' then 'Corona'
                when "Branch" = '16-Bakersfield' then 'Bakersfield'
                when "Branch" = '17-Houston' then 'Houston'
                when "Branch" = '18-Lake Forest' then 'Lake Forest'
                when "Branch" = '19-Moorpark' then 'Moorpark'
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
                when "Branch" = '30-El Centro' then 'El Centro'
                    else "Branch" end branch,
			cast(null as timestamp) as order_date,
			cast("MERCH_AMT" as float) as total_sales,
			cast("TOT_COST" as float) as total_cost,
			c."CT_DESC" as channel,
			'PEP' as company
		from sales.pep_invoice_data i
		left join sales.pep_customer c
			on i."CUST_NUM" = c."CUST_NUM"
		where i."CUST_NUM" <> '0FWPCORP'
		)
		
		union all
		
		(select distinct
			order_no,
			invoice_no,
			cast(invoice_date as timestamp) as invoice_date,
            --rename the branches to proper case to prevent random aliases in tableau workbook
			case 
                when branch_id = '000' then 'Corporate'
                when branch_id = '010' then 'Tampa'
                when branch_id = '020' then 'Spring Hill'
                when branch_id = '030' then 'Port Charlotte'
				when branch_id = '035' then 'Sarasota'
                when branch_id = '040' then 'North Miami'
                when branch_id = '050' then 'Naples'
                when branch_id = '060' then 'West Palm Beach'
                when branch_id = '070' then 'Melbourne'
                when branch_id = '080' then 'Fulfillment'
                when branch_id = '090' then 'Conroe'
                when branch_id = '098' then 'Warranty West Coast'
                when branch_id = '099' then 'Warranty American'
                when branch_id = '100' then 'Katy'
                when branch_id = '110' then 'Austin (FWP)'
                when branch_id = '120' then 'Plano'
                when branch_id = '130' then 'San Antonio (FWP)'
                when branch_id = '140' then 'Fort Worth'
                when branch_id = '200' then 'Meade'
                when branch_id = '210' then 'La Costa'
                when branch_id = '220' then 'Henderson'
                when branch_id = '250' then 'St. George'
                when branch_id = '301' then 'Anaheim'
				when branch_id = '302' then 'Indio'
				when branch_id = '303' then 'El Cajon'
				when branch_id = '304' then 'Murrieta'
				when branch_id = '305' then 'Livermore'
				when branch_id = '306' then 'Ontario'
				when branch_id = '307' then 'San Dimas'
				when branch_id = '308' then 'Cathedral City'
				when branch_id = '309' then 'San Fernando'
				when branch_id = '310' then 'Visalia'
				when branch_id = '311' then 'San Antonio (PEP)'
				when branch_id = '312' then 'Vista'
				when branch_id = '313' then 'Austin (PEP)'
				when branch_id = '314' then 'Palm Springs'
				when branch_id = '315' then 'Corona'
				when branch_id = '316' then 'Bakersfield'
				when branch_id = '317' then 'Houston'
				when branch_id = '318' then 'Lake Forest'
				when branch_id = '319' then 'Moorpark'
				when branch_id = '320' then 'North Austin'
				when branch_id = '321' then 'Duarte'
				when branch_id = '322' then 'Yucaipa'
				when branch_id = '323' then 'Riverside'
				when branch_id = '324' then 'Long Beach'
				when branch_id = '325' then 'Palm Desert'
				when branch_id = '326' then 'Los Angeles'
				when branch_id = '327' then 'Tempe'
				when branch_id = '328' then 'Phoenix'
				when branch_id = '329' then 'Santa Ana'
				when branch_id = '330' then 'El Centro'
				when branch_id = '331' then 'Rancho Cordova'
				when branch_id = '332' then 'Chandler'
                    else branch_description end branch_description,
			cast(order_date as timestamp) as order_date,
			max(total_sales)-max(freight)-max(tax_amount) as total_sales,
			sum(cogs_amount) as cogs_amount,
			class_1id as channel,
			'FWP' as company
		from sales.fwp_invoice_data 
		where customer_name <> 'POOL AND ELECTRICAL PRODUCTS'  
            --add these filters to match the GL per Katia Diaz/Reyes and Mike Peters
            and (invoice_line_type = '0' and invoice_adjustment_type = 'I' and rebilled_flag = 'N')
		group by order_no,
			invoice_no,
			invoice_date,
            branch_id,
			branch_description,
			order_date,
			class_1id
		)
	) a;

    DROP TABLE IF EXISTS aquacentral.invoice_data_merged;
	
    create table aquacentral.invoice_data_merged as 
    select *
	from (
				(select distinct
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
                when "Branch" = '13-Austin' then 'Austin (PEP)'
                when "Branch" = '14-Palm Springs' then 'Palm Springs'
                when "Branch" = '15-Corona' then 'Corona'
                when "Branch" = '16-Bakersfield' then 'Bakersfield'
                when "Branch" = '17-Houston' then 'Houston'
                when "Branch" = '18-Lake Forest' then 'Lake Forest'
                when "Branch" = '19-Moorpark' then 'Moorpark'
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
                when "Branch" = '30-El Centro' then 'El Centro'
                    else "Branch" end branch,
			cast(null as timestamp) as order_date,
			cast("MERCH_AMT" as float) as total_sales,
			cast("TOT_COST" as float) as total_cost,
			c."CT_DESC" as channel,
			'PEP' as company
		from sales.pep_invoice_data i
		left join sales.pep_customer c
			on i."CUST_NUM" = c."CUST_NUM"
		where i."CUST_NUM" <> '0FWPCORP'
		)
		
		union all
		
		(select distinct
			order_no,
			invoice_no,
			cast(invoice_date as timestamp) as invoice_date,
            --rename the branches to proper case to prevent random aliases in tableau workbook
			case 
                when branch_id = '000' then 'Corporate'
                when branch_id = '010' then 'Tampa'
                when branch_id = '020' then 'Spring Hill'
                when branch_id = '030' then 'Port Charlotte'
				when branch_id = '035' then 'Sarasota'
                when branch_id = '040' then 'North Miami'
                when branch_id = '050' then 'Naples'
                when branch_id = '060' then 'West Palm Beach'
                when branch_id = '070' then 'Melbourne'
                when branch_id = '080' then 'Fulfillment'
                when branch_id = '090' then 'Conroe'
                when branch_id = '098' then 'Warranty West Coast'
                when branch_id = '099' then 'Warranty American'
                when branch_id = '100' then 'Katy'
                when branch_id = '110' then 'Austin (FWP)'
                when branch_id = '120' then 'Plano'
                when branch_id = '130' then 'San Antonio (FWP)'
                when branch_id = '140' then 'Fort Worth'
                when branch_id = '200' then 'Meade'
                when branch_id = '210' then 'La Costa'
                when branch_id = '220' then 'Henderson'
                when branch_id = '250' then 'St. George'
                when branch_id = '301' then 'Anaheim'
				when branch_id = '302' then 'Indio'
				when branch_id = '303' then 'El Cajon'
				when branch_id = '304' then 'Murrieta'
				when branch_id = '305' then 'Livermore'
				when branch_id = '306' then 'Ontario'
				when branch_id = '307' then 'San Dimas'
				when branch_id = '308' then 'Cathedral City'
				when branch_id = '309' then 'San Fernando'
				when branch_id = '310' then 'Visalia'
				when branch_id = '311' then 'San Antonio (PEP)'
				when branch_id = '312' then 'Vista'
				when branch_id = '313' then 'Austin (PEP)'
				when branch_id = '314' then 'Palm Springs'
				when branch_id = '315' then 'Corona'
				when branch_id = '316' then 'Bakersfield'
				when branch_id = '317' then 'Houston'
				when branch_id = '318' then 'Lake Forest'
				when branch_id = '319' then 'Moorpark'
				when branch_id = '320' then 'North Austin'
				when branch_id = '321' then 'Duarte'
				when branch_id = '322' then 'Yucaipa'
				when branch_id = '323' then 'Riverside'
				when branch_id = '324' then 'Long Beach'
				when branch_id = '325' then 'Palm Desert'
				when branch_id = '326' then 'Los Angeles'
				when branch_id = '327' then 'Tempe'
				when branch_id = '328' then 'Phoenix'
				when branch_id = '329' then 'Santa Ana'
				when branch_id = '330' then 'El Centro'
				when branch_id = '331' then 'Rancho Cordova'
				when branch_id = '332' then 'Chandler'
                    else branch_description end branch_description,
			cast(order_date as timestamp) as order_date,
			max(total_sales)-max(freight)-max(tax_amount) as total_sales,
			sum(cogs_amount) as cogs_amount,
			class_1id as channel,
			'FWP' as company
		from sales.fwp_invoice_data 
		where customer_name <> 'POOL AND ELECTRICAL PRODUCTS'
            --add these filters to match the GL per Katia Diaz/Reyes and Mike Peters
            and (invoice_line_type = '0' and invoice_adjustment_type = 'I' and rebilled_flag = 'N')
		group by order_no,
			invoice_no,
			invoice_date,
            branch_id,
			branch_description,
			order_date,
			class_1id
		)
	) a;

END ;
$$
LANGUAGE plpgsql ;