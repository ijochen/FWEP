/**********TOTAL AMOUNTS PER MONTH NOT ADDING UP TO THE DSR
TRY TO ADD IN ORDER NO AND ORDER LINE INCASE THE PIVOT IS REMOVING DUPLICATES BECAUSE IT DOESN'T KNOW IT'S COMING FROM MULTIPLE ORDERS/LINES
**********/

with FWEP_Prod as (
select distinct company, prod_num, min(prod_desc) over(partition by prod_num) prod_desc, min(prod_group) over(partition by prod_num) prod_group
from Prelude.dbo.FWEP_Sales_Orders 
where year(invoice_date) >= 2021
group by company, prod_num, prod_desc, prod_group
),


FWEP_FMFY_Sales as (
select distinct *
from (
	select distinct
	company,
	prod_num, 
	sum(extended_price) sales, 
	concat(month_name,year)+'Sales' FMFY--,
	from Prelude.dbo.FWEP_Sales_Orders 
	where year(invoice_date) >= 2021
	group by 
	company,
	month_name,
	prod_num, 
	extended_price,
	year
) SalesData
pivot (
	sum(sales)
	for FMFY in (January2021Sales, February2021Sales, March2021Sales, April2021Sales, May2021Sales, June2021Sales)
	) as MonthlySales
--order by prod_num
),


FWEP_FMFY_UnitsSold as (
select distinct *
from (
	select distinct
	company,
	prod_num, 
	sum(qty_shipped) units_sold,
	concat(month_name,year)+'UnitsSold' FMFY--,
	from Prelude.dbo.FWEP_Sales_Orders 
	where year(invoice_date) >= 2021
	group by 
	company,
	month_name,
	prod_num,
	qty_shipped,
	year
) SalesData
pivot (
	sum(units_sold)
	for FMFY in (January2021UnitsSold, February2021UnitsSold, March2021UnitsSold, April2021UnitsSold, May2021UnitsSold, June2021UnitsSold)
	) as MonthlyUnitsSold
--order by prod_num
),


FWEP_FMFY_Sales_UnitsSold as (
select distinct p.company,
p.prod_num, p.prod_desc, p.prod_group,
January2021Sales, February2021Sales, March2021Sales, April2021Sales, May2021Sales, June2021Sales, 
January2021UnitsSold, February2021UnitsSold, March2021UnitsSold, April2021UnitsSold, May2021UnitsSold, June2021UnitsSold
from FWEP_Prod p 
left join FWEP_FMFY_Sales ms on p.prod_num = ms.prod_num and p.company = ms.company
left join FWEP_FMFY_UnitsSold mu on p.prod_num = mu.prod_num and p.company = mu.company
group by p.company, p.prod_num, p.prod_desc, p.prod_group,
January2021Sales, February2021Sales, March2021Sales, April2021Sales, May2021Sales, June2021Sales, 
January2021UnitsSold, February2021UnitsSold, March2021UnitsSold, April2021UnitsSold, May2021UnitsSold, June2021UnitsSold
--order by prod_num
),


FWEP_Vendor as (
select distinct company, prod_num, vendor_id, vendor_name--, vendor_rank
from (
select distinct company, prod_num, vendor_id, vendor_name, 
row_number() over(partition by prod_num, company order by vendor_count desc) vendor_rank, 
vendor_count
from (
select distinct company, prod_num, vendor_id, vendor_name, count(po_no) over(partition by vendor_name, prod_num) vendor_count
from Prelude.dbo.FWEP_Purchase_Orders
--where year(receipt_date) >= 2010
group by company, prod_num, vendor_id, vendor_name, po_no
--order by prod_num
) t1 
group by company, prod_num, vendor_id, vendor_name, vendor_count
--order by prod_num
) t2
where vendor_rank = 1
group by company, prod_num, vendor_id, vendor_name--, vendor_rank
--order by prod_num
),


FWEP_FMFY_Sales_UnitsSold_Vendor as (
select distinct s.company, --min(vendor_name) over(partition by s.prod_num order by vendor_count desc)
vendor_id,
case 
	when s.prod_num like '*UNI%' and vendor_name is null and s.company = 'PEP' then 'UNIVERSAL TILE COMPANY'
	when s.prod_num like '307%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like '88%' and vendor_name is null and s.company = 'PEP' then 'SPEARS MFG COMPANY'
	when s.prod_num like '920%' and vendor_name is null and s.company = 'PEP' then 'TURLEY INTERNATIONAL RESOURCES'
	when s.prod_num like '940%' and vendor_name is null and s.company = 'PEP' then 'S.R.SMITH,LLC'
	when s.prod_num like 'AAM%' and vendor_name is null and s.company = 'FWP' then 'A & A MANUFACTURING' 
	when s.prod_num like 'ABB%' and vendor_name is null and s.company = 'FWP' then 'A & B BRUSH MFG CORP' 
	when s.prod_num like 'ABS%' and vendor_name is null and s.company = 'PEP' then 'COPPERFIT INDUSTRIES, INC.' 
	when s.prod_num like 'ACP%' and vendor_name is null and s.company = 'FWP' then 'AQUA CREEK PRODUCTS LLC'
	when s.prod_num like 'AER%' and vendor_name is null and s.company = 'FWP' then 'WILLIAMS INDUSTRIAL SALES CO.'
	when s.prod_num like 'ALC%' and vendor_name is null and s.company = 'FWP' then 'ALLCHEM INDUSTRIES INC.'
	when s.prod_num like 'ALD%' and vendor_name is null and s.company = 'FWP' then 'ALADDIN EQUIPMENT CO.'
	when s.prod_num like 'ALF%' and vendor_name is null and s.company = 'PEP' then 'ONESOURCE DISTRIBUTORS, LLC'
	when s.prod_num like 'ALP%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'AMI%' and vendor_name is null and s.company = 'PEP' then 'ACTIVE MINERALS INTERNATIONAL'
	when s.prod_num like 'AND%' and vendor_name is null and s.company = 'PEP' then 'ANDERSON MFG. CO. INC.'
	when s.prod_num like 'AND%' and vendor_name is null and s.company = 'FWP' then 'ANDERSON MANUFACTURING'
	when s.prod_num like 'AOS%' and vendor_name is null and s.company = 'PEP' then 'REGAL BELOIT EPC, INC.'
	when s.prod_num like 'API%' and vendor_name is null and s.company = 'PEP' then 'AQUA PRODUCTS INC.'
	when s.prod_num like 'AQC%' and vendor_name is null and s.company = 'PEP' then 'HACH COMPANY (ETS)'
	when s.prod_num like 'AQS%' and vendor_name is null and s.company = 'PEP' then 'AQUASTAR POOL PRODUCTS, INC.'
	when s.prod_num like 'AQS%' and vendor_name is null and s.company = 'FWP' then 'AQUA STAR POOL PRODUCTS, INC.'
	when s.prod_num like 'ARL%' and vendor_name is null and s.company = 'PEP' then 'ARLINGTON INDUSTRIES INC.'
	when s.prod_num like 'AS%' and vendor_name is null and s.company = 'FWP' then 'FLUIDRA USA LLC (ASTRAL)'
	when s.prod_num like 'AZ%' and vendor_name is null and s.company = 'PEP' then 'LASCO FITTINGS, INC.'
	when s.prod_num like 'BAY%' and vendor_name is null and s.company = 'FWP' then 'WATERCO  BAKER HYDRO DIVISION'
	when s.prod_num like 'BIO%' and vendor_name is null and s.company = 'PEP' then 'BIO-DEX LABORATORIES'
	when s.prod_num like 'BIR%' and vendor_name is null and s.company = 'FWP' then 'BIRCH INSTRUMENTS AND CONTROLS C'
	when s.prod_num like 'BOB%' and vendor_name is null and s.company = 'PEP' then 'BoBe Water & Fire, LLC.'
	when s.prod_num like 'BOLT%' and vendor_name is null and s.company = 'PEP' then 'INDUSTRIAL THREADED PROD., INC'
	when s.prod_num like 'BRN%' and vendor_name is null and s.company = 'PEP' then 'CHARMAN MANUFACTURING INC.'
	when s.prod_num like 'BRP%' and vendor_name is null and s.company = 'PEP' then 'WEST COAST COPPER & SUPPLY INC'
	when s.prod_num like 'BWI%' and vendor_name is null and s.company = 'FWP' then 'BLUE WHITE INDUSTRIES'
	when s.prod_num like 'CCR%' and vendor_name is null and s.company = 'FWP' then 'CCI GROUP LLC'
	when s.prod_num like 'CEN%' and vendor_name is null and s.company = 'PEP' then 'GEORG FISCHER CENTRAL PLASTICS'
	when s.prod_num like 'CLIP%' and vendor_name is null and s.company = 'PEP' then 'C.L. INDUSTRIES, INC'
	when s.prod_num like 'CLJ%' and vendor_name is null and s.company = 'PEP' then 'CAL-JUNE INC.'
	when s.prod_num like 'CLJ%' and vendor_name is null and s.company = 'FWP' then 'CAL JUNE INCORPORATED'
	when s.prod_num like 'CMI%' and vendor_name is null and s.company = 'FWP' then 'CONSOLIDATED MANUFACTURING INTL LLC'
	when s.prod_num like 'CMP%' and vendor_name is null and s.company = 'PEP' then 'CUSTOM MOLDED PRODUCTS, LLC'
	when s.prod_num like 'CMP%' and vendor_name is null and s.company = 'FWP' then 'CUSTOM MOLDED PRODUCTS'
	when s.prod_num like 'CODY%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'COMRE%' and vendor_name is null and s.company = 'FWP' then 'COLOR MATCH POOL FITTINGS, INC.'
	when s.prod_num like 'CPF%' and vendor_name is null and s.company = 'PEP' then 'COPPERFIT INDUSTRIES, INC.'
	when s.prod_num like 'CPNCMP1OFF%' and vendor_name is null and s.company = 'PEP' then 'CUSTOM MOLDED PRODUCTS, LLC'
	when s.prod_num like 'CPNCOR1OFF%' and vendor_name is null and s.company = 'PEP' then 'CORONA LIGHTING INC.'
	when s.prod_num like 'CPNFCH%' and vendor_name is null and s.company = 'PEP' then 'FIRST CHOICE POOL PRODUCTS'
	when s.prod_num like 'CPNFIL1OFF%' and vendor_name is null and s.company = 'PEP' then 'FILBUR MANUFACTURING, LLC.'
	when s.prod_num like 'CPNHAS1OFF%' and vendor_name is null and s.company = 'PEP' then 'HASA'
	when s.prod_num like 'CPNHAY1OFF%' and vendor_name is null and s.company = 'PEP' then 'HAYWARD INDUSTRIES, INC.'
	when s.prod_num like 'CPNJDY1OFF%' and vendor_name is null and s.company = 'PEP' then 'ZODIAC POOL SYSTEMS INC'
	when s.prod_num like 'CPNJJE1OFF%' and vendor_name is null and s.company = 'PEP' then 'J&J ELECTRONICS'
	when s.prod_num like 'CPNORN1OFF%' and vendor_name is null and s.company = 'PEP' then 'ORENDA TECHNOLOGIES'
	when s.prod_num like 'CPNORN1OFF%' and vendor_name is null and s.company = 'FWP' then 'ORENDA TECHNOLOGIES'
	when s.prod_num like 'CPNPEN1OFF%' and vendor_name is null and s.company = 'PEP' then 'PENTAIR WATER POOL'
	when s.prod_num like 'CPNPEP%' and vendor_name is null and s.company = 'PEP' then 'P.E.P. BRANCH PROMOTIONS'
	when s.prod_num like 'CPNPRX1OFF%' and vendor_name is null and s.company = 'PEP' then 'POOLRX WORLDWIDE'
	when s.prod_num like 'CPNRAY1OFF%' and vendor_name is null and s.company = 'PEP' then 'RAYPAK INC.'
	when s.prod_num like 'CPNSDGE1OFF%' and vendor_name is null and s.company = 'PEP' then 'SAN DIEGO GAS & ELECTRIC'
	when s.prod_num like 'CPNSUP1OFF%' and vendor_name is null and s.company = 'PEP' then 'DECKO PRODUCTS/ SUPERIOR PUMPS'
	when s.prod_num like 'CPNUSM1OFF%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'CUT%' and vendor_name is null and s.company = 'PEP' then 'PACIFIC PARTS & CONTROLS'
	when s.prod_num like 'DAN%' and vendor_name is null and s.company = 'FWP' then 'CUSTOM MOLDED PRODUCTS'
	when s.prod_num like 'DEL%' and vendor_name is null and s.company = 'PEP' then 'CUSTOM MOLDED PRODUCTS, LLC'
	when s.prod_num like 'DFC%' and vendor_name is null and s.company = 'PEP' then 'W.R. MEADOWS INC.'
	when s.prod_num like 'DIV%' and vendor_name is null and s.company = 'PEP' then 'DIVERSITECH CORPORATION'	
	when s.prod_num like 'ELELPL%' and vendor_name is null and s.company = 'FWP' then 'HALCO LIGHTING TECHNOLOGIES LLC'
	when s.prod_num like 'EMT%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'EZP%' and vendor_name is null and s.company = 'FWP' then 'E-Z PRODUCTS'
	when s.prod_num like 'FCH4%' and vendor_name is null and s.company = 'PEP' then 'WATERWAY'	
	when s.prod_num like 'FCH72%' and vendor_name is null and s.company = 'PEP' then 'FILBUR MANUFACTURING, LLC.'
	when s.prod_num like 'FIL%' and vendor_name is null and s.company = 'PEP' then 'FILBUR MANUFACTURING, LLC.'
	when s.prod_num like 'GLB%' and vendor_name is null and s.company = 'PEP' then 'ARCH CHEMICALS, INC.'
	when s.prod_num like 'GLB%' and vendor_name is null and s.company = 'FWP' then 'ADVANTIS TECHNOLOGIES'
	when s.prod_num like 'GLD%' and vendor_name is null and s.company = 'FWP' then 'ADVANTIS TECHNOLOGIES'
	when s.prod_num like 'GLF%' and vendor_name is null and s.company = 'PEP' then 'CHARMAN MANUFACTURING INC.'
	when s.prod_num like 'GRP%' and vendor_name is null and s.company = 'FWP' then 'GOODYEAR RUBBER PRODUCTS'
	when s.prod_num like 'HANDLING CHARGE%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'HAS%' and vendor_name is null and s.company = 'FWP' then 'HASA INC'
	when s.prod_num like 'HAY%' and vendor_name is null and s.company = 'PEP' then 'HAYWARD INDUSTRIES, INC.'
	when s.prod_num like 'HAY%' and vendor_name is null and s.company = 'FWP' then 'HAYWARD POOL PRODUCTS'
	when s.prod_num like 'HDR%' and vendor_name is null and s.company = 'PEP' then 'HYDRO-QUIP'
	when s.prod_num like 'HIP%' and vendor_name is null and s.company = 'FWP' then 'HARRINGTON INDUSTRIAL PLASTICS'
	when s.prod_num like 'IAS%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'INF%' and vendor_name is null and s.company = 'PEP' then 'INTER-FAB INC. *DO NOT USE*'
	when s.prod_num like 'INL%' and vendor_name is null and s.company = 'FWP' then 'INLAYS INC.'
	when s.prod_num like 'INT%' and vendor_name is null and s.company = 'FWP' then 'INTERMATIC INC'
	when s.prod_num like 'IPS%' and vendor_name is null and s.company = 'PEP' then 'IPS CORPORATION'
	when s.prod_num like 'J&J%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'JDY%' and vendor_name is null and s.company = 'PEP' then 'ZODIAC POOL SYSTEMS INC'
	when s.prod_num like 'JDY%' and vendor_name is null and s.company = 'FWP' then 'ZODIAC POOL SYSTEMS INC'
	when s.prod_num like 'KAF%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'KAF%' and vendor_name is null and s.company = 'FWP' then 'PLASTIFLEX NORTH CAROLINA, LLC'
	when s.prod_num like 'KGS%' and vendor_name is null and s.company = 'FWP' then 'KOOLGRIPS LLC'
	when s.prod_num like 'KGT%' and vendor_name is null and s.company = 'PEP' then 'KING TECHNOLOGY, INC.'
	when s.prod_num like 'KTC%' and vendor_name is null and s.company = 'FWP' then 'KELLEY TECHNICAL COATINGS INC'
	when s.prod_num like 'LAM%' and vendor_name is null and s.company = 'PEP' then 'LAMOTTE COMPANY'
	when s.prod_num like 'LAM%' and vendor_name is null and s.company = 'FWP' then 'LAMOTTE COMPANY'
	when s.prod_num like 'LAS%' and vendor_name is null and s.company = 'FWP' then 'LASCO FITTINGS INC'
	when s.prod_num like 'LNX%' and vendor_name is null and s.company = 'PEP' then 'LENOX - A NEWELL RUBBERMAID CO'
	when s.prod_num like 'LTCB%' and vendor_name is null and s.company = 'FWP' then 'ADVANTIS TECHNOLOGIES'
	when s.prod_num like 'LTG%' and vendor_name is null and s.company = 'FWP' then 'FRANKLIN ELECTRIC - WATER TRANSFER'
	when s.prod_num like 'MATBLK%' or s.prod_num like 'MATCU%' and vendor_name is null and s.company = 'FWP' then 'CHARMAN MFG'
	when s.prod_num like 'MATPG%' or s.prod_num like 'MATWM%' and vendor_name is null and s.company = 'FWP' then 'MATCO NORCA INC'
	when s.prod_num like 'MB3%' and vendor_name is null and s.company = 'FWP' then 'MARKET BUILDERS 3 LLC'
	when s.prod_num like 'MMM%' and vendor_name is null and s.company = 'PEP' then '3M ELECTRICAL MARKETS DIVISION'
	when s.prod_num like 'MOR%' and vendor_name is null and s.company = 'FWP' then 'MORTEX MANUFACTURING CO., INC.'
	when s.prod_num like 'MUV%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'MWC%' and vendor_name is null and s.company = 'FWP' then 'MIDWEST CANVAS CORPORATION'
	when s.prod_num like 'NAC%' and vendor_name is null and s.company = 'PEP' then 'NC BRANDS L.P.'
	when s.prod_num like 'NAT%' and vendor_name is null and s.company = 'FWP' then 'BIO-LAB INC'
	when s.prod_num like 'NCR%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'NDS%' and vendor_name is null and s.company = 'PEP' then 'FLO CONTROL INC C/O NDS'
	when s.prod_num like 'NDS%' and vendor_name is null and s.company = 'FWP' then 'NDS'
	when s.prod_num like 'NIN%' and vendor_name is null and s.company = 'PEP' then '009 IDEAS'
	when s.prod_num like 'NPP%' and vendor_name is null and s.company = 'FWP' then 'NATURAL POOL PRODUCTS'
	when s.prod_num like 'NSS%' and vendor_name is null and s.company = 'FWP' then 'NATIONAL STOCK SIGN CO.'
	when s.prod_num like 'OBW%' and vendor_name is null and s.company = 'PEP' then 'OCEAN BLUE WATER PRODUCTS'
	when s.prod_num like 'OPT%' and vendor_name is null and s.company = 'PEP' then 'ONTARIO POOL TILE'
	when s.prod_num like 'ORN%' and vendor_name is null and s.company = 'PEP' then 'ORENDA TECHNOLOGIES'
	when s.prod_num like 'OUT%' and vendor_name is null and s.company = 'PEP' then 'THE OUTDOOR PLUS'
	when s.prod_num like 'P4F%' and vendor_name is null and s.company = 'PEP' then 'SPEARS MFG COMPANY'
	when s.prod_num like 'PAP%' and vendor_name is null and s.company = 'FWP' then 'PURAQUA PRODUCTS INC'
	when s.prod_num like 'PAS%' and vendor_name is null and s.company = 'PEP' then 'PASCO SPECIALTY MFG. INC.'
	when s.prod_num like 'PAS%' and vendor_name is null and s.company = 'FWP' then 'PASCO SPECIALTY AND MFG'
	when s.prod_num like 'PEFB%' and vendor_name is null then 'GEORG FISCHER CENTRAL PLASTICS'
	when s.prod_num like 'PEN%' and vendor_name is null and s.company = 'PEP' then 'PENTAIR WATER POOL'
	when s.prod_num like 'PEN%' and vendor_name is null and s.company = 'FWP' then 'PENTAIR WATER POOL & SPA INC'
	when s.prod_num like 'PEP24%' and vendor_name is null and s.company = 'PEP' then 'TECHNO-SOLIS USA'
	when s.prod_num like 'PEPWATERBOTTLE%' and vendor_name is null and s.company = 'PEP' then 'P.E.P. BRANCH PROMOTIONS'
	when s.prod_num like 'PLT%' and vendor_name is null and s.company = 'FWP' then 'PALINTEST LTD'
	when s.prod_num like 'PMA%' and vendor_name is null and s.company = 'FWP' then 'POOLMASTER'
	when s.prod_num like 'PMC%' and vendor_name is null and s.company = 'FWP' then 'PEP (HOLDING ACCT)'
	when s.prod_num like 'PMI%' and vendor_name is null and s.company = 'FWP' then 'POOLMISER'
	when s.prod_num like 'POOLRX%' and vendor_name is null and s.company = 'FWP' then 'POOLRX WORLDWIDE INC'
	when s.prod_num like 'PPC%' and vendor_name is null and s.company = 'FWP' then 'PETERSEN PRODUCTS'
	when s.prod_num like 'PPG%' and vendor_name is null and s.company = 'FWP' then 'AXIALL LLC'
	when s.prod_num like 'PRA%' and vendor_name is null and s.company = 'FWP' then 'PRAHER PLASTICS CANADA LTD'
	when s.prod_num like 'PRX%' and vendor_name is null and s.company = 'FWP' then 'POOLRX WORLDWIDE INC'
	when s.prod_num like 'PSS%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'PTG%' and vendor_name is null and s.company = 'FWP' then 'SUNCOAST CHEMICALS CO.'
	when s.prod_num like 'PTK%' and vendor_name is null and s.company = 'FWP' then 'LIBERTY POOL PRODUCTS'
	when s.prod_num like 'PTM-K%' and vendor_name is null and s.company = 'FWP' then 'OCEAN BLUE WATER PRODUCTS'
	when s.prod_num like 'PVC1436%' or s.prod_num like 'PVC26%' or s.prod_num like 'PVC407%' and vendor_name is null and s.company = 'FWP' then 'SPEARS MANUFACTURING CO.'
	when s.prod_num like 'PVC43%' or s.prod_num like 'PVC44%' and vendor_name is null and s.company = 'FWP' then 'LASCO FITTINGS INC'
	when s.prod_num like 'PVC45%' or s.prod_num like 'PVC47%' and vendor_name is null and s.company = 'FWP' then 'SPEARS MANUFACTURING CO.'
	when s.prod_num like 'PVC85%' or s.prod_num like 'PVC88%' and vendor_name is null and s.company = 'FWP' then 'SPEARS MANUFACTURING CO.'
	when s.prod_num like 'PVCH%' or s.prod_num like 'PVCS%' and vendor_name is null and s.company = 'FWP' then 'SPEARS MANUFACTURING CO.'
	when s.prod_num like 'PVN%' and vendor_name is null and s.company = 'FWP' then 'HAYWARD POOL PRODUCTS'
	when s.prod_num like 'RAY%' and vendor_name is null and s.company = 'FWP' then 'RAYPAK'
	when s.prod_num like 'RBC%' and vendor_name is null and s.company = 'PEP' then 'RAIN BIRD CORPORATION'
	when s.prod_num like 'RED%' and vendor_name is null and s.company = 'PEP' then 'CHARMAN MANUFACTURING INC.'
	when s.prod_num like 'REL%' and vendor_name is null and s.company = 'FWP' then 'BRIDGING-CHINA INTERNATIONAL LTD'	
	when s.prod_num like 'RESTOCKING%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'SAR%' and vendor_name is null and s.company = 'FWP' then 'WATERDROP POOL PRODUCTS'
	when s.prod_num like 'SIE%' and vendor_name is null and s.company = 'PEP' then 'INNOVATIVE POOL PRODUCTS'
	when s.prod_num like 'SKL%' and vendor_name is null and s.company = 'PEP' then 'SKIMLITE MFG.'
	when s.prod_num like 'SLM%' and vendor_name is null and s.company = 'FWP' then 'SKIMLITE MFG.'
	when s.prod_num like 'SLURRY%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'SMB%' and vendor_name is null and s.company = 'FWP' then 'SMOOTH-BOR PLASTICS'
	when s.prod_num like 'SPC%' and vendor_name is null and s.company = 'FWP' then 'STENNER PUMP'
	when s.prod_num like 'SPES%' and vendor_name is null and s.company = 'PEP' then 'SPEARS MFG COMPANY'
	when s.prod_num like 'SRS%' and vendor_name is null and s.company = 'FWP' then 'SR SMITH LLC'
	when s.prod_num like 'STA%' and vendor_name is null and s.company = 'PEP' then 'PENTAIR WATER POOL'
	when s.prod_num like 'STE%' and vendor_name is null and s.company = 'PEP' then 'STEGMEIER CORP.'
	when s.prod_num like 'STE%' and vendor_name is null and s.company = 'FWP' then 'STEGMEIER CORPORTION'
	when s.prod_num like 'SWD%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'TAY%' and vendor_name is null and s.company = 'FWP' then 'TAYLOR TECHNOLOGIES INC'
	when s.prod_num like 'TBP%' and vendor_name is null and s.company = 'FWP' then 'ZODIAC POOL SYSTEMS INC'
	when s.prod_num like 'TBS%' and vendor_name is null and s.company = 'PEP' then 'MISC. VENDOR'
	when s.prod_num like 'TCI%' and vendor_name is null and s.company = 'FWP' then 'RAINMAKER SALES INC.'
	when s.prod_num like 'TCV%' and vendor_name is null and s.company = 'PEP' then 'CONSOLIDATED MFG. INTL.'
	when s.prod_num like 'TD%' and vendor_name is null and s.company = 'FWP' then 'T.DULA INC.'
	when s.prod_num like 'TEC%' and vendor_name is null and s.company = 'PEP' then 'TECHNO-SOLIS USA'
	when s.prod_num like 'TNB%' and vendor_name is null and s.company = 'PEP' then 'ONESOURCE DISTRIBUTORS, LLC'
	when s.prod_num like 'UCC%' and vendor_name is null and s.company = 'FWP' then 'UNITED CHEMICAL CORP'
	when s.prod_num like 'ULF%' and vendor_name is null and s.company = 'PEP' then 'JM EAGLE - UL FITTINGS'
	when s.prod_num like 'ULS%' and vendor_name is null and s.company = 'PEP' then 'UNIQUE LIGHTING SYSTEMS, INC.'
	when s.prod_num like 'UNI%' and vendor_name is null and s.company = 'PEP' then 'UNICEL FILTER CARTRIDGES'
	when s.prod_num like 'UNI%' and vendor_name is null and s.company = 'FWP' then 'UNICEL'
	when s.prod_num like 'USM%' and vendor_name is null and s.company = 'PEP' then 'NIDEC MOTOR CORPORATION'
	when s.prod_num like 'VAN%' and vendor_name is null and s.company = 'PEP' then 'KIK INTERNATIONAL'
	when s.prod_num like 'VLP%' and vendor_name is null and s.company = 'FWP' then 'VAL PAK PRODUCTS'
	when s.prod_num like 'VPV%' and vendor_name is null and s.company = 'PEP' then 'VAL-PAK'
	when s.prod_num like 'VTP%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'WATER%' and vendor_name is null and s.company = 'FWP' then 'WATERWAY PLASTICS'
	when s.prod_num like 'WGC%' and vendor_name is null and s.company = 'FWP' then 'MISC ONE TIME VENDOR'
	when s.prod_num like 'WWP%' and vendor_name is null and s.company = 'PEP' then 'WATERWAY'
	when s.prod_num like 'WWP%' and vendor_name is null and s.company = 'FWP' then 'WATERWAY PLASTICS'
	when s.prod_num like 'X10%' and vendor_name is null and s.company = 'FWP' then 'CLOSED  X10 PRO'
	when s.prod_num like 'ZOD%' and vendor_name is null and s.company = 'FWP' then 'ZODIAC POOL SYSTEMS INC'
		else vendor_name end vendor_name, 
s.prod_num, prod_desc, prod_group,
isnull(January2021Sales,0) January2021Sales, isnull(February2021Sales,0) February2021Sales, isnull(March2021Sales,0) March2021Sales, 
isnull(April2021Sales,0) April2021Sales, isnull(May2021Sales,0) May2021Sales, isnull(June2021Sales,0) June2021Sales, 
isnull(January2021UnitsSold,0) January2021UnitsSold, isnull(February2021UnitsSold,0) February2021UnitsSold, isnull(March2021UnitsSold,0) March2021UnitsSold, 
isnull(April2021UnitsSold,0) April2021UnitsSold, isnull(May2021UnitsSold,0) May2021UnitsSold, isnull(June2021UnitsSold,0) June2021UnitsSold
from FWEP_FMFY_Sales_UnitsSold s
left join FWEP_Vendor v on s.prod_num = v.prod_num and s.company = v.company
--group by s.company
--order by company, prod_num
)/*,

FWEP_Sales as (*/
select distinct 
company,
case 
	when vendor_id is null and company = 'PEP' and vendor_name = '009 IDEAS' then '103111'
	when vendor_id is null and company = 'PEP' and vendor_name = '3M ELECTRICAL MARKETS DIVISION' then '100322'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ACTIVE MINERALS INTERNATIONAL' then '000247'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ANDERSON MFG. CO. INC.' then '010274'
	when vendor_id is null and company = 'PEP' and vendor_name = 'AQUA PRODUCTS INC.' then '100996'
	when vendor_id is null and company = 'PEP' and vendor_name = 'AQUASTAR POOL PRODUCTS, INC.' then '100945'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ARCH CHEMICALS, INC.' then '032075'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ARLINGTON INDUSTRIES INC.' then '010370'
	when vendor_id is null and company = 'PEP' and vendor_name = 'BIO-DEX LABORATORIES' then '012080'
	when vendor_id is null and company = 'PEP' and vendor_name = 'BoBe Water & Fire, LLC.' then '000127'
	when vendor_id is null and company = 'PEP' and vendor_name = 'C.L. INDUSTRIES, INC' then '010816'
	when vendor_id is null and company = 'PEP' and vendor_name = 'CHARMAN MANUFACTURING INC.' then '101837'
	when vendor_id is null and company = 'PEP' and vendor_name = 'CONSOLIDATED MFG. INTL.' then '100336'
	when vendor_id is null and company = 'PEP' and vendor_name = 'COPPERFIT INDUSTRIES, INC.' then '101074'
	when vendor_id is null and company = 'PEP' and vendor_name = 'CORONA LIGHTING INC.' then '101812'
	when vendor_id is null and company = 'PEP' and vendor_name = 'CUSTOM MOLDED PRODUCTS, LLC' then '100975'
	when vendor_id is null and company = 'PEP' and vendor_name = 'DECKO PRODUCTS/ SUPERIOR PUMPS' then '102514'
	when vendor_id is null and company = 'PEP' and vendor_name = 'DIVERSITECH CORPORATION' then '016185'
	when vendor_id is null and company = 'PEP' and vendor_name = 'FILBUR MANUFACTURING, LLC.' then '025252'
	when vendor_id is null and company = 'PEP' and vendor_name = 'FIRST CHOICE POOL PRODUCTS' then '102938'
	when vendor_id is null and company = 'PEP' and vendor_name = 'FLO CONTROL INC C/O NDS' then '020600'
	when vendor_id is null and company = 'PEP' and vendor_name = 'GEORG FISCHER CENTRAL PLASTICS' then '014140'
	when vendor_id is null and company = 'PEP' and vendor_name = 'HACH COMPANY (ETS)' then '018560'
	when vendor_id is null and company = 'PEP' and vendor_name = 'HASA' then '102983'
	when vendor_id is null and company = 'PEP' and vendor_name = 'HAYWARD INDUSTRIES, INC.' then '024115'
	when vendor_id is null and company = 'PEP' and vendor_name = 'HYDRO-QUIP' then '100973'
	when vendor_id is null and company = 'PEP' and vendor_name = 'INDUSTRIAL THREADED PROD., INC' then '026174'
	when vendor_id is null and company = 'PEP' and vendor_name = 'INNOVATIVE POOL PRODUCTS' then '026166'
	when vendor_id is null and company = 'PEP' and vendor_name = 'INTER-FAB INC.' or  vendor_name = 'INTER-FAB INC. *DO NOT USE*' then '026164'
	when vendor_id is null and company = 'PEP' and vendor_name = 'IPS CORPORATION' then '026170'
	when vendor_id is null and company = 'PEP' and vendor_name = 'J&J ELECTRONICS' then '100830'
	when vendor_id is null and company = 'PEP' and vendor_name = 'JM EAGLE - UL FITTINGS' then '102943'
	when vendor_id is null and company = 'PEP' and vendor_name = 'KIK INTERNATIONAL' then '000033'
	when vendor_id is null and company = 'PEP' and vendor_name = 'KING TECHNOLOGY, INC.' then '030160'
	when vendor_id is null and company = 'PEP' and vendor_name = 'LAMOTTE COMPANY' then '032061'
	when vendor_id is null and company = 'PEP' and vendor_name = 'LASCO FITTINGS, INC.' then '032100'
	when vendor_id is null and company = 'PEP' and vendor_name = 'LENOX - A NEWELL RUBBERMAID CO' then '100440'
	when vendor_id is null and company = 'PEP' and vendor_name = 'MISC. VENDOR' then '099999'
	when vendor_id is null and company = 'PEP' and vendor_name = 'NC BRANDS L.P.' then '036053'
	when vendor_id is null and company = 'PEP' and vendor_name = 'NIDEC MOTOR CORPORATION' then '018385'
	when vendor_id is null and company = 'PEP' and vendor_name = 'OCEAN BLUE WATER PRODUCTS' then '101066'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ONESOURCE DISTRIBUTORS, LLC' then '100255'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ONTARIO POOL TILE' then '102794'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ORENDA TECHNOLOGIES' then '102662'
	when vendor_id is null and company = 'PEP' and vendor_name = 'P.E.P. BRANCH PROMOTIONS' then '102939'
	when vendor_id is null and company = 'PEP' and vendor_name = 'PACIFIC PARTS & CONTROLS' then '101020'
	when vendor_id is null and company = 'PEP' and vendor_name = 'PASCO SPECIALTY MFG. INC.' then '040270'
	when vendor_id is null and company = 'PEP' and vendor_name = 'PENTAIR WATER POOL' then '040550'
	when vendor_id is null and company = 'PEP' and vendor_name = 'POOLRX WORLDWIDE' then '040442'
	when vendor_id is null and company = 'PEP' and vendor_name = 'RAIN BIRD CORPORATION' then '101073'
	when vendor_id is null and company = 'PEP' and vendor_name = 'RAYPAK INC.' then '044310'
	when vendor_id is null and company = 'PEP' and vendor_name = 'REGAL BELOIT EPC, INC.' then '034024'
	when vendor_id is null and company = 'PEP' and vendor_name = 'S.R.SMITH,LLC' then '047095'
	when vendor_id is null and company = 'PEP' and vendor_name = 'SAN DIEGO GAS & ELECTRIC' then '19009'
	when vendor_id is null and company = 'PEP' and vendor_name = 'SKIMLITE MFG.' then '012582'
	when vendor_id is null and company = 'PEP' and vendor_name = 'SPEARS MFG COMPANY' then '046762'
	when vendor_id is null and company = 'PEP' and vendor_name = 'STEGMEIER CORP.' then '046808'
	when vendor_id is null and company = 'PEP' and vendor_name = 'TECHNO-SOLIS USA' then '101452'
	when vendor_id is null and company = 'PEP' and vendor_name = 'THE OUTDOOR PLUS' then '101778'
	when vendor_id is null and company = 'PEP' and vendor_name = 'TURLEY INTERNATIONAL RESOURCES' then '101382'
	when vendor_id is null and company = 'PEP' and vendor_name = 'UNIQUE LIGHTING SYSTEMS, INC.' then '100467'
	when vendor_id is null and company = 'PEP' and vendor_name = 'VAL-PAK' then '052355'
	when vendor_id is null and company = 'PEP' and vendor_name = 'W.R. MEADOWS INC.' then '053980'
	when vendor_id is null and company = 'PEP' and vendor_name = 'WATERWAY' then '054050'
	when vendor_id is null and company = 'PEP' and vendor_name = 'WEST COAST COPPER & SUPPLY INC' then '034035'
	when vendor_id is null and company = 'PEP' and vendor_name = 'ZODIAC POOL SYSTEMS INC' then '040412'

	when vendor_id is null and company = 'FWP' and vendor_name = 'A & A MANUFACTURING' then '17520'
	when vendor_id is null and company = 'FWP' and vendor_name = 'A & B BRUSH MFG CORP' then '4259'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ADVANTIS TECHNOLOGIES' then '10449'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ALADDIN EQUIPMENT CO.' then '4265'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ALLCHEM INDUSTRIES INC.' then '4266'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ANDERSON MANUFACTURING' then '4747'
	when vendor_id is null and company = 'FWP' and vendor_name = 'AQUA CREEK PRODUCTS LLC' then '13446'
	when vendor_id is null and company = 'FWP' and vendor_name = 'AQUA STAR POOL PRODUCTS, INC.' then '10890'
	when vendor_id is null and company = 'FWP' and vendor_name = 'AXIALL LLC' then '18173'
	when vendor_id is null and company = 'FWP' and vendor_name = 'BIO-LAB INC' then '5163'
	when vendor_id is null and company = 'FWP' and vendor_name = 'BIRCH INSTRUMENTS AND CONTROLS C' then '18174'
	when vendor_id is null and company = 'FWP' and vendor_name = 'BLUE WHITE INDUSTRIES' then '4276'
	when vendor_id is null and company = 'FWP' and vendor_name = 'BRIDGING-CHINA INTERNATIONAL LTD' then '13485'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CAL JUNE INCORPORATED' then '18175'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CCI GROUP LLC' then '20104'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CHARMAN MFG' then '18177'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CLOSED  X10 PRO' then '5568'
	when vendor_id is null and company = 'FWP' and vendor_name = 'COLOR MATCH POOL FITTINGS, INC.' then '5652'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CONSOLIDATED MANUFACTURING INTL LLC' then '4619'
	when vendor_id is null and company = 'FWP' and vendor_name = 'CUSTOM MOLDED PRODUCTS' then '5341'
	when vendor_id is null and company = 'FWP' and vendor_name = 'E-Z PRODUCTS' then '12787'
	when vendor_id is null and company = 'FWP' and vendor_name = 'FLUIDRA USA LLC (ASTRAL)' then '4273'
	when vendor_id is null and company = 'FWP' and vendor_name = 'FRANKLIN ELECTRIC - WATER TRANSFER' then '4570'
	when vendor_id is null and company = 'FWP' and vendor_name = 'GOODYEAR RUBBER PRODUCTS' then '4292'
	when vendor_id is null and company = 'FWP' and vendor_name = 'HALCO LIGHTING TECHNOLOGIES LLC' then '5210'
	when vendor_id is null and company = 'FWP' and vendor_name = 'HARRINGTON INDUSTRIAL PLASTICS' then '18216'
	when vendor_id is null and company = 'FWP' and vendor_name = 'HASA INC' then '13977'
	when vendor_id is null and company = 'FWP' and vendor_name = 'HAYWARD POOL PRODUCTS' then '4293'
	when vendor_id is null and company = 'FWP' and vendor_name = 'INLAYS INC.' then '4454'
	when vendor_id is null and company = 'FWP' and vendor_name = 'INTERMATIC INC' then '4299'
	when vendor_id is null and company = 'FWP' and vendor_name = 'KELLEY TECHNICAL COATINGS INC' then '4304'
	when vendor_id is null and company = 'FWP' and vendor_name = 'KOOLGRIPS LLC' then '14032'
	when vendor_id is null and company = 'FWP' and vendor_name = 'LAMOTTE COMPANY' then '4310'
	when vendor_id is null and company = 'FWP' and vendor_name = 'LASCO FITTINGS INC' then '4306'
	when vendor_id is null and company = 'FWP' and vendor_name = 'LIBERTY POOL PRODUCTS' then '18222'
	when vendor_id is null and company = 'FWP' and vendor_name = 'MARKET BUILDERS 3 LLC' then '18226'
	when vendor_id is null and company = 'FWP' and vendor_name = 'MATCO NORCA INC' then '18225'
	when vendor_id is null and company = 'FWP' and vendor_name = 'MIDWEST CANVAS CORPORATION' then '4314'
	when vendor_id is null and company = 'FWP' and vendor_name = 'MISC ONE TIME VENDOR' then '11434'
	when vendor_id is null and company = 'FWP' and vendor_name = 'MORTEX MANUFACTURING CO., INC.' then '11646'
	when vendor_id is null and company = 'FWP' and vendor_name = 'NATIONAL STOCK SIGN CO.' then '4316'
	when vendor_id is null and company = 'FWP' and vendor_name = 'NATURAL POOL PRODUCTS' then '19747'
	when vendor_id is null and company = 'FWP' and vendor_name = 'NDS' then '4289'
	when vendor_id is null and company = 'FWP' and vendor_name = 'OCEAN BLUE WATER PRODUCTS' then '5466'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ORENDA TECHNOLOGIES' then '13408'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PALINTEST LTD' then '18232'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PASCO SPECIALTY AND MFG' then '18233'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PENTAIR WATER POOL & SPA INC' then '4269'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PEP (HOLDING ACCT)' then '21799'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PETERSEN PRODUCTS' then '18235'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PLASTIFLEX NORTH CAROLINA, LLC' then '10445'
	when vendor_id is null and company = 'FWP' and vendor_name = 'POOLMASTER' then '4757'
	when vendor_id is null and company = 'FWP' and vendor_name = 'POOLMISER' then '4315'
	when vendor_id is null and company = 'FWP' and vendor_name = 'POOLRX WORLDWIDE INC' then '12589'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PRAHER PLASTICS CANADA LTD' then '4684'
	when vendor_id is null and company = 'FWP' and vendor_name = 'PURAQUA PRODUCTS INC' then '19748'
	when vendor_id is null and company = 'FWP' and vendor_name = 'RAINMAKER SALES INC.' then '19786'
	when vendor_id is null and company = 'FWP' and vendor_name = 'RAYPAK' then '17071'
	when vendor_id is null and company = 'FWP' and vendor_name = 'SKIMLITE MFG.' then '9158'
	when vendor_id is null and company = 'FWP' and vendor_name = 'SMOOTH-BOR PLASTICS' then '4330'
	when vendor_id is null and company = 'FWP' and vendor_name = 'SPEARS MANUFACTURING CO.' then '4342'
	when vendor_id is null and company = 'FWP' and vendor_name = 'SR SMITH LLC' then '4290'
	when vendor_id is null and company = 'FWP' and vendor_name = 'STEGMEIER CORPORTION' then '4339'
	when vendor_id is null and company = 'FWP' and vendor_name = 'STENNER PUMP' then '4340'
	when vendor_id is null and company = 'FWP' and vendor_name = 'SUNCOAST CHEMICALS CO.' then '5399'
	when vendor_id is null and company = 'FWP' and vendor_name = 'T.DULA INC.' then '18204'
	when vendor_id is null and company = 'FWP' and vendor_name = 'TAYLOR TECHNOLOGIES INC' then '4344'
	when vendor_id is null and company = 'FWP' and vendor_name = 'UNICEL' then '4348'
	when vendor_id is null and company = 'FWP' and vendor_name = 'UNITED CHEMICAL CORP' then '4346'
	when vendor_id is null and company = 'FWP' and vendor_name = 'VAL PAK PRODUCTS' then '10640'
	when vendor_id is null and company = 'FWP' and vendor_name = 'WATERCO  BAKER HYDRO DIVISION' then '4275'
	when vendor_id is null and company = 'FWP' and vendor_name = 'WATERDROP POOL PRODUCTS' then '15111'
	when vendor_id is null and company = 'FWP' and vendor_name = 'WATERWAY PLASTICS' then '4354'
	when vendor_id is null and company = 'FWP' and vendor_name = 'WILLIAMS INDUSTRIAL SALES CO.' then '18169'
	when vendor_id is null and company = 'FWP' and vendor_name = 'ZODIAC POOL SYSTEMS INC' then '4279'

		else vendor_id end vendor_id,
vendor_name, prod_num, prod_desc, prod_group, January2021Sales, February2021Sales, March2021Sales, April2021Sales, May2021Sales, June2021Sales,
January2021UnitsSold, February2021UnitsSold, March2021UnitsSold, April2021UnitsSold, May2021UnitsSold, June2021UnitsSold
from 
FWEP_FMFY_Sales_UnitsSold_Vendor
where January2021Sales <> '0' or February2021Sales <> '0' or March2021Sales <> '0' or April2021Sales <> '0' or May2021Sales <> '0' or June2021Sales <> '0' 
	or January2021UnitsSold <> '0' or February2021UnitsSold <> '0' or March2021UnitsSold <> '0' or April2021UnitsSold <> '0' or May2021UnitsSold <> '0' or June2021UnitsSold <> '0'
order by prod_num
--)

/*
------- Sales by Vendor & SKU -----------
select * from FWEP_Sales
--where vendor_name is null --and company = 'PEP'
where vendor_id is null and company = 'FWP'
--order by prod_num
order by vendor_name
------- Sales by Vendor & SKU -----------
--19402 rows 6/30/21 4:53pm
--19439 rows 6/30/21 5:12pm
--19464 rows 7/1/21 12:34pm
*/

/*
------- Sales by Vendor -----------
select distinct company, 
vendor_id, 
vendor_name, 
sum(January2021Sales) January2021Sales, 
sum(February2021Sales) February2021Sales, 
sum(March2021Sales) March2021Sales, 
sum(April2021Sales) April2021Sales, 
sum(May2021Sales) May2021Sales, 
sum(June2021Sales) June2021Sales,
sum(January2021UnitsSold) January2021UnitsSold, 
sum(February2021UnitsSold) February2021UnitsSold, 
sum(March2021UnitsSold) March2021UnitsSold, 
sum(April2021UnitsSold) April2021UnitsSold, 
sum(May2021UnitsSold) May2021UnitsSold, 
sum(June2021UnitsSold) June2021UnitsSold
from FWEP_Sales
group by company, 
vendor_id, 
vendor_name/*, January2021Sales, February2021Sales, March2021Sales, April2021Sales, May2021Sales, June2021Sales,
January2021UnitsSold, February2021UnitsSold, March2021UnitsSold, April2021UnitsSold, May2021UnitsSold, June2021UnitsSold*/
order by company, vendor_name
------- Sales by Vendor -----------
--678 rows 6/30/21 5:12pm
--679 rows 7/1/21 12:43pm
--758 rows 7/1/21 1:39pm
--682 rows 7/2/21 6:22pm
*/