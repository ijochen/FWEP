import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger('main-logger')

logger.setLevel(logging.INFO)
logger.info("Test log message")

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# 1) Load PEP - Read from FWP_SQL.Prelude Database and write to Data Warehouse 
logger.info("******** START READING PEP *************")

# Invoice last 30 days
pep_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=Prelude"
pep_query = """(
--pep 2019 sales 2,182,660 rows 8/16/21
--pep 2020 sales 2,512,217 rows 8/16/21
--pep 2021 sales 1,713,921 rows 8/16/21

--pep 2019 sales 2,184,655 rows 8/19/21
--pep 2020 sales 2,514,532 rows 8/19/21
--pep 2021 sales 1,773,246 rows 8/19/21

--pep 2019 sales 2,187,145 rows 9/8/21
--pep 2020 sales 2,516,760 rows 9/8/21
--pep 2021 sales 1,931,399 rows 9/8/21

	select --distinct --top(100)
		'PEP' as COMPANY,
		CT.CT_DESC CUST_TYPE,
		OH.CUST_NUM, 
		CU.CUST_DESC,
		CU.CITY CUST_CITY,
		CU.STATE CUST_STATE,
		OH.USER_ID,
		U.USER_DESC USER_NAME,
		OH.CUST_PO_NUM,
		OH.ORD_NUM, 
		OL.SEQ_NUM LINE_NUM, 
		OL.PROD_NUM, 
		concat(p.PROD_DESC1,' ',p.PROD_DESC2) PROD_DESC,
		ca.PLINE_DESC PROD_CATEGORY,
		OL.UN_MEAS,
		cast(OL.ORD_QTY as float) ORD_QTY, 
		cast(OL.SHP_QTY as float) SHP_QTY, 
		OL.NET_PRICE, 
		(OL.NET_PRICE * OL.SHP_QTY) EXT_PRICE,
		OL.PROFIT_EXT, 
		(OL.ACCT_COST * OL.SHP_QTY) EXT_COST, 
		(INV_AMT - TOT_ORD_DOL) TAX,
		OH.TOT_ORD_DOL,
		OH.INV_AMT,
		cast(OH.INV_DATE as date) INV_DATE, 
		OH.INV_NUM,
		min(substring(MISC_GL,14,17)) GL_ACCOUNT_NUM,
		OL.WHSE_NUM,
		case	 
			when OL.WHSE_NUM = '01' then 'PEP - ANAHEIM CA'
			when OL.WHSE_NUM = '02' then 'PEP - INDIO CA'
			when OL.WHSE_NUM = '03' then 'PEP - EL CAJON CA'
			when OL.WHSE_NUM = '04' then 'PEP - MURRIETA CA'
			when OL.WHSE_NUM = '05' then 'PEP - LIVERMORE CA'
			when OL.WHSE_NUM = '06' then 'PEP - ONTARIO CA'
			when OL.WHSE_NUM = '07' then 'PEP - SAN DIMAS CA'
			when OL.WHSE_NUM = '08' then 'PEP - CATHEDRAL CITY CA'
			when OL.WHSE_NUM = '09' then 'PEP - SAN FERNANDO CA'
			when OL.WHSE_NUM = '10' then 'PEP - VISALIA CA'
			when OL.WHSE_NUM = '11' then 'PEP - SAN ANTONIO TX'
			when OL.WHSE_NUM = '12' then 'PEP - VISTA CA'
			when OL.WHSE_NUM = '13' then 'PEP - AUSTIN TX'
			when OL.WHSE_NUM = '14' then 'PEP - PALM SPRINGS CA'
			when OL.WHSE_NUM = '15' then 'PEP - CORONA CA'
			when OL.WHSE_NUM = '16' then 'PEP - BAKERSFIELD CA'
			when OL.WHSE_NUM = '17' then 'PEP - HOUSTON TX'
			when OL.WHSE_NUM = '18' then 'PEP - LAKE FOREST CA'
			when OL.WHSE_NUM = '19' then 'PEP - MOORPARK CA'
			when OL.WHSE_NUM = '20' then 'PEP - NORTH AUSTIN TX'
			when OL.WHSE_NUM = '21' then 'PEP - DUARTE CA'
			when OL.WHSE_NUM = '22' then 'PEP - YUCAIPA CA'
			when OL.WHSE_NUM = '23' then 'PEP - RIVERSIDE CA'
			when OL.WHSE_NUM = '24' then 'PEP - LONG BEACH CA'
			when OL.WHSE_NUM = '25' then 'PEP - PALM DESERT CA'
			when OL.WHSE_NUM = '26' then 'PEP - LOS ANGELES CA'
			when OL.WHSE_NUM = '27' then 'PEP - TEMPE AZ'
			when OL.WHSE_NUM = '28' then 'PEP - PHOENIX AZ'
			when OL.WHSE_NUM = '29' then 'PEP - SANTA ANA CA'
			when OL.WHSE_NUM = '30' then 'PEP - EL CENTRO CA'
			when OL.WHSE_NUM = '98' then 'PEP - CORPORATE WAREHOUSE'
			when OL.WHSE_NUM = '99' then 'PEP - CENTRAL SHIPPING WAREHOUSE'
				else OL.WHSE_NUM end WHSE_NAME,
		OH.SEL_WHSE, 
		case	 
			when OH.SEL_WHSE = '01' then 'PEP - ANAHEIM CA'
			when OH.SEL_WHSE = '02' then 'PEP - INDIO CA'
			when OH.SEL_WHSE = '03' then 'PEP - EL CAJON CA'
			when OH.SEL_WHSE = '04' then 'PEP - MURRIETA CA'
			when OH.SEL_WHSE = '05' then 'PEP - LIVERMORE CA'
			when OH.SEL_WHSE = '06' then 'PEP - ONTARIO CA'
			when OH.SEL_WHSE = '07' then 'PEP - SAN DIMAS CA'
			when OH.SEL_WHSE = '08' then 'PEP - CATHEDRAL CITY CA'
			when OH.SEL_WHSE = '09' then 'PEP - SAN FERNANDO CA'
			when OH.SEL_WHSE = '10' then 'PEP - VISALIA CA'
			when OH.SEL_WHSE = '11' then 'PEP - SAN ANTONIO TX'
			when OH.SEL_WHSE = '12' then 'PEP - VISTA CA'
			when OH.SEL_WHSE = '13' then 'PEP - AUSTIN TX'
			when OH.SEL_WHSE = '14' then 'PEP - PALM SPRINGS CA'
			when OH.SEL_WHSE = '15' then 'PEP - CORONA CA'
			when OH.SEL_WHSE = '16' then 'PEP - BAKERSFIELD CA'
			when OH.SEL_WHSE = '17' then 'PEP - HOUSTON TX'
			when OH.SEL_WHSE = '18' then 'PEP - LAKE FOREST CA'
			when OH.SEL_WHSE = '19' then 'PEP - MOORPARK CA'
			when OH.SEL_WHSE = '20' then 'PEP - NORTH AUSTIN TX'
			when OH.SEL_WHSE = '21' then 'PEP - DUARTE CA'
			when OH.SEL_WHSE = '22' then 'PEP - YUCAIPA CA'
			when OH.SEL_WHSE = '23' then 'PEP - RIVERSIDE CA'
			when OH.SEL_WHSE = '24' then 'PEP - LONG BEACH CA'
			when OH.SEL_WHSE = '25' then 'PEP - PALM DESERT CA'
			when OH.SEL_WHSE = '26' then 'PEP - LOS ANGELES CA'
			when OH.SEL_WHSE = '27' then 'PEP - TEMPE AZ'
			when OH.SEL_WHSE = '28' then 'PEP - PHOENIX AZ'
			when OH.SEL_WHSE = '29' then 'PEP - SANTA ANA CA'
			when OH.SEL_WHSE = '30' then 'PEP - EL CENTRO CA'
			when OH.SEL_WHSE = '98' then 'PEP - CORPORATE WAREHOUSE'
			when OH.SEL_WHSE = '99' then 'PEP - CENTRAL SHIPPING WAREHOUSE'
				else OH.SEL_WHSE end SEL_WHSE_NAME,
		OH.SHIP_VIA_NUM,
		S.SHIP_VIA_DESC,
		cast(OH.ORD_DATE as datetime) ORD_DATE, 
		cast(OH.PCK_DATE as datetime) PCK_DATE,
		case when OH.REQ_DATE = '1210-10-11' then '2021-10-11' else cast(OH.REQ_DATE as datetime) end REQ_DATE
	from Prelude.dbo.ORDER_HISTORY_LINE_IJO_1 OL
	left join Prelude.dbo.ORDER_HISTORY_IJO OH on left(OL.ID, charindex('!',OL.ID)-1) = OH.ID 
	left join Prelude.dbo.PRODUCT_IJO P on OL.PROD_NUM = p.PROD_NUM
	left join Prelude.dbo.CATEGORY_IJO CA on p.PLINE_NUM = ca.PLINE_NUM
	left join Prelude.dbo.USER_ID_NF U on OH.USER_ID = u.USER_NUM
	left join Prelude.dbo.SHIP_VIA_NF_IJO S on oh.SHIP_VIA_NUM = s.SHIP_VIA_NUM
	left join Prelude.dbo.CUSTOMER_IJO CU on OH.CUST_NUM = cu.CUST_NUM
	left join Prelude.dbo.CUST_TYPE_1_NF CT on cu.TYPE = CT.CT_NUM 
	where year(OH.INV_DATE) = 2021 and ol.CO_NUM = '001' and oh.ID like '001%' and p.CO_NUM = '001' and ca.CO_NUM = '001' and s.ID like '001%' and cu.ID like '001%'
	group by 
		CT.CT_DESC,
		OH.CUST_NUM,
		CU.CUST_DESC,
		CU.CITY,
		CU.STATE,
		OH.USER_ID,
		U.USER_DESC,
		OH.SHIP_VIA_NUM,
		OH.ORD_DATE,
		OH.CUST_PO_NUM,
		OH.ORD_NUM, 
		OL.SEQ_NUM, 
		OL.PROD_NUM, 
		p.PROD_DESC1, 
		p.PROD_DESC2,
		ca.PLINE_DESC,
		OL.UN_MEAS,
		OL.ORD_QTY, 
		OL.SHP_QTY, 
		OL.NET_PRICE, 
		--OL.LINE_EXT,
		OL.PROFIT_EXT, 
		--OL.NET_COST_EXT, 
		OL.ACCT_COST,
		OH.TOT_ORD_DOL,
		OH.INV_AMT,
		OH.INV_DATE, 
		OH.INV_NUM,
		OL.MISC_GL,
		OH.SEL_WHSE, 
		OL.WHSE_NUM,
		OH.SHIP_VIA_NUM,
		S.SHIP_VIA_DESC,
		OH.ORD_DATE,
		OH.PCK_DATE,
		OH.REQ_DATE
)"""

pep_df = spark.read.format("jdbc") \
   .option("url", pep_url) \
   .option("query", pep_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
pep_df.write.jdbc(url=url, table="sales.pep_sales_2021", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

# logger.info("******** START READING FWP *************")


# fwp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
# fwp_query = """(
# --fwp 2019 sales 956,751 rows
# --fwp 2020 sales 1,077,202 rows
# --fwp 2021 sales 822,194 rows
# 	select --distinct --top(100)
# 		'FWP' as company,
# 		c.class_1id customer_type,
# 		ih.customer_id, 
# 		ih.bill2_name customer_name, 
# 		ih.bill2_city customer_city,
# 		ih.bill2_state customer_state,
# 		oh.taker created_by,
# 		u.name created_by_name,
# 		ol.cust_po_no,
# 		ol.order_no, 
# 		cast(ol.line_no as varchar) line_no, 
# 		il.item_id prod_num, 
# 		il.item_desc prod_desc, 
# 		pg.product_group_desc prod_group,
# 		il.unit_of_measure,
# 		cast(qty_requested as float) qty_ordered, 
# 		cast(qty_shipped as float) qty_shipped, 
# 		il.unit_price, 
# 		il.extended_price,
# 		(il.extended_price - il.cogs_amount) profit_amount,
# 		il.cogs_amount, 
# 		ih.tax_amount, 
# 		ih.amount_paid paid_amount, 
# 		ih.total_amount invoice_amount, 
# 		cast(ih.invoice_date as datetime) invoice_date, 
# 		ih.invoice_no, 
# 		il.gl_revenue_account_no,
# 		cast(ih.sales_location_id as varchar) sales_location_id,
# 		b1.branch_description sales_location_name,
# 		ih.branch_id,
# 		b2.branch_description branch_name,
# 		oh.front_counter trans_type,
# 		case when oh.front_counter = 'Y' then 'FRONT COUNTER'
# 			when oh.front_counter = 'N' then 'DELIVERY' end trans_desc,
# 		cast(ih.order_date as datetime) order_date, 
# 		cast(ih.ship_date as datetime) ship_date,
# 		cast(ol.required_date as datetime) required_date
# 	from CommerceCenter.dbo.invoice_line il
# 	left join CommerceCenter.dbo.invoice_hdr ih on il.invoice_no = ih.invoice_no
# 	join CommerceCenter.dbo.oe_line ol on il.order_no = ol.order_no and il.oe_line_number = ol.line_no-- and il.customer_part_number = ol.customer_part_number
# 	left join CommerceCenter.dbo.oe_hdr oh on ol.order_no = oh.order_no
# 	left join CommerceCenter.dbo.branch b1 on ih.sales_location_id = b1.branch_id
# 	left join CommerceCenter.dbo.branch b2 on ih.branch_id = b2.branch_id
# 	left join CommerceCenter.dbo.product_group pg on il.product_group_id = pg.product_group_id
# 	left join CommerceCenter.dbo.customer c on ih.customer_id = c.customer_id
# 	left join CommerceCenter.dbo.users u on oh.taker = u.id
# 	where year(invoice_date) = 2019 and gl_revenue_account_no like '4000%' and il.invoice_line_type = '0' and ih.invoice_adjustment_type = 'I' and c.customer_name not like 'POOL AND ELECTRICAL PRODUCTS' 
# 	group by 
# 		c.class_1id,
# 		ih.customer_id, 
# 		ih.bill2_name, 
# 		ih.bill2_city,
# 		ih.bill2_state,
# 		oh.taker,
# 		u.name,
# 		ol.cust_po_no,
# 		ol.order_no, 
# 		ol.line_no, 
# 		item_id, 
# 		item_desc, 
# 		pg.product_group_desc,
# 		il.unit_of_measure,
# 		qty_requested, 
# 		qty_shipped, 
# 		il.unit_price, 
# 		il.extended_price,
# 		cogs_amount, 
# 		tax_amount, 
# 		amount_paid, 
# 		total_amount, 
# 		invoice_date, 
# 		ih.invoice_no, 
# 		gl_revenue_account_no,
# 		sales_location_id, 
# 		b1.branch_description,
# 		ih.branch_id, 
# 		b2.branch_description,
# 		oh.front_counter,
# 		ih.order_date, 
# 		ih.ship_date,
# 		ol.required_date
# )"""

# fwp_df = spark.read.format("jdbc") \
#    .option("url", fwp_url) \
#    .option("query", fwp_query) \
#    .option("user", "ichen") \
#    .option("password", "Qwer1234$") \
#    .load()
   
# mode = "overwrite"
# url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
# properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
# fwp_df.write.jdbc(url=url, table="sales.fwp_sales_2019", mode=mode, properties=properties)


# logger.info("******** END READING FWP *************")


job.commit()