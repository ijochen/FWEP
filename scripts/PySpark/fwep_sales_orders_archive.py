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
    select distinct
        'PEP' as company,
        t3.CUST_NUM, 
        max(cu.CUST_DESC) CUST_DESC,
        max(cu.CITY) CUST_CITY,
        max(cu.STATE) CUST_STATE,
        cast(t3.ORD_DATE as datetime) ORD_DATE, 
        t3.ORD_NUM, 
        t1.SEQ_NUM LINE_NUM, 
        t1.PROD_NUM, 
        p.PROD_DESC1 PROD_DESC,
        ca.PLINE_DESC PROD_GROUP,
        cast(t1.ORD_QTY as float) ORD_QTY, 
        cast(t1.SHP_QTY as float) SHP_QTY, 
        t1.NET_PRICE, 
        t1.LINE_EXT,
        t1.PROFIT_EXT, 
        t1.NET_COST_EXT, 
        (INV_AMT - TOT_ORD_DOL) TAX,
        t3.TOT_ORD_DOL,
        t3.INV_AMT,
        cast(t3.INV_DATE as datetime) INV_DATE, 
        t3.INV_NUM,
        substring(MISC_GL,14,17) GL_ACCOUNT_NUM,
        t3.SEL_WHSE, 
        case 
            when t3.SEL_WHSE = '01' then 'Anaheim'
            when t3.SEL_WHSE = '02' then 'Indio'
            when t3.SEL_WHSE = '03' then 'El Cajon'
            when t3.SEL_WHSE = '04' then 'Murrieta'
            when t3.SEL_WHSE = '05' then 'Livermore'
            when t3.SEL_WHSE = '06' then 'Ontario'
            when t3.SEL_WHSE = '07' then 'San Dimas'
            when t3.SEL_WHSE = '08' then 'Cathedral City'
            when t3.SEL_WHSE = '09' then 'San Fernando'
            when t3.SEL_WHSE = '10' then 'Visalia'
            when t3.SEL_WHSE = '11' then 'San Antonio (PEP)'
            when t3.SEL_WHSE = '12' then 'Vista'
            when t3.SEL_WHSE = '13' then 'Austin (PEP)'
            when t3.SEL_WHSE = '14' then 'Palm Springs'
            when t3.SEL_WHSE = '15' then 'Corona'
            when t3.SEL_WHSE = '16' then 'Bakersfield'
            when t3.SEL_WHSE = '17' then 'Houston'
            when t3.SEL_WHSE = '18' then 'Lake Forest'
            when t3.SEL_WHSE = '19' then 'Moorpark'
            when t3.SEL_WHSE = '20' then 'North Austin'
            when t3.SEL_WHSE = '21' then 'Duarte'
            when t3.SEL_WHSE = '22' then 'Yucaipa'
            when t3.SEL_WHSE = '23' then 'Riverside'
            when t3.SEL_WHSE = '24' then 'Long Beach'
            when t3.SEL_WHSE = '25' then 'Palm Desert'
            when t3.SEL_WHSE = '26' then 'Los Angeles'
            when t3.SEL_WHSE = '27' then 'Tempe'
            when t3.SEL_WHSE = '28' then 'Phoenix'
            when t3.SEL_WHSE = '29' then 'Santa Ana'
            when t3.SEL_WHSE = '30' then 'El Centro'
            when t3.SEL_WHSE = '98' then 'Corporate Warehouse'
            when t3.SEL_WHSE = '99' then 'Central Shipping Warehouse'
                else t3.SEL_WHSE end SEL_WHSE_NAME,
        t1.WHSE_NUM,
        case 
            when t1.WHSE_NUM = '01' then 'Anaheim'
            when t1.WHSE_NUM = '02' then 'Indio'
            when t1.WHSE_NUM = '03' then 'El Cajon'
            when t1.WHSE_NUM = '04' then 'Murrieta'
            when t1.WHSE_NUM = '05' then 'Livermore'
            when t1.WHSE_NUM = '06' then 'Ontario'
            when t1.WHSE_NUM = '07' then 'San Dimas'
            when t1.WHSE_NUM = '08' then 'Cathedral City'
            when t1.WHSE_NUM = '09' then 'San Fernando'
            when t1.WHSE_NUM = '10' then 'Visalia'
            when t1.WHSE_NUM = '11' then 'San Antonio (PEP)'
            when t1.WHSE_NUM = '12' then 'Vista'
            when t1.WHSE_NUM = '13' then 'Austin (PEP)'
            when t1.WHSE_NUM = '14' then 'Palm Springs'
            when t1.WHSE_NUM = '15' then 'Corona'
            when t1.WHSE_NUM = '16' then 'Bakersfield'
            when t1.WHSE_NUM = '17' then 'Houston'
            when t1.WHSE_NUM = '18' then 'Lake Forest'
            when t1.WHSE_NUM = '19' then 'Moorpark'
            when t1.WHSE_NUM = '20' then 'North Austin'
            when t1.WHSE_NUM = '21' then 'Duarte'
            when t1.WHSE_NUM = '22' then 'Yucaipa'
            when t1.WHSE_NUM = '23' then 'Riverside'
            when t1.WHSE_NUM = '24' then 'Long Beach'
            when t1.WHSE_NUM = '25' then 'Palm Desert'
            when t1.WHSE_NUM = '26' then 'Los Angeles'
            when t1.WHSE_NUM = '27' then 'Tempe'
            when t1.WHSE_NUM = '28' then 'Phoenix'
            when t1.WHSE_NUM = '29' then 'Santa Ana'
            when t1.WHSE_NUM = '30' then 'El Centro'
            when t1.WHSE_NUM = '98' then 'Corporate Warehouse'
            when t1.WHSE_NUM = '99' then 'Central Shipping Warehouse'
                else t1.WHSE_NUM end WHSE_NAME,
        datename(month,t3.INV_DATE) MONTH_NAME
        from Prelude.dbo.ORDER_HISTORY_LINE_IJO_1 t1 
        left join Prelude.dbo.ORDER_HIST_LINE_KEY__MV_SUB t2 on t1.ID = t2.LINE_KEY
        left join Prelude.dbo.ORDER_HISTORY_IJO t3 on t2.ID = t3.ID
        left join Prelude.dbo.CUSTOMER_IJO cu on t3.CUST_NUM = cu.CUST_NUM
        left join Prelude.dbo.SSProduct p on t1.PROD_NUM = p.PROD_NUM
        left join Prelude.dbo.CATEGORY_IJO ca on p.PLINE_NUM = ca.PLINE_NUM
        where INV_DATE >= '2020-01-01' and t1.PROD_NUM not in ('C','CSB','CS','CI','CN','CP','MN') and cu.CUST_NUM not like '%CLOSED' and p.CO_NUM = '001' and ca.CO_NUM = '001'
        group by 
            t3.CUST_NUM,
            t3.ORD_DATE, 
            t3.ORD_NUM, 
            t1.SEQ_NUM, 
            t1.PROD_NUM, 
            p.PROD_DESC1,
            ca.PLINE_DESC,
            t1.ORD_QTY, 
            t1.SHP_QTY, 
            t1.NET_PRICE, 
            t1.LINE_EXT,
            t1.PROFIT_EXT, 
            t1.NET_COST_EXT, 
            t3.TOT_ORD_DOL,
            t3.INV_AMT,
            t3.INV_DATE, 
            t3.INV_NUM,
            t1.MISC_GL,
            t3.SEL_WHSE, 
            t1.WHSE_NUM
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
pep_df.write.jdbc(url=url, table="sales.pep_sales", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

logger.info("******** START READING FWP *************")


fwp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
fwp_query = """(
    select distinct
        'FWP' as company,
        ih.customer_id, 
        ih.bill2_name customer_name, 
        ih.bill2_city customer_city,
        ih.bill2_state customer_state,
        ih.order_date, 
        il.order_no, 
        cast(il.line_no as varchar) line_no, 
        il.item_id prod_num, 
        il.item_desc prod_desc, 
        pg.product_group_desc prod_group,
        cast(qty_requested as float) qty_ordered, 
        cast(qty_shipped as float) qty_shipped, 
        unit_price, 
        extended_price,
        (extended_price - cogs_amount) profit_amount,
        cogs_amount, 
        tax_amount, 
        amount_paid paid_amount, 
        total_amount invoice_amount, 
        invoice_date, 
        ih.invoice_no, 
        gl_revenue_account_no,
        cast(ih.sales_location_id as varchar) sales_location_id,
        case 
            when sales_location_id = 1 or sales_location_id = 10 then 'Tampa'
            when sales_location_id = 20 then 'Spring Hill'
            when sales_location_id = 30 then 'Port Charlotte'
            when sales_location_id = 35 then 'Sarasota'
            when sales_location_id = 40 then 'North Miami'
            when sales_location_id = 50 then 'Naples'
            when sales_location_id = 60 then 'West Palm Beach'
            when sales_location_id = 70 then 'Melbourne'
            when sales_location_id = 80 then 'Fulfillment'
            when sales_location_id = 90 then 'Conroe'
            when sales_location_id = 100 then 'Katy'
            when sales_location_id = 110 then 'Austin (FWP)'
            when sales_location_id = 120 then 'Plano'
            when sales_location_id = 130 then 'San Antonio (FWP)'
            when sales_location_id = 140 then 'Fort Worth'
            when sales_location_id = 200 then 'Meade'
            when sales_location_id = 210 then 'La Costa'
            when sales_location_id = 220 then 'Henderson'
            when sales_location_id = 250 then 'St. George'
            when sales_location_id = 304 then 'Murrieta'
            when sales_location_id = 305 then 'Livermore'
            when sales_location_id = 330 then 'El Centro'
            when sales_location_id = 331 then 'Rancho Cordova'
            when sales_location_id = 9289 then 'Warranty West Coast'
            when sales_location_id = 9290 then 'Warranty American'
                else sl.location_name end sales_location_name,
        ih.branch_id,
        case 
            when ih.branch_id = 1 or ih.branch_id = 10 then 'Tampa'
            when ih.branch_id = 20 then 'Spring Hill'
            when ih.branch_id = 30 then 'Port Charlotte'
            when ih.branch_id = 35 then 'Sarasota'
            when ih.branch_id = 40 then 'North Miami'
            when ih.branch_id = 50 then 'Naples'
            when ih.branch_id = 60 then 'West Palm Beach'
            when ih.branch_id = 70 then 'Melbourne'
            when ih.branch_id = 80 then 'Fulfillment'
            when ih.branch_id = 90 then 'Conroe'
            when ih.branch_id = 100 then 'Katy'
            when ih.branch_id = 110 then 'Austin (FWP)'
            when ih.branch_id = 120 then 'Plano'
            when ih.branch_id = 130 then 'San Antonio (FWP)'
            when ih.branch_id = 140 then 'Fort Worth'
            when ih.branch_id = 200 then 'Meade'
            when ih.branch_id = 210 then 'La Costa'
            when ih.branch_id = 220 then 'Henderson'
            when ih.branch_id = 250 then 'St. George'
            when ih.branch_id = 304 then 'Murrieta'
            when ih.branch_id = 305 then 'Livermore'
            when ih.branch_id = 330 then 'El Centro'
            when ih.branch_id = 331 then 'Rancho Cordova'
            when ih.branch_id = 9289 then 'Warranty West Coast'
            when ih.branch_id = 9290 then 'Warranty American'
                else bl.location_name end branch_location_name,
        datename(month,invoice_date) month_name
    from CommerceCenter.dbo.invoice_hdr ih 
    left join CommerceCenter.dbo.invoice_line il on ih.invoice_no = il.invoice_no
    left join CommerceCenter.dbo.location sl on ih.sales_location_id = sl.location_id
    left join CommerceCenter.dbo.location bl on ih.branch_id = bl.location_id
    left join CommerceCenter.dbo.product_group pg on il.product_group_id = pg.product_group_id
    where year(invoice_date) >= 2020 and gl_revenue_account_no like '4000%' and il.order_no is not null
    group by 
        ih.customer_id, 
        ih.bill2_name, 
        ih.bill2_city,
        ih.bill2_state,
        order_date, 
        il.order_no, 
        line_no, 
        item_id, 
        item_desc, 
        pg.product_group_desc,
        qty_requested, 
        qty_shipped, 
        unit_price, 
        extended_price,
        cogs_amount, 
        tax_amount, 
        amount_paid, 
        total_amount, 
        invoice_date, 
        ih.invoice_no, 
        gl_revenue_account_no,
        sales_location_id, 
        sl.location_name,
        branch_id, 
        bl.location_name
)"""

fwp_df = spark.read.format("jdbc") \
   .option("url", fwp_url) \
   .option("query", fwp_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()
   
mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
fwp_df.write.jdbc(url=url, table="sales.fwp_sales", mode=mode, properties=properties)


logger.info("******** END READING FWP *************")


job.commit()