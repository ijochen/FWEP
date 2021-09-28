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

# PEP POs from 2019 to YTD
pep_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=Prelude"
pep_query = """(
    select distinct --top(100)
        'PEP' as COMPANY,
        ph.PO_TYPE,
        ph.PO_NUM,
        pl.SEQ_NUM,
        ph.VEND_NUM,
        v.VEND_DESC,
        ph.BUYER_NUM,
        case when u.USER_DESC is null then ph.BUYER_NUM else u.USER_DESC end BUYER_NAME,
        pl.PROD_NUM,
        concat(p.PROD_DESC1,' ',p.PROD_DESC2) PROD_DESC,
        ca.PLINE_DESC PROD_CATEGORY,
        pl.VPROD_NUM,
        pl.UN_MEAS,
        ORD_QTY,
        REC_QTY,
        pl.GRS_COST,
        pl.EXT_AMT,
        ph.CENTRAL_WHSE_NUM,
        case	 
            when ph.CENTRAL_WHSE_NUM = '01' then 'PEP - ANAHEIM CA'
            when ph.CENTRAL_WHSE_NUM = '02' then 'PEP - INDIO CA'
            when ph.CENTRAL_WHSE_NUM = '03' then 'PEP - EL CAJON CA'
            when ph.CENTRAL_WHSE_NUM = '04' then 'PEP - MURRIETA CA'
            when ph.CENTRAL_WHSE_NUM = '05' then 'PEP - LIVERMORE CA'
            when ph.CENTRAL_WHSE_NUM = '06' then 'PEP - ONTARIO CA'
            when ph.CENTRAL_WHSE_NUM = '07' then 'PEP - SAN DIMAS CA'
            when ph.CENTRAL_WHSE_NUM = '08' then 'PEP - CATHEDRAL CITY CA'
            when ph.CENTRAL_WHSE_NUM = '09' then 'PEP - SAN FERNANDO CA'
            when ph.CENTRAL_WHSE_NUM = '10' then 'PEP - VISALIA CA'
            when ph.CENTRAL_WHSE_NUM = '11' then 'PEP - SAN ANTONIO TX'
            when ph.CENTRAL_WHSE_NUM = '12' then 'PEP - VISTA CA'
            when ph.CENTRAL_WHSE_NUM = '13' then 'PEP - AUSTIN TX'
            when ph.CENTRAL_WHSE_NUM = '14' then 'PEP - PALM SPRINGS CA'
            when ph.CENTRAL_WHSE_NUM = '15' then 'PEP - CORONA CA'
            when ph.CENTRAL_WHSE_NUM = '16' then 'PEP - BAKERSFIELD CA'
            when ph.CENTRAL_WHSE_NUM = '17' then 'PEP - HOUSTON TX'
            when ph.CENTRAL_WHSE_NUM = '18' then 'PEP - LAKE FOREST CA'
            when ph.CENTRAL_WHSE_NUM = '19' then 'PEP - MOORPARK CA'
            when ph.CENTRAL_WHSE_NUM = '20' then 'PEP - NORTH AUSTIN TX'
            when ph.CENTRAL_WHSE_NUM = '21' then 'PEP - DUARTE CA'
            when ph.CENTRAL_WHSE_NUM = '22' then 'PEP - YUCAIPA CA'
            when ph.CENTRAL_WHSE_NUM = '23' then 'PEP - RIVERSIDE CA'
            when ph.CENTRAL_WHSE_NUM = '24' then 'PEP - LONG BEACH CA'
            when ph.CENTRAL_WHSE_NUM = '25' then 'PEP - PALM DESERT CA'
            when ph.CENTRAL_WHSE_NUM = '26' then 'PEP - LOS ANGELES CA'
            when ph.CENTRAL_WHSE_NUM = '27' then 'PEP - TEMPE AZ'
            when ph.CENTRAL_WHSE_NUM = '28' then 'PEP - PHOENIX AZ'
            when ph.CENTRAL_WHSE_NUM = '29' then 'PEP - SANTA ANA CA'
            when ph.CENTRAL_WHSE_NUM = '30' then 'PEP - EL CENTRO CA'
            when ph.CENTRAL_WHSE_NUM = '98' then 'PEP - CORPORATE WAREHOUSE'
            when ph.CENTRAL_WHSE_NUM = '99' then 'PEP - CENTRAL SHIPPING WAREHOUSE'
                else ph.CENTRAL_WHSE_NUM end CENTRAL_WHSE_NAME,
        pl.WHSE_NUM,
        case	 
            when pl.WHSE_NUM = '01' then 'PEP - ANAHEIM CA'
            when pl.WHSE_NUM = '02' then 'PEP - INDIO CA'
            when pl.WHSE_NUM = '03' then 'PEP - EL CAJON CA'
            when pl.WHSE_NUM = '04' then 'PEP - MURRIETA CA'
            when pl.WHSE_NUM = '05' then 'PEP - LIVERMORE CA'
            when pl.WHSE_NUM = '06' then 'PEP - ONTARIO CA'
            when pl.WHSE_NUM = '07' then 'PEP - SAN DIMAS CA'
            when pl.WHSE_NUM = '08' then 'PEP - CATHEDRAL CITY CA'
            when pl.WHSE_NUM = '09' then 'PEP - SAN FERNANDO CA'
            when pl.WHSE_NUM = '10' then 'PEP - VISALIA CA'
            when pl.WHSE_NUM = '11' then 'PEP - SAN ANTONIO TX'
            when pl.WHSE_NUM = '12' then 'PEP - VISTA CA'
            when pl.WHSE_NUM = '13' then 'PEP - AUSTIN TX'
            when pl.WHSE_NUM = '14' then 'PEP - PALM SPRINGS CA'
            when pl.WHSE_NUM = '15' then 'PEP - CORONA CA'
            when pl.WHSE_NUM = '16' then 'PEP - BAKERSFIELD CA'
            when pl.WHSE_NUM = '17' then 'PEP - HOUSTON TX'
            when pl.WHSE_NUM = '18' then 'PEP - LAKE FOREST CA'
            when pl.WHSE_NUM = '19' then 'PEP - MOORPARK CA'
            when pl.WHSE_NUM = '20' then 'PEP - NORTH AUSTIN TX'
            when pl.WHSE_NUM = '21' then 'PEP - DUARTE CA'
            when pl.WHSE_NUM = '22' then 'PEP - YUCAIPA CA'
            when pl.WHSE_NUM = '23' then 'PEP - RIVERSIDE CA'
            when pl.WHSE_NUM = '24' then 'PEP - LONG BEACH CA'
            when pl.WHSE_NUM = '25' then 'PEP - PALM DESERT CA'
            when pl.WHSE_NUM = '26' then 'PEP - LOS ANGELES CA'
            when pl.WHSE_NUM = '27' then 'PEP - TEMPE AZ'
            when pl.WHSE_NUM = '28' then 'PEP - PHOENIX AZ'
            when pl.WHSE_NUM = '29' then 'PEP - SANTA ANA CA'
            when pl.WHSE_NUM = '30' then 'PEP - EL CENTRO CA'
            when pl.WHSE_NUM = '98' then 'PEP - CORPORATE WAREHOUSE'
            when pl.WHSE_NUM = '99' then 'PEP - CENTRAL SHIPPING WAREHOUSE'
                else pl.WHSE_NUM end WHSE_NAME,
        cast(ph.PO_DATE as date) PO_DATE,
        cast(ph.REC_DATE as date) REC_DATE,
        cast(pl.DEL_DATE as date) DEL_DATE
    from Prelude.dbo.PO_HISTORY_LINE_IJO pl
    left join Prelude.dbo.PO_HISTORY_IJO ph on left(PL.ID, charindex('!',PL.ID)-1) = ph.ID
    left join Prelude.dbo.PRODUCT_IJO p on pl.PROD_NUM = p.PROD_NUM
    left join Prelude.dbo.CATEGORY_IJO ca on p.PLINE_NUM = ca.PLINE_NUM
    left join Prelude.dbo.VEND_IJO v on ph.VEND_NUM = v.VEND_NUM and ph.CO_NUM = v.CO_NUM
    left join Prelude.dbo.BUYER_NF b on ph.BUYER_NUM = b.BUYER_NUM
    left join Prelude.dbo.USER_ID_NF U on ph.BUYER_NUM = u.USER_NUM
    where /*pl.PROD_NUM not in ('C','CSB','CS','CI','CN','CP','MN')  and */p.CO_NUM = '001' and ca.CO_NUM = '001' and year(ph.PO_DATE) >= 2019
    group by ph.PO_TYPE, ph.PO_NUM, pl.SEQ_NUM,
        ph.VEND_NUM, v.VEND_DESC, ph.BUYER_NUM, b.BUYER_DESC, u.USER_DESC,
        pl.PROD_NUM, p.PROD_DESC1, p.PROD_DESC2, ca.PLINE_DESC, pl.VPROD_NUM,
        pl.UN_MEAS,	pl.ORD_QTY, pl.REC_QTY,
        pl.GRS_COST, pl.EXT_AMT,
        ph.CENTRAL_WHSE_NUM, pl.WHSE_NUM,
        ph.PO_DATE, ph.REC_DATE, pl.DEL_DATE
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
pep_df.write.jdbc(url=url, table="procurement.pep_purchase_orders_new", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

logger.info("******** START READING FWP *************")

# FWP POs from 2019 to YTD
fwp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
fwp_query = """(
    select distinct --top(100)
        'FWP' as company,
        ph.po_type,         
        cast(ph.po_no as varchar) po_no,
        cast(pl.line_no as varchar) line_no,
        cast(ph.vendor_id as varchar) vendor_id, 
        v.vendor_name,
        case when pl.created_by like 'FWPNET\%' then replace(pl.created_by,'FWPNET\\','') 
            when pl.created_by like 'poolelectrical\%' then replace(pl.created_by,'poolelectrical\\','') 
            end buyer,
        u.name buyer_name,
        i.item_id prod_num,
        pl.item_description prod_desc,
        i.default_sales_discount_group prod_group,
        ins.supplier_part_no,
        pl.unit_of_measure,
        cast(pl.qty_ordered as varchar) qty_ordered,
        cast(pl.qty_received as varchar) qty_received,
        pl.unit_price,
        (pl.unit_price * pl.qty_ordered) ext_price,
        cast(ph.location_id as varchar) location_id,
        b1.branch_description location_name,
        ph.branch_id,
        b2.branch_description branch_name,
        cast(ph.order_date as date) order_date,
        cast(ph.receipt_date as date) receipt_date,
        cast(pl.date_due as date) promised_del_date
    from CommerceCenter.dbo.po_line pl
    left join CommerceCenter.dbo.po_hdr ph on pl.po_no = ph.po_no
    left join CommerceCenter.dbo.vendor v on ph.vendor_id = v.vendor_id
    left join CommerceCenter.dbo.inv_mast i on pl.inv_mast_uid = i.inv_mast_uid
    left join CommerceCenter.dbo.inventory_supplier ins on pl.inv_mast_uid = ins.inv_mast_uid and ph.vendor_id = ins.supplier_id
    left join CommerceCenter.dbo.branch b1 on ph.location_id = b1.branch_id
    left join CommerceCenter.dbo.branch b2 on ph.branch_id = b2.branch_id
    left join CommerceCenter.dbo.users u on replace(pl.created_by,'FWPNET\\','') = u.id or replace(pl.created_by,'poolelectrical\\','') = u.id
    where year(ph.order_date) >= 2019 
    group by ph.po_type, ph.po_no, pl.line_no, 
        ph.vendor_id, v.vendor_name, pl.created_by, u.name,
        i.item_id, pl.item_description, i.default_sales_discount_group, ins.supplier_part_no, 
        pl.unit_of_measure,	pl.qty_ordered, pl.qty_received, pl.unit_price, 
        ph.location_id, ph.branch_id, b1.branch_description, b2.branch_description,
        ph.order_date, ph.receipt_date, pl.date_due
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
fwp_df.write.jdbc(url=url, table="procurement.fwp_purchase_orders_new", mode=mode, properties=properties)


logger.info("******** END READING FWP *************")


job.commit()