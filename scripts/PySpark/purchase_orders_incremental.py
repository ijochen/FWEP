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
        ph.PO_TYPE,
        ph.PO_NUM,
        pl.SEQ_NUM,
        ph.VEND_NUM,
        v.VEND_DESC,
        case when b.BUYER_DESC is null then ph.BUYER_NUM else b.BUYER_DESC end buyer,
        pl.PROD_NUM,
        p.PROD_DESC1,
        pl.ORD_QTY,
        pl.REC_QTY,
        pl.GRS_COST,
        pl.EXT_AMT,
        ph.CENTRAL_WHSE_NUM,
        pl.WHSE_NUM,
        ph.PO_DATE,
        ph.REC_DATE,
        pl.DEL_DATE
    from Prelude.dbo.PO_HISTORY_LINE_IJO pl
    left join Prelude.dbo.PO_HISTORY_IJO ph on substring(pl.ID,1,13) = ph.ID
    left join Prelude.dbo.SSProduct p on pl.PROD_NUM = p.PROD_NUM
    left join Prelude.dbo.ssVEND_NF v on ph.VEND_NUM = v.VEND_NUM and ph.CO_NUM = v.CO_NUM
    left join Prelude.dbo.BUYER_NF b on ph.BUYER_NUM = b.BUYER_NUM
    where pl.PROD_NUM not in ('C','CSB') and ph.PO_DATE >= dateadd(day,-30,getdate())
    group by ph.PO_TYPE, ph.PO_NUM, pl.SEQ_NUM,
        ph.VEND_NUM, v.VEND_DESC, ph.BUYER_NUM, b.BUYER_DESC,
        pl.PROD_NUM, p.PROD_DESC1,
        pl.ORD_QTY, pl.REC_QTY,
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
pep_df.write.jdbc(url=url, table="procurement.pep_purchase_orders_incremental", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

logger.info("******** START READING FWP *************")


fwp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
fwp_query = """(
    select distinct 
        'FWP' as company,
        ph.po_type, 
        cast(ph.po_no as varchar) po_no,
        pl.line_no,
        ph.vendor_id, 
        v.vendor_name,
        replace(pl.created_by, 'FWPNET\', '') buyer,
        i.item_id,
        pl.item_description,
        cast(pl.qty_ordered as varchar) qty_ordered,
        cast(pl.qty_received as varchar) qty_received,
        pl.unit_price,
        (pl.unit_price * pl.qty_ordered) ext_price,
        ph.location_id,
        ph.branch_id,
        cast(ph.order_date as datetime) order_date,
        cast(ph.receipt_date as datetime) receipt_date,
        cast(pl.date_due as datetime) promised_del_date
    from CommerceCenter.dbo.po_hdr ph
    left join CommerceCenter.dbo.po_line pl on ph.po_no = pl.po_no
    left join CommerceCenter.dbo.vendor v on ph.vendor_id = v.vendor_id
    left join CommerceCenter.dbo.inv_mast i on pl.inv_mast_uid = i.inv_mast_uid
    where ph.order_date >= dateadd(day,-30,getdate())
    group by ph.po_type, ph.po_no, pl.line_no, 
        ph.vendor_id, v.vendor_name, pl.created_by, 
        i.item_id, pl.item_description, 
        pl.qty_ordered, pl.qty_received, 
        pl.unit_price, 
        ph.location_id, ph.branch_id, 
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
fwp_df.write.jdbc(url=url, table="procurement.fwp_purchase_orders_incremental", mode=mode, properties=properties)


logger.info("******** END READING FWP *************")


# 3) Merge tables together in a stored proc
import pg8000

conn = pg8000.connect(
    database='analytics',
    user='postgres',
    password='kHSmwnXWrG^L3N$V2PXPpY22*47',
    host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
    port=5432
)

query = "select procurement.upsert_pep_purchases()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()


queryTwo = "select procurement.upsert_fwp_purchases()"
cur = conn.cursor()
cur.execute(queryTwo)
conn.commit()
cur.close()

queryThree = "select procurement.load_purchases()"
cur = conn.cursor()
cur.execute(queryThree)
conn.commit()
cur.close()

conn.close()


job.commit()