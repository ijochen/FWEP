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


erp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
erp_query = """(
    select distinct 
        il.location_id, 
        b.branch_description, 
        im.default_sales_discount_group item_group, 
        im.item_id, 
        im.item_desc, 
        avg(fi.cost) avg_fifo_cost,
        /*sum(qty_on_hand)*/ qty_on_hand, 
        /*sum(qty_allocated)*/ qty_allocated, 
        --(sum(qty_on_hand) - sum(qty_allocated)) qty_available,
        (qty_on_hand - qty_allocated) qty_available
        --cast(getdate() as date) trans_date
    from CommerceCenter.dbo.inv_loc il
    left join CommerceCenter.dbo.branch b on il.location_id = b.branch_id
    left join CommerceCenter.dbo.inv_mast im on il.inv_mast_uid = im.inv_mast_uid
    left join CommerceCenter.dbo.fifo_layers fi on im.inv_mast_uid = fi.inv_mast_uid
    where qty_on_hand > 0 or qty_allocated > 0 
    group by 
        il.location_id, 
        b.branch_description, 
        im.default_sales_discount_group, 
        im.item_id, 
        im.item_desc
)"""

ss_df = spark.read.format("jdbc") \
   .option("url", erp_url) \
   .option("query", erp_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
ss_df.write.jdbc(url=url, table="warehouse.inventory_position_incremental", mode=mode, properties=properties)


# 3) Merge tables together in a stored proc
import pg8000

conn = pg8000.connect(
    database='analytics',
    user='postgres',
    password='kHSmwnXWrG^L3N$V2PXPpY22*47',
    host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
    port=5432
)

query = "select warehouse.upsert_fwp_inventory_position()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()

conn.close()


job.commit()