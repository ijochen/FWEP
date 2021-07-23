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
        t.location_id, 
        l.location_name, 
        t.bin, 
        i.item_id,
        i.item_desc,
		sum(t.quantity) over (partition by i.item_id, t.bin, t.location_id) quantity_on_hand,
		max(t.date_created) over(partition by t.location_id, t.bin, i.item_id) date_received
    from CommerceCenter.dbo.inv_tran_bin_detail t 
    left join CommerceCenter.dbo.inv_mast i
        on t.inv_mast_uid = i.inv_mast_uid
    left join CommerceCenter.dbo.location l 
        on t.location_id = l.location_id
    left join CommerceCenter.dbo.inventory_receipts_hdr irl 
        on t.transaction_number = irl.receipt_number
    where bin like '%REC%' 
    group by t.location_id, 
        t.location_id, 
        l.location_name, 
        t.bin, 
        i.item_id,
        i.item_desc,
		t.quantity,
		t.date_created
	having sum(t.quantity) > '0'
	order by location_id, bin, item_id
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
ss_df.write.jdbc(url=url, table="warehouse.receiving_fwp", mode=mode, properties=properties)


# 3) Merge tables together in a stored proc
# import pg8000

# conn = pg8000.connect(
#     database='analytics',
#     user='postgres',
#     password='kHSmwnXWrG^L3N$V2PXPpY22*47',
#     host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
#     port=5432
# )

# query = "select warehouse.upsert_inv_transactions()"
# cur = conn.cursor()
# cur.execute(query)
# conn.commit()
# cur.close()

# queryTwo = "select warehouse.load_receiving()"
# cur = conn.cursor()
# cur.execute(queryTwo)
# conn.commit()
# cur.close()

# conn.close()


job.commit()