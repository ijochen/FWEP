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
   select 
        cast(trh.transfer_no as varchar) trh_transfer_no,
        transfer_date, 
        unit_quantity, 
        qty_to_transfer, 
        qty_transferred,
        avg(fi.cost) avg_fifo_cost,
        item_id, 
        item_desc,
        cast(from_location_id as varchar) from_location_id, 
        flo.location_name from_location, 
        cast(to_location_id as varchar) to_location_id, 
        tlo.location_name to_location, 
        planned_recpt_date, 
        trh.created_by hdr_created_by, 
        trl.created_by line_created_by
    from CommerceCenter.dbo.transfer_hdr trh 
    join CommerceCenter.dbo.transfer_line trl on trh.transfer_no = trl.transfer_no
    left join CommerceCenter.dbo.location flo on trh.from_location_id = flo.location_id 
    left join CommerceCenter.dbo.location tlo on trh.to_location_id = tlo.location_id
    left join CommerceCenter.dbo.inv_mast im on trl.inv_mast_uid = im.inv_mast_uid
    left join CommerceCenter.dbo.fifo_layers fi on im.inv_mast_uid = fi.inv_mast_uid
    where year(transfer_date) >= '2020'
    group by 
        trh.transfer_no,
        transfer_date, 
        unit_quantity, 
        trl.qty_to_transfer, 
        trl.qty_transferred,
        item_id, item_desc,
        from_location_id, 
        flo.location_name, 
        to_location_id, 
        tlo.location_name, 
        planned_recpt_date, 
        trh.created_by, 
        trl.created_by
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
ss_df.write.jdbc(url=url, table="warehouse.inv_transfers", mode=mode, properties=properties)


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

# conn.close()


job.commit()