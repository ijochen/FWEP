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


erp_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=P21Test"
erp_query = """(
    select 
        faoip.location_id, 
        b.branch_description, 
        faoip.item_group, 
        faoip.item_id, 
        faoip.item_desc, 
        ins.supplier_part_no,
        im.purchase_pricing_unit unit_of_measure, 
        round(avg(fi.cost),2) avg_fifo_cost,
        (faoip.qty_available + 5) qty_on_hand, 
        '5' qty_allocated, 
        faoip.qty_available, 
        '2021-09-01' trans_date
    from Prelude.dbo.FWP_APS_Sep2020_Inv_Pos faoip
    left join P21Test.dbo.inv_mast im on faoip.item_id = im.item_id
    left join P21Test.dbo.branch b on faoip.location_id = b.branch_id
    left join P21Test.dbo.inv_loc il on faoip.location_id = il.location_id and im.inv_mast_uid = il.inv_mast_uid
    left join P21Test.dbo.inventory_supplier ins on im.inv_mast_uid = ins.inv_mast_uid and il.primary_supplier_id = ins.supplier_id
    left join P21Test.dbo.fifo_layers fi on im.inv_mast_uid = fi.inv_mast_uid
    group by faoip.location_id, 
        b.branch_description, 
        faoip.item_group, 
        faoip.item_id, 
        faoip.item_desc, 
        ins.supplier_part_no,  
        im.purchase_pricing_unit, 
        faoip.qty_available
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
ss_df.write.jdbc(url=url, table="warehouse.inventory_position_oct2020", mode=mode, properties=properties)



job.commit()