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


erp_url = "jdbc:sqlserver://172.31.1.227:1433;databaseName=CommerceCenter"
erp_query = """(
   select 
      ol.order_no ol_order_no,
      ol.line_no ol_line_no,

      oh.customer_id oh_customer_id ,
      ship2_name customer_name, 
      location_id,
      source_location_id shipping_location,
        
		front_counter,
		ol.disposition ol_disposition,
		oh.projected_order oh_projected_orders,
		oh.completed oh_completed,
		oh.will_call oh_will_call,

      oh.date_created oh_date_created, 
      ol.date_created ol_date_created
        --,* 

      from FWP_SQL.CommerceCenter.dbo.oe_line ol
      left join FWP_SQL.CommerceCenter.dbo.oe_hdr oh on ol.order_no = oh.order_no
      where ol.order_no in (
		   select
			distinct key1_value 
		   from dbo.audit_trail WITH (NOLOCK) 
		   where key1_cd = 'order_no' 
		   --and date_created >= DATEADD(day,-30, GETDATE()) 
      )
)"""

erp_df = spark.read.format("jdbc") \
   .option("url", erp_url) \
   .option("query", erp_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
erp_df.write.jdbc(url=url, table="warehouse.front_counter_fwp", mode=mode, properties=properties)


# 3) Merge tables together in a stored proc
# import pg8000

# conn = pg8000.connect(
#     database='analytics',
#     user='postgres',
#     password='kHSmwnXWrG^L3N$V2PXPpY22*47',
#     host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
#     port=5432
# )

# query = "select warehouse.upsert_front_counter()"
# cur = conn.cursor()
# cur.execute(query)
# conn.commit()
# cur.close()

# conn.close()


job.commit()