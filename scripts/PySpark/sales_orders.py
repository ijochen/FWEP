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




# 1) Load PEP - Read from S3 file and write to Data Warehouse 
logger.info("******** START READING PEP *************")

# Invoice last 30 days
s3_df = spark.read.format("csv") \
   .option("header", "true") \
   .load("s3a://pep-bucket-analytics/Last30Days.csv")

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
s3_df.write.jdbc(url=url, table="sales.pep_invoice_data_INCREMENTAL", mode=mode, properties=properties)

# Customer list
s3_df_cust = spark.read.format("csv") \
   .option("header", "true") \
   .load("s3a://pep-bucket-analytics/CustType.csv")
s3_df_cust.write.jdbc(url=url, table="sales.pep_customer", mode=mode, properties=properties)



logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

logger.info("******** START READING FWP *************")


erp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
erp_query = """(
   select v.*,
        memo_amount as invoice_hdr_memo_amount
   from p21_sales_history_report_view v
   left join dbo.invoice_hdr h 
        on v.invoice_no = h.invoice_no
   where v.invoice_date >= DATEADD(day,-30, GETDATE())
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
ss_df.write.jdbc(url=url, table="sales.fwp_invoice_data_incremental", mode=mode, properties=properties)


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

query = "select sales.upsert_pep_invoices()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()


queryTwo = "select sales.upsert_fwp_invoices()"
cur = conn.cursor()
cur.execute(queryTwo)
conn.commit()
cur.close()

queryThree = "select sales.load_invoices()"
cur = conn.cursor()
cur.execute(queryThree)
conn.commit()
cur.close()

conn.close()


job.commit()