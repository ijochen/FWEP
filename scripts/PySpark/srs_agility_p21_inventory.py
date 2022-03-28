import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
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



# 1) Load Inventory Data from Agility into P21 // Read from S3 file and write to P21 Production Database // Pulled Snowflake VW_ITEMAVAILABILITY of [plattesql].[dbo].[qty_total]

s3_df = spark.read.format("csv") \
   .option("header", "true") \
    .load("s3a://srs-bucket/HLSG_Inventory.csv")


# erp_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
# erp_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# s3_df.select("*").write.format("jdbc")\
#     .mode("overwrite") \
#     .option("url", erp_url)\
#     .option("dbtable","dbo.staging_hlsg_inventory") \
#     .option("user", "ichen") \
#     .option("password", "Qwer1234$") \
#     .save()

mode = "overwrite"
url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
s3_df.write.jdbc(url=url, table="staging_hlsg_inventory", mode=mode, properties=properties)


# 4) Merge tables together in a stored proc


# dw-poc-dev spark test
source_jdbc_conf = glueContext.extract_jdbc_conf('jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest')

from py4j.java_gateway import java_import
java_import(sc._gateway.jvm,"java.sql.Connection")
java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
java_import(sc._gateway.jvm,"java.sql.DriverManager")
java_import(sc._gateway.jvm,"java.sql.SQLException")

conn = sc._gateway.jvm.DriverManager.getConnection(source_jdbc_conf.get('url'), source_jdbc_conf.get('user'), source_jdbc_conf.get('password'))

print(conn.getMetaData().getDatabaseProductName())

# call stored procedure, in this case I call sp_start_job
cstmt = conn.prepareCall("{call master.dbo.hlsg_p21_test(?)}");
cstmt.setString("job_name", "testjob");
results = cstmt.execute();

conn.close()



job.commit()