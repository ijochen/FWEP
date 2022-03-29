import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from awsglue.job import Job
#import _mssql
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

mode = "overwrite"
url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
s3_df.write.jdbc(url=url, table="staging_hlsg_inventory", mode=mode, properties=properties)


# 2) Fetch Item Data from P21 

p21_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=P21Test"
p21_query = """(
    select distinct 
        im.inv_mast_uid
        , im.item_id 
        , item_desc
        , im.default_selling_unit 
    from P21Test.dbo.inv_mast im
)"""

p21_item_df = spark.read.format("jdbc") \
   .option("url", p21_url) \
   .option("query", p21_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

mode = "overwrite"
url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
p21_item_df.write.jdbc(url=url, table="p21_item_data", mode=mode, properties=properties)


item_ref_df = s3_df \
    .join(p21_item_df, \
        (s3_df.P21ITEM == p21_item_df.item_id) & \
        (s3_df.P21UOM == p21_item_df.default_selling_unit)\    
    .select( \
        s3_df["*"], \
        p21_item_df["*"], \
    )

mode = "overwrite"
url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
item_ref_df.write.jdbc(url=url, table="agility_p21_itemxref", mode=mode, properties=properties)    

# 2) Call Stored Proc in P21 MSSQL to create the cross ref table between Agility Product Codes and P21 Product Codes to get Inv Mast Uid to then Update the Inv_Loc Table




# 4) Merge tables together in a stored proc

# erp_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
# erp_query = """(
#     select distinct hi.*
#         , im.inv_mast_uid
#         , im.item_id
#         , item_desc
#         , im.default_selling_unit 
#     from HLSGTest.dbo.staging_hlsg_inventory hi
#     join P21Test.dbo.inv_mast im on hi.pepproductcd = im.item_id and hi.PEPUOM = im.default_selling_unit
#     where AGILITYUOM = PEPUOM
# )"""

# ss_df = spark.read.format("jdbc") \
#    .option("url", erp_url) \
#    .option("query", erp_query) \
#    .option("user", "ichen") \
#    .option("password", "Qwer1234$") \
#    .load()

# mode = "overwrite"
# url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
# properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
# s3_df.write.jdbc(url=url, table="agility_p21_itemxref", mode=mode, properties=properties)


# import pg8000

# conn = pg8000.connect(
#     database='HLSGTest',
#     user='ichen',
#     password='Qwer1234$',
#     host='10.0.10.18',
#     port=1433
# )

# query = "exec hlsg_p21_test()"
# cur = conn.cursor()
# cur.execute(query)
# conn.commit()
# cur.close()



# glue_connection_name = 'HLSG Azure Test'
# database_name = 'HLSGTest'
# stored_proc = 'exec hlsg_p21_test()'

# logger = glueContext.get_logger()
    
# logger.info('Getting details for connection ' + glue_connection_name)
# source_jdbc_conf = glueContext.extract_jdbc_conf(glue_connection_name)
    
# from py4j.java_gateway import java_import
# java_import(sc._gateway.jvm,"java.sql.Connection")
# java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
# java_import(sc._gateway.jvm,"java.sql.DriverManager")
# java_import(sc._gateway.jvm,"java.sql.SQLException")
    
# conn = sc._gateway.jvm.DriverManager.getConnection(source_jdbc_conf.get('url') + '/' + database_name, source_jdbc_conf.get('user'), source_jdbc_conf.get('password'))
# logger.info('Connected to ' + conn.getMetaData().getDatabaseProductName() + ', ' + source_jdbc_conf.get('url') + '/' + database_name)
    
# stmt = conn.createStatement();
# rs = stmt.executeUpdate('call ' + stored_proc);
    
# logger.info("Finished")



# # dw-poc-dev spark test
# source_jdbc_conf = glueContext.extract_jdbc_conf('jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest')

# from py4j.java_gateway import java_import
# java_import(sc._gateway.jvm,"java.sql.Connection")
# java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
# java_import(sc._gateway.jvm,"java.sql.DriverManager")
# java_import(sc._gateway.jvm,"java.sql.SQLException")

# conn = sc._gateway.jvm.DriverManager.getConnection(source_jdbc_conf.get('url'), source_jdbc_conf.get('user'), source_jdbc_conf.get('password'))

# print(conn.getMetaData().getDatabaseProductName())

# # call stored procedure, in this case I call sp_start_job
# cstmt = conn.prepareCall("{call master.dbo.hlsg_p21_test(?)}");
# cstmt.setString("job_name", "testjob");
# results = cstmt.execute();

# conn.close()



# erp_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
# erp_query = """(
#     exec hlsg_p21_test
# )"""




job.commit()