
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job


## Can't find module, the module from the external library isn't correct

#from ._pymssql import *
#from ._pymssql import __version__, __full_version__
#from pymssql import _mssql										
#import _mssql

import cython

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

logger = glueContext.get_logger()

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

## Dataframe of P21 Items 
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

# mode = "overwrite"
# url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
# properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
# p21_item_df.write.jdbc(url=url, table="p21_item_data", mode=mode, properties=properties)

## Create joined dataframe of all the Agility Items and P21 Items as a Cross Reference to Update the Quantities
item_ref_df = s3_df \
    .join(p21_item_df, \
        (s3_df.P21ITEM == p21_item_df.item_id) & \
        (s3_df.P21UOM == p21_item_df.default_selling_unit)   
    ) \
    .select( \
        s3_df["*"], \
        p21_item_df["*"]
    )

mode = "overwrite"
url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest"
properties = {"user": "ichen","password": "Qwer1234$","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
item_ref_df.write.jdbc(url=url, table="agility_p21_itemxref", mode=mode, properties=properties)    

# 3) Run SQL Job to execute Stored Proc in P21 MSSQL to create the cross ref table between Agility Product Codes and P21 Product Codes to get Inv Mast Uid to then Update the Inv_Loc Table

#conn = pymssql.connect(server = 'jdbc:sqlserver://10.0.10.18:1433;databaseName=HLSGTest', user = 'ichen', password = 'Qwer1234$', database = 'HLSGTest' )

#source_jdbc_conf = glueContext.extract_jdbc_conf('HLSG Azure Test')


# 4) Generate Exception Report and Load to S3 bucket and use Matillion to send email attachment

## Dataframe of P21 Items 
p21_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=P21Test"
p21_exc_report_query = """(
    --ITEM NOT FOUND IN P21, SCRIPT FOR EXCEPTION REPORT
        select 
            hlsg.* 
            , im.inv_mast_uid
            , im.item_id 
            , item_desc
            , im.default_selling_unit 
        from HLSGTest.dbo.staging_hlsg_inventory hlsg
        left join P21Test.dbo.inv_mast im on hlsg.P21ITEM = im.item_id
        where item_id is null
        union 

    --ITEMS WHERE UOM ARE ALL MISMATCHING
        select 
            hlsg.* 
            , im.inv_mast_uid
            , im.item_id 
            , item_desc
            , im.default_selling_unit 
        from HLSGTest.dbo.staging_hlsg_inventory hlsg
        left join P21Test.dbo.inv_mast im on hlsg.P21ITEM = im.item_id
        where item_id is null or AGILITYUOM <> P21UOM or P21UOM <> im.default_selling_unit
)"""

p21_exc_report_df = spark.read.format("jdbc") \
   .option("url", p21_url) \
   .option("query", p21_exc_report_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()


p21_exc_report_df.coalesce(1)\
    .write.format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("s3a://srs-bucket/reports")
 
    
import boto3

s3 = boto3.resource('s3')

source_bucket = 'srs-bucket'
srcPrefix = 'srs-bucket/reports/'



# try:
#     client = boto3.client('s3')
    
#     ## Get a list of files with prefix (we know there will be only one file)
    
#     response = client.list_objects(
#         Bucket = source_bucket,
#         Prefix = srcPrefix
#     )
#     name = response["Contents"][0]["Key"]

# print(name)

#     ## Store Target File File Prefix, this is the new name of the file
#     target_source = {'Bucket': source_bucket, 'Key': name}
#     print(target_source)
    
#     target_key = srcPrefix + 'HLSG_P21_B2B_Exception_Report'
    
#     print(target_key)
    
#     ### Now Copy the file with New Name
#     client.copy(CopySource=target_source, Bucket=source_bucket, Key=target_key)
    
#     ### Delete the old file
#     client.delete_object(Bucket=source_bucket, Key=name)
    
# except Exception as e:
#         ## do nothing
#         print('error occured')


# 4) Merge tables together in a stored proc




job.commit()