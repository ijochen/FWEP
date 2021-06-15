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


# 1) Load FWP Inter Company Transfer Mapping Data - Read from S3 file and write to Data Warehouse 
logger.info("******** START READING FWP COLLABORATIVE DATA*************")

# Inter Company Transfer Mapping Data
s3_df_trans = spark.read.format("csv") \
   .option("header", "true") \
   .load("s3a://fwp-bucket/InterCompanyTransferMappingData.csv")

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
s3_df_trans.write.jdbc(url=url, table="warehouse.intercompany_transfer_mapping_data", mode=mode, properties=properties)

# Regional Mgmt Mapping
s3_df_mgmt = spark.read.format("csv") \
   .option("header", "true") \
   .load("s3a://fwp-bucket/RegionalMgmtMapping.csv")
s3_df_mgmt.write.jdbc(url=url, table="warehouse.regional_mgmt_mapping_data", mode=mode, properties=properties)



logger.info("******** END READING FWP COLLABORATIVE DATA*************")


job.commit()