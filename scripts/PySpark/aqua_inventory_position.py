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

# 1) Load FWP - Read from Tampa On-Prem Sql Server and Write to RDS PostgreSQL Data Warehouse

erp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
erp_query = """(
  select distinct 
        'P21' erp,
        il.location_id, 
		l.location_name,
        im.default_sales_discount_group item_group, 
        im.item_id, 
        im.item_desc, 
        ins.supplier_part_no,
        im.purchase_pricing_unit unit_of_measure, 
        round(avg(fi.cost),2) avg_fifo_cost,
        qty_on_hand, 
        qty_allocated, 
        (qty_on_hand - qty_allocated) qty_available,
		dateadd(dd, 1, eomonth(getdate(), -1)) trans_date
    from CommerceCenter.dbo.inv_loc il
	left join CommerceCenter.dbo.location l on il.location_id = l.location_id
    left join CommerceCenter.dbo.inv_mast im on il.inv_mast_uid = im.inv_mast_uid
    left join CommerceCenter.dbo.inventory_supplier ins on il.inv_mast_uid = ins.inv_mast_uid and il.primary_supplier_id = ins.supplier_id
    left join CommerceCenter.dbo.fifo_layers fi on il.inv_mast_uid = fi.inv_mast_uid
    where (qty_on_hand > 0 or qty_allocated > 0)  --and ins.supplier_part_no is not null
    group by 
        il.location_id, 
		l.location_name,
        im.default_sales_discount_group, 
        im.item_id, 
        im.item_desc, 
        ins.supplier_part_no,
        im.purchase_pricing_unit,
        qty_on_hand,
        qty_allocated
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
ss_df.write.jdbc(url=url, table="warehouse.fweps_inventory_position_incremental", mode=mode, properties=properties)

# 2) Load PEP - Read from P21 Azure Test Server Prelude Database and Write to RDS PostgreSQL Data Warehouse

erp_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=Prelude"
erp_query = """(
    SELECT DISTINCT
        'PRELUDE' ERP
        , WS.WHSE_NUM
        , WA.WHSE_DESC
        , CA.PLINE_DESC PROD_CATEGORY
        , WS.PROD_NUM
        , CONCAT(P.PROD_DESC1,' ',P.PROD_DESC2) PROD_DESC
        , V.VEND_NUM
        , ISNULL(PV.VPROD_NUM,'') VEND_PROD_NUM
        , WS.PHY_UOM_CNT
        , WS.AVG_COST
        , WS.ONHAND QTY_ONHAND
        --, PP.QPSO_TOT COMMIT_SO_PICK
        --, PP.QPST_TOT COMMIT_TRANS_PICK
        , ISNULL((CAST(PP.QPSO_TOT AS FLOAT) + CAST(PP.QPST_TOT AS FLOAT)),0) QTY_ALLOCATED
        , CAST(WS.ONHAND AS FLOAT) - ISNULL((CAST(PP.QPSO_TOT AS FLOAT) + CAST(PP.QPST_TOT AS FLOAT)),0) QTY_AVAILABLE
    FROM PRELUDE.DBO.WHSE_STAT_IJO WS
    LEFT JOIN PRELUDE.DBO.PRODUCT_IJO P ON WS.CO_NUM = P.CO_NUM AND WS.PROD_NUM = P.PROD_NUM
    LEFT JOIN PRELUDE.DBO.CATEGORY_IJO CA ON WS.CO_NUM = CA.CO_NUM AND P.PLINE_NUM = CA.PLINE_NUM
    LEFT JOIN PRELUDE.DBO.WHSE_ADDR_IJO WA ON WS.CO_NUM = LEFT(WA.ID,3) AND WS.WHSE_NUM = WA.WHSE_NUM
    LEFT JOIN PRELUDE.DBO.PROD_POINTER_IJO PP ON WS.CO_NUM = PP.CO_NUM AND WS.WHSE_NUM = PP.WHSE_NUM AND WS.PROD_NUM = PP.PROD_NUM
    LEFT JOIN (
                SELECT 
                    WHSE_NUM
                    , PROD_NUM
                    , CASE 
                        WHEN WS.FST_ACQ_VNM_PY = '' AND WS.FST_ACQ_VNM_CY = '' THEN WS.FST_ACQ_VNUM
                        WHEN WS.FST_ACQ_VNM_PY = '' THEN WS.FST_ACQ_VNM_CY
                        ELSE WS.FST_ACQ_VNM_PY END VEND_NUM
                FROM PRELUDE.DBO.WHSE_STAT_IJO WS
                WHERE WS.CO_NUM = '001' 
                    AND CAST(WS.ONHAND AS FLOAT) <> '0'
                ) V ON WS.WHSE_NUM = V.WHSE_NUM AND WS.PROD_NUM = V.PROD_NUM
    LEFT JOIN PRELUDE.DBO.PRD_VEND_IJO PV ON WS.CO_NUM = PV.CO_NUM AND V.VEND_NUM = PV.VEND_NUM AND WS.WHSE_NUM = PV.WHSE_NUM AND WS.PROD_NUM = PV.PROD_NUM
    WHERE CAST(WS.ONHAND AS FLOAT) <> '0' 
        AND WS.CO_NUM = '001'	
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
ss_df.write.jdbc(url=url, table="warehouse.pep_inventory_position_incremental", mode=mode, properties=properties)


# 3) Merge tables together in a stored proc

import pg8000

conn = pg8000.connect(
    database='analytics',
    user='postgres',
    password='kHSmwnXWrG^L3N$V2PXPpY22*47',
    host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
    port=5432
)

query = "select warehouse.upsert_fweps_inventory_position_interim()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()

queryTwo = "select warehouse.load_fweps_inventory_position()"
cur = conn.cursor()
cur.execute(queryTwo)
conn.commit()
cur.close()

conn.close()


job.commit()