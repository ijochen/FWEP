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




# 1) Load PEP - Read from FWP Azure Test Server Prelude Database and write to Data Warehouse 
logger.info("******** START READING PEP *************")

# Invoice last 30 days
pep_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=Prelude"
pep_query = """(
    select distinct OH.ORD_NUM, OH.INV_NUM, OH.INV_DATE, OH.SEL_WHSE, 
            case	 
    			when OH.SEL_WHSE = '01' then '01-Anaheim'
    			when OH.SEL_WHSE = '02' then '02-Indio'
    			when OH.SEL_WHSE = '03' then '03-El Cajon'
    			when OH.SEL_WHSE = '04' then '04-Murrieta'
    			when OH.SEL_WHSE = '05' then '05-Livermore'
    			when OH.SEL_WHSE = '06' then '06-Ontario'
    			when OH.SEL_WHSE = '07' then '07-San Dimas'
    			when OH.SEL_WHSE = '08' then '08-Cathedral City'
    			when OH.SEL_WHSE = '09' then '09-San Fernando'
    			when OH.SEL_WHSE = '10' then '10-Visalia'
    			when OH.SEL_WHSE = '11' then '11-San Antonio'
    			when OH.SEL_WHSE = '12' then '12-Vista'
    			when OH.SEL_WHSE = '13' then '13-Austin'
    			when OH.SEL_WHSE = '14' then '14-Palm Springs'
    			when OH.SEL_WHSE = '15' then '15-Corona'
    			when OH.SEL_WHSE = '16' then '16-Bakersfield'
    			when OH.SEL_WHSE = '17' then '17-Houston'
    			when OH.SEL_WHSE = '18' then '18-Lake Forest'
    			when OH.SEL_WHSE = '19' then '19-Moorpark'
    			when OH.SEL_WHSE = '20' then '20-North Austin'
    			when OH.SEL_WHSE = '21' then '21-Duarte'
    			when OH.SEL_WHSE = '22' then '22-Yucaipa'
    			when OH.SEL_WHSE = '23' then '23-Riverside'
    			when OH.SEL_WHSE = '24' then '24-Long Beach'
    			when OH.SEL_WHSE = '25' then '25-Palm Desert'
    			when OH.SEL_WHSE = '26' then '26-Los Angeles'
    			when OH.SEL_WHSE = '27' then '27-Tempe'
    			when OH.SEL_WHSE = '28' then '28-Phoenix'
    			when OH.SEL_WHSE = '29' then '29-Santa Ana'
    			when OH.SEL_WHSE = '30' then '30-El Centro'
    				else OH.SEL_WHSE end Branch,
        OH.ORD_DATE, OH.INV_AMT, OH.MERCH_AMT, OH.TOT_COST, OH.CUST_NUM, min(CU.CUST_DESC) CUST_DESC, CT.CT_DESC
    from Prelude.dbo.ORDER_HISTORY_IJO OH
    left join Prelude.dbo.CUSTOMER_IJO CU on OH.CUST_NUM = cu.CUST_NUM
    left join Prelude.dbo.CUST_TYPE_1_NF CT on cu.TYPE = CT.CT_NUM 
    --pull from FWP_SQL when need to re-ETL the historical data
    --where year(OH.INV_DATE) = 2021 AND OH.SEL_WHSE NOT IN ('98','99') and OH.CUST_NUM != '0FWPCORP' AND OH.ID LIKE '001%' AND CU.ID LIKE '001%' AND CT.ID LIKE '001%'
    where OH.INV_DATE between dateadd(dd,-30,getdate()) and getdate() and OH.SEL_WHSE not in ('98','99') and OH.CUST_NUM != '0FWPCORP' --AND OH.ID LIKE '001%' AND CU.ID LIKE '001%' AND CT.ID LIKE '001%'
    group by OH.ORD_NUM, OH.INV_NUM, OH.INV_DATE, OH.SEL_WHSE, OH.ORD_DATE, OH.INV_AMT, OH.MERCH_AMT, OH.TOT_COST, OH.CUST_NUM, CT.CT_DESC--, CU.CUST_DESC
)"""

pep_df = spark.read.format("jdbc") \
   .option("url", pep_url) \
   .option("query", pep_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
pep_df.write.jdbc(url=url, table="sales.pep_invoices_incremental", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")

job.commit()



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
      and gl_revenue_account_no like '4000%'
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

queryFour = "select sales.load_invoices_mgmt_map()"
cur = conn.cursor()
cur.execute(queryFour)
conn.commit()
cur.close()

conn.close()


job.commit()