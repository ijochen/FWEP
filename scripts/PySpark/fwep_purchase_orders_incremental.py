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


# 1) Load PEP - Read from FWP_SQL.Prelude Database and write to Data Warehouse 
logger.info("******** START READING PEP *************")

# PEP POs Last 60 Days
pep_url = "jdbc:sqlserver://10.0.10.18:1433;databaseName=Prelude"
pep_query = """(
    SELECT --DISTINCT --TOP(100)
        'PEP' AS COMPANY,
        'PRELUDE' AS ERP,
        PH.PO_TYPE,
        PH.PO_NUM,
        PL.SEQ_NUM,
        PH.VEND_NUM,
        V.VEND_DESC,
        PH.BUYER_NUM,
        CASE WHEN U.USER_DESC IS NULL THEN PH.BUYER_NUM ELSE U.USER_DESC END BUYER_NAME,
        PL.PROD_NUM,
        CONCAT(P.PROD_DESC1,' ',P.PROD_DESC2) PROD_DESC,
        CA.PLINE_DESC PROD_CATEGORY,
        PL.VPROD_NUM,
        PL.UN_MEAS,
        ORD_QTY,
        REC_QTY,
        PL.GRS_COST,
        PL.EXT_AMT,
        PH.CENTRAL_WHSE_NUM,
        CASE	 
            WHEN PH.CENTRAL_WHSE_NUM = '01' THEN 'PEP - ANAHEIM CA'
            WHEN PH.CENTRAL_WHSE_NUM = '02' THEN 'PEP - INDIO CA'
            WHEN PH.CENTRAL_WHSE_NUM = '03' THEN 'PEP - EL CAJON CA'
            WHEN PH.CENTRAL_WHSE_NUM = '04' THEN 'PEP - MURRIETA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '05' THEN 'PEP - LIVERMORE CA'
            WHEN PH.CENTRAL_WHSE_NUM = '06' THEN 'PEP - ONTARIO CA'
            WHEN PH.CENTRAL_WHSE_NUM = '07' THEN 'PEP - SAN DIMAS CA'
            WHEN PH.CENTRAL_WHSE_NUM = '08' THEN 'PEP - CATHEDRAL CITY CA'
            WHEN PH.CENTRAL_WHSE_NUM = '09' THEN 'PEP - SAN FERNANDO CA'
            WHEN PH.CENTRAL_WHSE_NUM = '10' THEN 'PEP - VISALIA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '11' THEN 'PEP - SAN ANTONIO TX'
            WHEN PH.CENTRAL_WHSE_NUM = '12' THEN 'PEP - VISTA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '13' THEN 'PEP - AUSTIN TX'
            WHEN PH.CENTRAL_WHSE_NUM = '14' THEN 'PEP - PALM SPRINGS CA'
            WHEN PH.CENTRAL_WHSE_NUM = '15' THEN 'PEP - CORONA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '16' THEN 'PEP - BAKERSFIELD CA'
            WHEN PH.CENTRAL_WHSE_NUM = '17' THEN 'PEP - HOUSTON TX'
            WHEN PH.CENTRAL_WHSE_NUM = '18' THEN 'PEP - LAKE FOREST CA'
            WHEN PH.CENTRAL_WHSE_NUM = '19' THEN 'PEP - MOORPARK CA'
            WHEN PH.CENTRAL_WHSE_NUM = '20' THEN 'PEP - NORTH AUSTIN TX'
            WHEN PH.CENTRAL_WHSE_NUM = '21' THEN 'PEP - DUARTE CA'
            WHEN PH.CENTRAL_WHSE_NUM = '22' THEN 'PEP - YUCAIPA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '23' THEN 'PEP - RIVERSIDE CA'
            WHEN PH.CENTRAL_WHSE_NUM = '24' THEN 'PEP - LONG BEACH CA'
            WHEN PH.CENTRAL_WHSE_NUM = '25' THEN 'PEP - PALM DESERT CA'
            WHEN PH.CENTRAL_WHSE_NUM = '26' THEN 'PEP - LOS ANGELES CA'
            WHEN PH.CENTRAL_WHSE_NUM = '27' THEN 'PEP - TEMPE AZ'
            WHEN PH.CENTRAL_WHSE_NUM = '28' THEN 'PEP - PHOENIX AZ'
            WHEN PH.CENTRAL_WHSE_NUM = '29' THEN 'PEP - SANTA ANA CA'
            WHEN PH.CENTRAL_WHSE_NUM = '30' THEN 'PEP - EL CENTRO CA'
            WHEN PH.CENTRAL_WHSE_NUM = '98' THEN 'PEP - CORPORATE WAREHOUSE'
            WHEN PH.CENTRAL_WHSE_NUM = '99' THEN 'PEP - CENTRAL SHIPPING WAREHOUSE'
                ELSE PH.CENTRAL_WHSE_NUM END CENTRAL_WHSE_NAME,
        PL.WHSE_NUM,
        CASE	 
            WHEN PL.WHSE_NUM = '01' THEN 'PEP - ANAHEIM CA'
            WHEN PL.WHSE_NUM = '02' THEN 'PEP - INDIO CA'
            WHEN PL.WHSE_NUM = '03' THEN 'PEP - EL CAJON CA'
            WHEN PL.WHSE_NUM = '04' THEN 'PEP - MURRIETA CA'
            WHEN PL.WHSE_NUM = '05' THEN 'PEP - LIVERMORE CA'
            WHEN PL.WHSE_NUM = '06' THEN 'PEP - ONTARIO CA'
            WHEN PL.WHSE_NUM = '07' THEN 'PEP - SAN DIMAS CA'
            WHEN PL.WHSE_NUM = '08' THEN 'PEP - CATHEDRAL CITY CA'
            WHEN PL.WHSE_NUM = '09' THEN 'PEP - SAN FERNANDO CA'
            WHEN PL.WHSE_NUM = '10' THEN 'PEP - VISALIA CA'
            WHEN PL.WHSE_NUM = '11' THEN 'PEP - SAN ANTONIO TX'
            WHEN PL.WHSE_NUM = '12' THEN 'PEP - VISTA CA'
            WHEN PL.WHSE_NUM = '13' THEN 'PEP - AUSTIN TX'
            WHEN PL.WHSE_NUM = '14' THEN 'PEP - PALM SPRINGS CA'
            WHEN PL.WHSE_NUM = '15' THEN 'PEP - CORONA CA'
            WHEN PL.WHSE_NUM = '16' THEN 'PEP - BAKERSFIELD CA'
            WHEN PL.WHSE_NUM = '17' THEN 'PEP - HOUSTON TX'
            WHEN PL.WHSE_NUM = '18' THEN 'PEP - LAKE FOREST CA'
            WHEN PL.WHSE_NUM = '19' THEN 'PEP - MOORPARK CA'
            WHEN PL.WHSE_NUM = '20' THEN 'PEP - NORTH AUSTIN TX'
            WHEN PL.WHSE_NUM = '21' THEN 'PEP - DUARTE CA'
            WHEN PL.WHSE_NUM = '22' THEN 'PEP - YUCAIPA CA'
            WHEN PL.WHSE_NUM = '23' THEN 'PEP - RIVERSIDE CA'
            WHEN PL.WHSE_NUM = '24' THEN 'PEP - LONG BEACH CA'
            WHEN PL.WHSE_NUM = '25' THEN 'PEP - PALM DESERT CA'
            WHEN PL.WHSE_NUM = '26' THEN 'PEP - LOS ANGELES CA'
            WHEN PL.WHSE_NUM = '27' THEN 'PEP - TEMPE AZ'
            WHEN PL.WHSE_NUM = '28' THEN 'PEP - PHOENIX AZ'
            WHEN PL.WHSE_NUM = '29' THEN 'PEP - SANTA ANA CA'
            WHEN PL.WHSE_NUM = '30' THEN 'PEP - EL CENTRO CA'
            WHEN PL.WHSE_NUM = '98' THEN 'PEP - CORPORATE WAREHOUSE'
            WHEN PL.WHSE_NUM = '99' THEN 'PEP - CENTRAL SHIPPING WAREHOUSE'
                ELSE PL.WHSE_NUM END WHSE_NAME,		
        CAST(PH.PO_DATE AS DATE) PO_DATE,
        CAST(PH.REC_DATE AS DATE) REC_DATE,
        CASE WHEN PL.DEL_DATE = '0000-00-00' OR PL.DEL_DATE = '1606-00-00' THEN '1900-01-01' ELSE CAST(PL.DEL_DATE AS DATE) END DEL_DATE
    FROM PRELUDE.DBO.PO_HISTORY_LINE_IJO PL
    LEFT JOIN PRELUDE.DBO.PO_HISTORY_IJO PH ON LEFT(PL.ID, CHARINDEX('!',PL.ID)-1) = PH.ID
    LEFT JOIN PRELUDE.DBO.PRODUCT_IJO P ON PL.PROD_NUM = P.PROD_NUM
    LEFT JOIN PRELUDE.DBO.CATEGORY_IJO CA ON P.PLINE_NUM = CA.PLINE_NUM
    LEFT JOIN PRELUDE.DBO.VEND_IJO V ON PH.VEND_NUM = V.VEND_NUM AND PH.CO_NUM = V.CO_NUM
    LEFT JOIN PRELUDE.DBO.BUYER_NF B ON PH.BUYER_NUM = B.BUYER_NUM
    LEFT JOIN PRELUDE.DBO.USER_ID_NF U ON PH.BUYER_NUM = U.USER_NUM
    WHERE P.CO_NUM = '001' AND CA.CO_NUM = '001'
		/*AND PL.PROD_NUM NOT IN ('C','CSB','CS','CI','CN','CP','MN')  AND */
		AND PH.PO_DATE >= DATEADD(DAY,-60,GETDATE())
    GROUP BY PH.PO_TYPE, PH.PO_NUM, PL.SEQ_NUM,
        PH.VEND_NUM, V.VEND_DESC, PH.BUYER_NUM, B.BUYER_DESC, U.USER_DESC,
        PL.PROD_NUM, P.PROD_DESC1, P.PROD_DESC2, CA.PLINE_DESC, PL.VPROD_NUM,
        PL.UN_MEAS,	PL.ORD_QTY, PL.REC_QTY,
        PL.GRS_COST, PL.EXT_AMT,
        PH.CENTRAL_WHSE_NUM, PL.WHSE_NUM,
        PH.PO_DATE, PH.REC_DATE, PL.DEL_DATE
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
pep_df.write.jdbc(url=url, table="procurement.pep_purchase_orders_incremental", mode=mode, properties=properties)


logger.info("******** END READING PEP *************")


# 2) Load FWP - Read from Sql Server and write to Data Warehouse

logger.info("******** START READING FWP *************")

# FWP POs Last 60 Days
fwp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
fwp_query = """(
    select distinct --top(100)
        'FWP' as company,
        'P21' as erp,  
        ph.po_type,         
        cast(ph.po_no as varchar) po_no,
        cast(pl.line_no as varchar) line_no,
        cast(ph.vendor_id as varchar) vendor_id, 
        v.vendor_name,
        case when pl.created_by like 'FWPNET\%' then replace(pl.created_by,'FWPNET\\','') 
            when pl.created_by like 'poolelectrical\%' then replace(pl.created_by,'poolelectrical\\','') 
            end buyer,
        u.name buyer_name,
        i.item_id prod_num,
        pl.item_description prod_desc,
        i.default_sales_discount_group prod_group,
        ins.supplier_part_no,
        pl.unit_of_measure,
        cast(pl.qty_ordered as varchar) qty_ordered,
        cast(pl.qty_received as varchar) qty_received,
        pl.unit_price,
        (pl.unit_price * pl.qty_ordered) ext_price,
        cast(ph.location_id as varchar) location_id,
        b1.branch_description location_name,
        ph.branch_id,
        b2.branch_description branch_name,
        cast(ph.order_date as date) order_date,
        cast(pl.received_date as date) received_date,
        cast(pl.date_due as date) promised_del_date
    from CommerceCenter.dbo.po_line pl
    left join CommerceCenter.dbo.po_hdr ph on pl.po_no = ph.po_no
    left join CommerceCenter.dbo.vendor v on ph.vendor_id = v.vendor_id
    left join CommerceCenter.dbo.inv_mast i on pl.inv_mast_uid = i.inv_mast_uid
    left join CommerceCenter.dbo.inventory_supplier ins on pl.inv_mast_uid = ins.inv_mast_uid and ph.vendor_id = ins.supplier_id
    left join CommerceCenter.dbo.branch b1 on ph.location_id = b1.branch_id
    left join CommerceCenter.dbo.branch b2 on ph.branch_id = b2.branch_id
    left join CommerceCenter.dbo.users u on replace(pl.created_by,'FWPNET\\','') = u.id or replace(pl.created_by,'poolelectrical\\','') = u.id
    where year(ph.order_date) >= 2019 and ins.supplier_part_no is not null
    group by ph.po_type, ph.po_no, pl.line_no, 
        ph.vendor_id, v.vendor_name, pl.created_by, u.name,
        i.item_id, pl.item_description, i.default_sales_discount_group, ins.supplier_part_no, 
        pl.unit_of_measure,	pl.qty_ordered, pl.qty_received, pl.unit_price, 
        ph.location_id, ph.branch_id, b1.branch_description, b2.branch_description,
        ph.order_date, pl.received_date, pl.date_due
)"""

fwp_df = spark.read.format("jdbc") \
   .option("url", fwp_url) \
   .option("query", fwp_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()
   
mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
fwp_df.write.jdbc(url=url, table="procurement.fwp_purchase_orders_incremental", mode=mode, properties=properties)


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

query = "select procurement.upsert_fwp_purchases()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()


queryTwo = "select procurement.upsert_pep_purchases()"
cur = conn.cursor()
cur.execute(queryTwo)
conn.commit()
cur.close()

queryThree = "select procurement.load_fwep_purchases()"
cur = conn.cursor()
cur.execute(queryThree)
conn.commit()
cur.close()

conn.close()


job.commit()