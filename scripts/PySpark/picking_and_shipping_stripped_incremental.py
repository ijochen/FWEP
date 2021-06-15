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


erp_url = "jdbc:sqlserver://128.1.100.9:1433;databaseName=CommerceCenter"
erp_query = """(
    select 
        ol.order_no ol_order_no,
        ol.line_no ol_line_no,

        oh.customer_id oh_customer_id,
        ship2_name customer_name, 
        location_id,
        source_location_id shipping_location,
        
        front_counter,
		ol.disposition ol_disposition,
		oh.projected_order oh_projected_orders,
        oh.approved oh_approved,
		oh.completed oh_completed,
		oh.will_call oh_will_call,	    

        oh.date_created oh_date_created, 
        ol.date_created ol_date_created

    from FWP_SQL.CommerceCenter.dbo.oe_line ol
    left join FWP_SQL.CommerceCenter.dbo.oe_hdr oh 
        on ol.order_no = oh.order_no
    where ol.order_no in (
		select
		    distinct key1_value 
		from dbo.audit_trail WITH (NOLOCK) 
		where key1_cd = 'order_no' 
		   and date_created >= DATEADD(day,-1, GETDATE())
    )
)"""

erp_order_lines_df = spark.read.format("jdbc") \
   .option("url", erp_url) \
   .option("query", erp_query) \
   .option("user", "ichen") \
   .option("password", "Qwer1234$") \
   .load()

erp_print_date_query = """
    select 
        t.pick_ticket_no,
        t.order_no,
        td.oe_line_no,
        t.invoice_no,
        t.print_date,
        t.ship_date,
        t.delete_flag,
        td.print_quantity,
		row_number() over (partition by t.order_no, td.oe_line_no order by t.print_date asc) as row_num
    from dbo.oe_pick_ticket t
    left join dbo.oe_pick_ticket_detail td
        on t.pick_ticket_no = td.pick_ticket_no
    where t.order_no in (
        select 
            distinct key1_value 
        from dbo.audit_trail WITH (NOLOCK)
        where key1_cd = 'order_no' 
            and date_created >= DATEADD(day,-1, GETDATE())
    ) and t.delete_flag = 'N'
"""

erp_print_date_df = spark.read.format("jdbc") \
    .option("url",  erp_url) \
    .option("query", erp_print_date_query) \
    .option("user", "ichen") \
    .option("password", "Qwer1234$") \
    .load()


joined_df = erp_order_lines_df \
	.join(erp_print_date_df, \
        (erp_order_lines_df.ol_order_no == erp_print_date_df.order_no) & \
        (erp_order_lines_df.ol_line_no == erp_print_date_df.oe_line_no), 'left') \
    .select( \
        erp_order_lines_df["*"], \
        erp_print_date_df["pick_ticket_no"], \
        erp_print_date_df["print_date"], \
        erp_print_date_df["ship_date"], \
        erp_print_date_df["print_quantity"], \
        erp_print_date_df["invoice_no"], \
    )

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
joined_df.write.jdbc(url=url, table="warehouse.picking_and_shipping_stripped_fwp_incremental_new", mode=mode, properties=properties)


# 3) Merge tables together in a stored proc
import pg8000

conn = pg8000.connect(
    database='analytics',
    user='postgres',
    password='kHSmwnXWrG^L3N$V2PXPpY22*47',
    host='db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com',
    port=5432
)

query = "select warehouse.upsert_picking_and_shipping_stripped()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()

conn.close()


job.commit()