import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger('main-logger')

logger.setLevel(logging.INFO)


logger.info("******** START READING FWP *************")
erp_url = "jdbc:sqlserver://172.31.1.227:1433;databaseName=CommerceCenter"
erp_query = """
	select
	    -- General order info 
			ol.order_no as ol_order_no, -- order_number
			ol.line_no as ol_line_no,	 -- order_line
			oh.location_id as oh_location_id,
			l.location_name, 
			l.default_branch_id,
			oh.cancel_flag as oh_cancel_flag,
			oh.hold_invoice_flag as oh_hold_invoice_flag, -- order_hold (credit hold) ...
			oh.front_counter as oh_front_counter,
			ol.disposition as ol_disposition, -- order_disposition (out of stock - B backorder, T transfer, ..)
			oh.completed as oh_completed,
			oh.projected_order as oh_projected_order,
			oh.source_location_id as oh_shipping_location,
            oh.will_call as oh_will_call,
			
		-- Shipping
			oh.customer_id as customer_id,
			oh.ship2_name as customer_name,
			oh.carrier_id as oh_carrier_id, -- ship_method (pick up, UPS, ...)
		    a.name as carrier_name,
		
		-- Dates
			oh.requested_ship_date as oh_requested_ship_date,
			oh.original_promise_date as oh_original_promise_date,
			oh.promise_date as oh_promise_date,
			ol.required_date as ol_required_date, -- promise ship date  
			oh.order_date as oh_order_date,
		
		-- Size of the order
			-- dollar_value_order
			-- pick_order_lines
			oh.pick_ticket_type as oh_pick_ticket_type, -- priced or unpriced 
			oh.packing_basis as oh_packing_basis -- order full order partial (can ship without fully packed)
	from dbo.oe_line ol WITH (NOLOCK)
	left join dbo.oe_hdr oh WITH (NOLOCK)
	    on ol.order_no = oh.order_no 
	left join dbo.location l WITH (NOLOCK)
		on oh.location_id = l.location_id
	left join dbo.address a WITH (NOLOCK)
	    on oh.carrier_id = a.id
	where ol.order_no in (
		select
			distinct key1_value 
		from dbo.audit_trail WITH (NOLOCK) 
		where key1_cd = 'order_no' and 
			date_created >= DATEADD(day,-30, GETDATE()) 
	)
"""
erp_order_lines_df = spark.read.format("jdbc") \
    .option("url", erp_url) \
    .option("query", erp_query) \
    .option("user", "pwales") \
    .option("password", "Bi4437j!") \
    .load()


erp_ship_query = """
    select 
		a.order_no, 
        a.line_no,
		a.qty_invoiced,
		a.date_created as ship_date,
		u.name as shipper,
		--a.clean_created_by as shipper,
		row_number() over (partition by a.order_no, a.line_no order by a.date_created asc) as row_num
	from (
		select
			key1_value as order_no,
            line_no,
			new_value as qty_invoiced,
			date_created,
			created_by,
			replace(created_by, 'FWPNET\\', '') as clean_created_by
		from dbo.audit_trail WITH (NOLOCK)
		where key1_cd = 'order_no' and 
			column_changed = 'qty_invoiced' and
			cast(new_value as float) > 0.0 and 
			key1_value in (
				select
					distinct key1_value 
				from dbo.audit_trail WITH (NOLOCK) 
				where key1_cd = 'order_no' and 
					date_created >= DATEADD(day,-30, GETDATE()) 
			)
	) a 
	left join dbo.users u WITH (NOLOCK)
		on a.clean_created_by = u.id
"""

erp_pick_query = """
    select 
		a.order_no, 
        a.line_no,
		a.qty_picked, 
		a.date_created as pick_date,
		u.name as picker,
		--a.clean_created_by as picker,
		row_number() over (partition by a.order_no, a.line_no order by a.date_created asc) as row_num
	from (
		select
			key1_value as order_no,
            line_no,
			new_value as qty_picked,
			date_created,
			created_by,
			replace(created_by, 'FWPNET\\', '') as clean_created_by
		from dbo.audit_trail WITH (NOLOCK)
		where key1_cd = 'order_no' and 
			column_changed = 'qty_on_pick_tickets' and 
			cast(new_value as float) > 0.0 and 
			key1_value in (
				select
					distinct key1_value 
				from dbo.audit_trail WITH (NOLOCK) 
				where key1_cd = 'order_no' and 
					date_created >= DATEADD(day,-30, GETDATE())
			)
	) a 
	left join dbo.users u WITH (NOLOCK)
		on a.clean_created_by = u.id 
"""


erp_print_date_query = """
    select 
        t.pick_ticket_no,
        t.order_no,
        td.oe_line_no,
        t.print_date,
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
        where key1_cd = 'order_no' and 
            date_created >= DATEADD(day, -30, GETDATE())
    ) and t.delete_flag = 'N'
"""


erp_ship_df = spark.read.format("jdbc") \
    .option("url",  erp_url) \
    .option("query", erp_ship_query) \
    .option("user", "pwales") \
    .option("password", "Bi4437j!") \
    .load() 

erp_pick_df = spark.read.format("jdbc") \
    .option("url",  erp_url) \
    .option("query", erp_pick_query) \
    .option("user", "pwales") \
    .option("password", "Bi4437j!") \
    .load()
    
erp_print_date_df = spark.read.format("jdbc") \
    .option("url",  erp_url) \
    .option("query", erp_print_date_query) \
    .option("user", "pwales") \
    .option("password", "Bi4437j!") \
    .load()


joined_df = erp_order_lines_df \
	.join(erp_print_date_df, \
        (erp_order_lines_df.ol_order_no == erp_print_date_df.order_no) & \
        (erp_order_lines_df.ol_line_no == erp_print_date_df.oe_line_no), 'left') \
	.join(erp_pick_df, \
        (erp_order_lines_df.ol_order_no == erp_pick_df.order_no) & \
        (erp_order_lines_df.ol_line_no == erp_pick_df.line_no) & \
		(erp_print_date_df.row_num == erp_pick_df.row_num), 'left') \
    .join(erp_ship_df, \
        (erp_order_lines_df.ol_order_no == erp_ship_df.order_no) & \
        (erp_order_lines_df.ol_line_no == erp_ship_df.line_no) & \
		(erp_print_date_df.row_num == erp_ship_df.row_num), 'left') \
    .select( \
        erp_order_lines_df["*"], \
        erp_ship_df["qty_invoiced"], \
        erp_ship_df["ship_date"], \
        erp_ship_df["shipper"], \
        erp_pick_df["qty_picked"], \
        erp_pick_df["pick_date"], \
        erp_pick_df["picker"], \
        erp_print_date_df["pick_ticket_no"], \
        erp_print_date_df["print_date"], \
        erp_print_date_df["print_quantity"]
    )

mode = "overwrite"
url = "jdbc:postgresql://db-cluster.cluster-ce0xsttrdwys.us-east-2.rds.amazonaws.com:5432/analytics"
properties = {"user": "postgres","password": "kHSmwnXWrG^L3N$V2PXPpY22*47","driver": "org.postgresql.Driver"}
joined_df.write.jdbc(url=url, table="warehouse.picking_and_shipping_fwp_INCREMENTAL", mode=mode, properties=properties)

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

query = "select warehouse.upsert_picking_and_shipping_fwp()"
cur = conn.cursor()
cur.execute(query)
conn.commit()
cur.close()


conn.close()


job.commit()