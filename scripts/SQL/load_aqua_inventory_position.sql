-- DROP FUNCTION warehouse.load_aqua_inventory_position()

CREATE FUNCTION warehouse.load_aqua_inventory_position() RETURNS void AS $$
BEGIN

    DROP TABLE IF EXISTS warehouse.aqua_inventory_position;

    CREATE TABLE warehouse.aqua_inventory_position AS
    SELECT * FROM 
    (       
        (
            SELECT 
                erp
                , cast(fipi.location_id as text) location_id 
                , fipi.location_name
                , fipi.item_group
                , fipi.item_id
                , fipi.item_desc
                , fipi.supplier_part_no
                , fipi.unit_of_measure
                , fipi.avg_fifo_cost
                , fipi.qty_on_hand
                , fipi.qty_allocated
                , fipi.qty_available
                , fipi.trans_date
                , coalesce(fsdm.qty_sold,0) qty_sold
                , coalesce(fpdm.qty_received,0) qty_received
            FROM warehouse.fweps_inventory_position_interim fipi 
            -- Demand: What did we sell to Customers?
            LEFT JOIN (
                        SELECT
                            fsdm.branch_id, 
                            fsdm.prod_num, 
                            extract(month from fsdm.invoice_date) as month, 
                            extract(year from fsdm.invoice_date) as year, 
                            sum(fsdm.qty_ordered) qty_sold
                        FROM sales.fwep_sales_data_merged fsdm
                        WHERE fsdm.company = 'FWP'
                        GROUP BY
                            fsdm.branch_id, 
                            fsdm.prod_num, 
                            extract(month from fsdm.invoice_date), 
                            extract(year from fsdm.invoice_date)
                        ) fsdm
                            on cast(fipi.location_id as text) = trim(leading '0' from fsdm.branch_id) 
                            and fipi.item_id = fsdm.prod_num 
                            and extract(month from fipi.trans_date) = fsdm.month
                            and extract(year from fipi.trans_date) = fsdm.year
            -- Supply: What did we receive from Vendors?
            LEFT JOIN (
                        SELECT
                            fpdm.location_id, 
                            fpdm.prod_num, 
                            extract(year from fpdm.received_date) as year, 
                            extract(month from fpdm.received_date) as month, 
                            sum(cast(fpdm.qty_received as float)) qty_received
                        FROM procurement.fwep_purchase_data_merged fpdm 
                        WHERE fpdm.company = 'FWP'
                        GROUP BY
                            fpdm.location_id, 
                            fpdm.prod_num, 
                            extract(year from fpdm.received_date), 
                            extract(month from fpdm.received_date)
                        ) fpdm 
                            on cast(fipi.location_id as text) = fpdm.location_id 
                            and fipi.item_id = fpdm.prod_num 
                            and extract(year from fipi.trans_date) = fpdm.year
                            and extract(month from fipi.trans_date) = fpdm.month
        )
        UNION
        (
            SELECT 
                pipi.*
                , coalesce(fsdm.qty_sold,0) qty_sold 
                , coalesce(fpdm.qty_received,0) qty_received
            FROM warehouse.pep_inventory_position_interim pipi 
            -- Demand: What did we sell to Customers?
            LEFT JOIN (
                        SELECT
                            fsdm.branch_id
                            , fsdm.prod_num
                            , extract(month from fsdm.invoice_date) as month
                            , extract(year from fsdm.invoice_date) as year
                            , sum(fsdm.qty_ordered) qty_sold
                        FROM sales.fwep_sales_data_merged fsdm
                        WHERE fsdm.company = 'PEP'
                        GROUP BY
                            fsdm.branch_id
                            , fsdm.prod_num
                            , extract(month from fsdm.invoice_date)
                            , extract(year from fsdm.invoice_date)
                        ) fsdm 
                            --on pipi."WHSE_NUM" = trim(leading '0' from fsdm.branch_id)
                            ON pipi."WHSE_NUM" = fsdm.branch_id
                            AND pipi."PROD_NUM" = fsdm.prod_num
                            AND extract(month from pipi."TRANS_DATE") = fsdm.month
                            AND extract(year from pipi."TRANS_DATE") = fsdm.year
            -- Supply: What did we receive from Vendors?
            LEFT JOIN (
                        SELECT
                            fpdm.location_id
                            , fpdm.prod_num
                            , extract(month from fpdm.received_date) as month
                            , extract(year from fpdm.received_date) as year
                            --, sum(cast(fpdm.qty_received as float8)) qty_received
                            --, case when cast(fpdm.qty_received as varchar) = '' then '0' else sum(cast(fpdm.qty_received as float8)) end qty_received
                            , sum(cast(case when cast(fpdm.qty_received as varchar) = '' then '0' else fpdm.qty_received end as float)) qty_received
                        FROM procurement.fwep_purchase_data_merged fpdm 
                        WHERE fpdm.company = 'PEP'
                        GROUP BY
                            fpdm.location_id
                            , fpdm.prod_num
                            , extract(month from fpdm.received_date)
                            , extract(year from fpdm.received_date)

                        ) fpdm
                            --on pipi."WHSE_NUM" = trim(leading '0' from fpdm.location_id)
                            ON pipi."WHSE_NUM" = fpdm.location_id
                            AND pipi."PROD_NUM" = fpdm.prod_num
                            AND extract(month from pipi."TRANS_DATE") = fpdm.month
                            AND extract(year from pipi."TRANS_DATE") = fpdm.year
        )
    ) a;

END ;
$$
LANGUAGE plpgsql ;

-- SELECT warehouse.load_aqua_inventory_position()