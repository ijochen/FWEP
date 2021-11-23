CREATE FUNCTION warehouse.load_fweps_inventory_position() RETURNS void AS $$
BEGIN

    DROP TABLE IF EXISTS warehouse.fweps_inventory_position;

    CREATE TABLE warehouse.fweps_inventory_position AS
    SELECT * FROM (
        select 
            ipi.*, 
            coalesce(fsdm.qty_sold,0) qty_sold, 
            coalesce(fpdm.qty_received,0) qty_received
        from warehouse.fweps_inventory_position_interim ipi 
        -- Demand: What did we sell to Customers?
        left join (
                    select 
                        fsdm.branch_id, 
                        fsdm.prod_num, 
                        extract(month from fsdm.invoice_date) as month, 
                        extract(year from fsdm.invoice_date) as year, 
                        sum(fsdm.qty_ordered) qty_sold
                    from sales.fwep_sales_data_merged fsdm
                    where fsdm.company = 'FWP'
                    group by 
                        fsdm.branch_id, 
                        fsdm.prod_num, 
                        extract(month from fsdm.invoice_date), 
                        extract(year from fsdm.invoice_date)
                    ) fsdm 
                        on cast(ipi.location_id as text) = trim(leading '0' from fsdm.branch_id) 
                        and ipi.item_id = fsdm.prod_num 
                        and extract(month from ipi.trans_date) = fsdm.month
                        and extract(year from ipi.trans_date) = fsdm.year
        -- Supply: What did we receive from Vendors?
        left join (
                    select 
                        fpdm.location_id, 
                        fpdm.prod_num, 
                        extract(year from fpdm.received_date) as year, 
                        extract(month from fpdm.received_date) as month, 
                        sum(cast(fpdm.qty_received as float)) qty_received
                    from procurement.fwep_purchase_data_merged fpdm 
                    where fpdm.company = 'FWP'
                    group by 
                        fpdm.location_id, 
                        fpdm.prod_num, 
                        extract(year from fpdm.received_date), 
                        extract(month from fpdm.received_date)
                    ) fpdm 
                        on cast(ipi.location_id as text) = fpdm.location_id 
                        and ipi.item_id = fpdm.prod_num 
                        and extract(year from ipi.trans_date) = fpdm.year
                        and extract(month from ipi.trans_date) = fpdm.month
    ) a;

END ;
$$
LANGUAGE plpgsql ;
