--DROP FUNCTION sales.load_invoices_mgmt_map()

CREATE FUNCTION sales.load_invoices_mgmt_map() RETURNS void AS $$
BEGIN
    DROP TABLE IF EXISTS sales.invoice_data_merged_mgmt;

    CREATE TABLE sales.invoice_data_merged_mgmt AS
    SELECT * FROM (
        select 
        order_no,
        invoice_no,
        invoice_date,
        branch,
        order_date,
        total_sales, 
        total_cost,
        channel,
        company,
        "Director" director,
        "State" state,
        "Region Detail" region_detail,
        "Regional Leader" regional_leader,
        "City" city,
        "Location ID" location_id, 
        "Region" region,
        "Manager" manager,
        "Time Zone" time_zone       
        from sales.invoice_data_merged sidm
        left join warehouse.regional_mgmt_mapping_data rmmd on sidm.branch = rmmd."City"
    ) a;

END;
$$
LANGUAGE plpgsql;

--SELECT sales.load_invoices_mgmt_map()