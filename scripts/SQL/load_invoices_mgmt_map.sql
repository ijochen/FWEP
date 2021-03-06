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
            --lpad("Location ID"::text,3,'0'),
            --trim(leading '0' from "Location ID") location_id, 
            trim(leading '0' from branch_id) location_id, --Marty wants to remove the leading zeroes everywhere 
            "Region" region,
            "Manager" manager,
            "Time Zone" time_zone,
            "Buyer" buyer,
            "Former Entity" former_entity       
        from sales.invoice_data_merged sidm
        --use this join and save the "Location ID" column in the regional mgmt mapping sheet in sharepoint as a general data type so the join doesn't duplicate and mismatch P21 and Prelude's branch ID leading zeroes 
        left join warehouse.regional_mgmt_mapping_data rmmd on sidm.branch = rmmd."City" and trim(leading '0' from branch_id) = rmmd."Location ID"
        -- left join warehouse.regional_mgmt_mapping_data rmmd on sidm.branch = rmmd."City" and branch_id = rmmd."Location ID"
        -- and sidm.branch_id = lpad(rmmd."Location ID"::text,3,'0')
    ) a;

END;
$$
LANGUAGE plpgsql;

--SELECT sales.load_invoices_mgmt_map()