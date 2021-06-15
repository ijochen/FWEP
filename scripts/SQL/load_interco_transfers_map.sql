CREATE FUNCTION warehouse.load_interco_transfers() RETURNS void AS $$
BEGIN
    DROP TABLE IF EXISTS warehouse.interco_transfers;

    CREATE TABLE warehouse.interco_transfers AS
    SELECT * FROM (
        select 
        trh_transfer_no transfer_no,
        transfer_date,
        unit_quantity,
        qty_to_transfer,
        qty_transferred,
        avg_fifo_cost, 
        item_id,
        item_desc,
        from_location_id,
        from_location,
        to_location_id,
        to_location,
        planned_recpt_date,
        hdr_created_by,
        line_created_by,
        "From ID" from_id,
        "From Name" from_name,
        "From State" from_state,
        "To ID" to_id,
        "To Name" to_name,
        "To State" to_state,
        "Touches" touches,
        "LANE TYPE" lane_type
        from warehouse.inv_transfers it
        left join warehouse.intercompany_transfer_mapping_data itmd 
            on it.from_location_id = itmd."From ID" and it.to_location_id = itmd."To ID"
    ) a;

END;
$$
LANGUAGE plpgsql;

