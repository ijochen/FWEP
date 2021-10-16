--DROP FUNCTION warehouse.upsert_fwp_inventory_position()

CREATE FUNCTION warehouse.upsert_fwp_inventory_position() RETURNS void AS $$
BEGIN
	
    --Create table that counts the duplicates in all the tables
    DROP TABLE IF EXISTS warehouse.inventory_position_count;

    CREATE TABLE warehouse.inventory_position_count as 
    SELECT *, COUNT(item_id) OVER( PARTITION BY location_id, item_id, qty_on_hand, qty_allocated, qty_available) COUNT
    FROM (
        --main table for Tableau Dash
        select * from warehouse.inventory_position
        union
        --original table from first ETL
        SELECT * FROM warehouse.inventory_position_interim
        UNION
        --daily refreshed table of what's currently in stock at each branch
        SELECT *, CURRENT_DATE trans_date FROM warehouse.inventory_position_incremental
    ) t1;


    --create a table that ranks the duplicates by dates to see which was the original record
    DROP TABLE IF EXISTS warehouse.inventory_position_rank;

    CREATE TABLE warehouse.inventory_position_rank as 
    SELECT *, dense_rank() OVER( PARTITION BY location_id, item_id, count order by count, trans_date asc) rank
    FROM (
        SELECT * FROM warehouse.inventory_position_count
    ) t2;

    --delete the duplicates before inserting it into the production table
    DELETE FROM warehouse.inventory_position_rank 
    WHERE count = 2 and rank = 2;

    --delete all rows before inserting the unique/distinct records
    TRUNCATE TABLE warehouse.inventory_position;

    --reinsert distinct records
    INSERT INTO warehouse.inventory_position
    SELECT location_id, branch_description, item_group, item_id, item_desc, supplier_part_no, unit_of_measure, avg_fifo_cost, qty_on_hand, qty_allocated, qty_available, trans_date
    FROM warehouse.inventory_position_rank;

    --drop these tables to save on cost/space
    DROP TABLE IF EXISTS warehouse.inventory_position_count, warehouse.inventory_position_rank ;

	
END ;
$$
LANGUAGE plpgsql ;


--SELECT warehouse.upsert_fwp_inventory_position()