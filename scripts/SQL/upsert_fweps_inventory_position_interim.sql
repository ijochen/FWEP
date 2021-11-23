-- drop function warehouse.fweps_inventory_position_incremental;

CREATE FUNCTION warehouse.upsert_fweps_inventory_position_interim()  RETURNS void AS $$
BEGIN
	
	-- Delete
	delete from warehouse.fweps_inventory_position_interim
	where date_trunc('month', trans_date) in (
		select trans_date 
	    from warehouse.fweps_inventory_position_incremental
	);
	
	-- Reinsert 
	insert into warehouse.fweps_inventory_position_interim 
	select * from warehouse.fweps_inventory_position_incremental;

	
END ;
$$
LANGUAGE plpgsql ;

-- select * from warehouse.fweps_inventory_position_incremental;