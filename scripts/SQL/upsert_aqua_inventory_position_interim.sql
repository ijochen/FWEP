-- drop function warehouse.upsert_aqua_inventory_position_interim();

CREATE FUNCTION warehouse.upsert_aqua_inventory_position_interim()  RETURNS void AS $$
BEGIN
	
	-- Delete P21 FWP/APS/PEP Data
		--for the current month, because the current month's date will be dynamic. 
		--When we roll into the next month, the previous months' data will be static.		
	delete from warehouse.fweps_inventory_position_interim
	where date_trunc('month', trans_date) in (
		select trans_date 
	    from warehouse.fweps_inventory_position_incremental
	);
	
	-- Reinsert P21 FWP/APS/PEP Data
	insert into warehouse.fweps_inventory_position_interim 
	select * from warehouse.fweps_inventory_position_incremental;

	
	-- Delete Prelude PEP Data 
		--for the current month, because the current month's date will be dynamic. 
		--When we roll into the next month, the previous months' data will be static.
	DELETE FROM warehouse.pep_inventory_position_interim
	WHERE DATE_TRUNC('MONTH', "TRANS_DATE") IN (
		SELECT "TRANS_DATE"
		FROM warehouse.pep_inventory_position_incremental
	);

	-- Reinsert Prelude PEP Data 
	INSERT INTO warehouse.pep_inventory_position_interim
	SELECT * FROM warehouse.pep_inventory_position_incremental;
	
END ;
$$
LANGUAGE plpgsql ;

-- select * from warehouse.upsert_aqua_inventory_position_interim();