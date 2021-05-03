
CREATE FUNCTION warehouse.upsert_front_counter()  RETURNS void AS $$
begin
	
	-- Delete
	delete from warehouse.front_counter_fwp
	where ol_order_no in (
		select distinct ol_order_no
		from warehouse.front_counter_fwp_INCREMENTAL
	);
	
	-- Reinsert 
	insert into warehouse.front_counter_fwp
	select * from warehouse.front_counter_fwp_INCREMENTAL;
	
END ;
$$
LANGUAGE plpgsql ;