
CREATE FUNCTION warehouse.upsert_picking_and_shipping_stripped()  RETURNS void AS $$
begin
	
	-- Delete
	delete from warehouse.picking_and_shipping_stripped_fwp
	where ol_order_no in (
		select distinct ol_order_no
		from warehouse.picking_and_shipping_stripped_fwp_incremental
	);
	
	-- Reinsert 
	insert into warehouse.picking_and_shipping_stripped_fwp
	select * from warehouse.picking_and_shipping_stripped_fwp_INCREMENTAL;
	
END ;
$$
LANGUAGE plpgsql ;