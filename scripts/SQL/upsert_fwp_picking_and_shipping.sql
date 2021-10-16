

--select warehouse.upsert_picking_and_shipping_fwp()

--drop function warehouse.upsert_picking_and_shipping_fwp()

-- truncate table warehouse.picking_and_shipping_fwp


CREATE FUNCTION warehouse.upsert_picking_and_shipping_fwp()  RETURNS void AS $$
begin
	
	-- Delete
	delete from warehouse.picking_and_shipping_fwp
	where ol_order_no in (
		select distinct ol_order_no
		from warehouse.picking_and_shipping_fwp_INCREMENTAL
	);
	
	-- Reinsert 
	insert into warehouse.picking_and_shipping_fwp
	select * from warehouse.picking_and_shipping_fwp_INCREMENTAL;
	
END ;
$$
LANGUAGE plpgsql ;

