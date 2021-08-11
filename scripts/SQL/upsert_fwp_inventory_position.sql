CREATE FUNCTION warehouse.upsert_fwp_inventory_position() RETURNS void AS $$
begin
	
	--Insert item in locations where quantities has changed so we can see the trends 
	insert into warehouse.inventory_position
    select *, current_date trans_date from (
        select ipi.location_id, ipi.branch_description, ipi.item_group, ipi.item_id, ipi.item_desc, ipi.avg_fifo_cost, ipi.qty_on_hand, ipi.qty_allocated, ipi.qty_available
        from warehouse.inventory_position_incremental ipi
        except
        select ip.location_id, ip.branch_description, ip.item_group, ip.item_id, ip.item_desc, ip.avg_fifo_cost, ip.qty_on_hand, ip.qty_allocated, ip.qty_available 
        from warehouse.inventory_position ip
        ) t1;
	
END ;
$$
LANGUAGE plpgsql ;