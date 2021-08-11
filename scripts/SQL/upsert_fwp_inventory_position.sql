CREATE FUNCTION warehouse.upsert_fwp_inventory_position() RETURNS void AS $$
begin
	
	--Insert item in locations where quantities has changed so we can see the trends 
	insert into warehouse.inventory_position
    select * from (
        select * from warehouse.inventory_position_incremental
        except
        select * from warehouse.inventory_position
        ) t1;
	
END ;
$$
LANGUAGE plpgsql ;