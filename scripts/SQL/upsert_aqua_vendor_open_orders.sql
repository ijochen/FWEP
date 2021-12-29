
-- save the interim as the original file, incase we need to revert back, use the pristine table as the table for Tableau
-- SELECT * INTO procurement.vendor_open_orders
-- FROM procurement.vendor_open_orders_interim

-- DROP FUNCTION procurement.upsert_aqua_vendor_open_orders()

CREATE FUNCTION procurement.upsert_aqua_vendor_open_orders()  RETURNS void AS $$
BEGIN
	
	--delete past vendor open orders data because we only want to see the latest updates
    DELETE FROM procurement.vendor_open_orders 
    WHERE vendor IN (
        SELECT vendor 
        FROM procurement.vendor_open_orders_incremental 
    );
	
	-- Reinsert 
	INSERT INTO sales.fwp_invoice_data 
	SELECT * FROM sales.fwp_invoice_data_incremental;
	
END ;
$$
LANGUAGE plpgsql ;

-- SELECT procurement.upsert_aqua_vendor_open_orders()