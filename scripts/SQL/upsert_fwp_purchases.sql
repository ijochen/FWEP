CREATE FUNCTION procurement.upsert_fwp_purchases() RETURNS void AS $$
BEGIN

    --Delete
    delete from procurement.fwp_purchase_orders
    where po_no in (
        select po_no
        from procurement.fwp_purchase_order_incremental
    );

    --Reinsert
    insert into procurement.fwp_purchase_orders
    select * from procurement.fwp_purchase_order_incremental;

END;
$$
LANGUAGE plpgsql;