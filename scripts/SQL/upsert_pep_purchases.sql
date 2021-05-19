--DROP FUNCTION procurement.upsert_pep_purchases()

CREATE FUNCTION procurement.upsert_pep_purchases() RETURNS void AS $$
BEGIN

    --Delete
    delete from procurement.pep_purchase_orders
    where "PO_NUM" in (
        select "PO_NUM"
        from procurement.pep_purchase_orders_incremental
    );

    --Reinsert
    insert into procurement.pep_purchase_orders
    select * from procurement.pep_purchase_orders_incremental;

END;
$$
LANGUAGE plpgsql;

--SELECT procurement.upsert_pep_purchases();