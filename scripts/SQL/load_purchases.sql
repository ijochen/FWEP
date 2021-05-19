--DROP FUNCTION procurement.load_purchases()

CREATE FUNCTION procurement.load_purchases() RETURNS void AS $$
BEGIN   
    DROP TABLE IF EXISTS procurement.purchase_orders_merged;

    CREATE TABLE procurement.purchase_orders_merged AS
    select * 
    from (
        (select * from procurement.fwp_purchase_orders)
    UNION
        (SELECT * from procurement.pep_purchase_orders)
    ) a;
END;
$$
LANGUAGE plpgsql;