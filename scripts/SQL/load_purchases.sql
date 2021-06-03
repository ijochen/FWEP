--DROP FUNCTION procurement.load_purchases()

CREATE FUNCTION procurement.load_purchases() RETURNS void AS $$
BEGIN   
    DROP TABLE IF EXISTS procurement.purchase_orders_merged;

    CREATE TABLE procurement.fwep_purchase_data_merged AS
    SELECT DISTINCT * 
    from (
        (SELECT DISTINCT * FROM procurement.fwp_purchase_orders)
    UNION
        (SELECT DISTINCT * FROM procurement.pep_purchase_orders)
    ) a;
END;
$$
LANGUAGE plpgsql;