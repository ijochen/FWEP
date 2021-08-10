--DROP FUNCTION procurement.load_purchases()

CREATE FUNCTION procurement.load_fwep_purchases() RETURNS void AS $$
BEGIN   
    DROP TABLE IF EXISTS procurement.fwep_purchase_data_merged;

    CREATE TABLE procurement.fwep_purchase_data_merged AS
    SELECT DISTINCT * 
    from (
        (SELECT DISTINCT * FROM procurement.fwp_purchase_orders)
    UNION
        (SELECT DISTINCT * FROM procurement.pep_purchase_orders)
    ) a;

    DROP TABLE IF EXISTS aquacentral.fwep_purchase_data_merged;

    CREATE TABLE aquacentral.fwep_purchase_data_merged AS
    SELECT DISTINCT * 
    from (
        (SELECT DISTINCT * FROM procurement.fwp_purchase_orders)
    UNION
        (SELECT DISTINCT * FROM procurement.pep_purchase_orders)
    ) b;
END;
$$
LANGUAGE plpgsql;