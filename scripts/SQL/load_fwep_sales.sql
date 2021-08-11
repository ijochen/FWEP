CREATE FUNCTION sales.load_fwep_sales() RETURNS void AS $$
BEGIN
    DROP TABLE IF EXISTS sales.fwep_sales_data_merged;
    
    CREATE TABLE sales.fwep_sales_data_merged AS
    SELECT DISTINCT * 
    FROM (
        (SELECT DISTINCT * FROM sales.fwp_sales)
        UNION
        (SELECT DISTINCT * FROM sales.pep_sales)
    ) a;

    DROP TABLE IF EXISTS aquacentral.fwep_sales_data_merged;
    
    CREATE TABLE aquacentral.fwep_sales_data_merged AS
    SELECT DISTINCT * 
    FROM (
        (SELECT DISTINCT * FROM sales.fwp_sales)
        UNION
        (SELECT DISTINCT * FROM sales.pep_sales)
    ) b;

END;
$$
LANGUAGE plpgsql;