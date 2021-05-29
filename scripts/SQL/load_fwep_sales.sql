CREATE FUNCTION sales.load_fwep_sales() RETURNS void AS $$
begin
    DROP TABLE IF EXISTS sales.fwep_sales_data_merged as 