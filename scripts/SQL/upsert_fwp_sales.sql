CREATE FUNCTION sales.upsert_fwp_sales() RETURNS void AS $$
BEGIN

    --Delete
    delete from sales.fwp_sales
    where invoice_no in (
        select invoice_no
        from sales.fwp_sales_incremental
    );

    --Reinsert
    insert into sales.fwp_sales
    select * from sales.fwp_sales_incremental;

END;
$$
LANGUAGE plpgsql;