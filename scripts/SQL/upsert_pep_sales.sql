CREATE FUNCTION sales.upsert_pep_sales() RETURNS void AS $$
BEGIN

    --Delete
    delete from sales.pep_sales
    where "INV_NUM" in (
        select "INV_NUM"
        from sales.pep_sales_incremental
    );

    --Reinsert
    insert into sales.pep_sales
    select distinct * from sales.pep_sales_incremental;

END;
$$
LANGUAGE plpgsql;