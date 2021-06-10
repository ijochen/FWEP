CREATE FUNCTION sales.load_fwep_sales() RETURNS void AS $$
BEGIN
    DROP TABLE IF EXISTS sales.fwep_sales_data_merged;
    
    CREATE TABLE sales.fwep_sales_data_merged AS
    SELECT DISTINCT * 
    FROM (
        (SELECT DISTINCT
            company,
            customer_id,
            customer_name,
            customer_city,
            customer_state,
            order_date,
            order_no,
            line_no,
            prod_num,
            prod_desc,
            prod_group,
            qty_ordered,
            qty_shipped,
            unit_price,
            extended_price,
            profit_amount, 
            cogs_amount, 
            tax_amount,
            paid_amount,
            invoice_amount,
            invoice_date,
            invoice_no,
            gl_revenue_account_no,
            sales_location_id,
            sales_location_name,
            branch_id,
            branch_location_name,
            month_name
        FROM sales.fwp_sales
        )

        UNION

        (SELECT DISTINCT
            "company",
            "CUST_NUM" customer_id,
            "CUST_DESC" customer_name,
            "CUST_CITY" customer_city,
            "CUST_STATE" customer_state,
            "ORD_DATE" order_date,
            "ORD_NUM" order_no,
            "LINE_NUM" line_no,
            "PROD_NUM" prod_num,
            "PROD_DESC" prod_desc,
            "PROD_GROUP" prod_group,
            "ORD_QTY" qty_ordered,
            "SHP_QTY" qty_shipped,
            "NET_PRICE" unit_price,
            "LINE_EXT" extended_price,
            "PROFIT_EXT" profit_amount, 
            "NET_COST_EXT" cogs_amount, 
            "TAX" tax_amount,
            "TOT_ORD_DOL" amount_paid,
            "INV_AMT" invoice_amount,
            "INV_DATE" invoice_date,
            "INV_NUM" invoice_no,
            "GL_ACCOUNT_NUM" gl_revenue_account_no,
            "SEL_WHSE" sales_location_id,
            "SEL_WHSE_NAME" sales_location_name,
            "WHSE_NUM" branch_id,
            "WHSE_NAME" branch_location_name,
            "MONTH_NAME" month_name
        FROM sales.pep_sales
        )
    ) a;
END;
$$
LANGUAGE plpgsql;