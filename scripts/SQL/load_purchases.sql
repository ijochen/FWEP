CREATE FUNCTION procurement.load_purchases() RETURNS void AS $$
BEGIN   
    DROP TABLE IF EXISTS procurement.purchases_orders_merged;
    select * 
    from (
        (select 
            company, 
            po_type,
            po_no,
            line_no,
            vendor_id,
            vendor_name,
            buyer,
            item_id,
            item_description,
            qty_ordered,
            qty_received,
            unit_price,
            ext_price,
            location_id,
            branch_id,
            order_date,
            receipt_date,
            promised_del_date
        from procurement.fwp_purchase_orders
        )
    UNION
        (SELECT
            company,
            PO_TYPE,
            PO_NUM,
            SEQ_NUM,
            VEND_NUM,
            VEND_DESC,
            buyer,
            PROD_NUM,
            PROD_DESC1,
            ORD_QTY,
            REC_QTY,
            GRS_COST,
            EXT_AMT,
            CENTRAL_WHSE_NUM,
            WHSE_NUM,
            PO_DATE,
            REC_DATE,
            DEL_DATE
        from procurement.pep_purchase_orders)
    ) a;
END;
$$
LANGUAGE plpgsql;