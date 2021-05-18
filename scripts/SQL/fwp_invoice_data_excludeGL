--row count
select * from sales.fwp_invoice_data
where gl_revenue_account_no = '29300000'

--total $
select sum(total_sales) from sales.fwp_invoice_data
where gl_revenue_account_no = '29300000' 

--total $ YTD
select sum(total_sales) from sales.fwp_invoice_data
where gl_revenue_account_no = '29300000' and invoice_date >= '2021-01-01'

--total $ PY
select sum(total_sales) from sales.fwp_invoice_data
where gl_revenue_account_no = '29300000' and invoice_date < '2021-01-01'

--delete
delete from sales.fwp_invoice_data
where gl_revenue_account_no = '29300000'

--execute stored proc
select sales.load_invoices()
