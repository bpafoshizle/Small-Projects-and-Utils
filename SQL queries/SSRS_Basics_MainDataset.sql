select 
dd.quarter_descr,
rp.business_division_code, 
rp.business_division_descr,
rp.business_group_code,
rp.business_group_descr,
rp.minor_line_code,
rp.minor_line_descr,
sum(mf.gross_sales_amount),
sum(mf.freight_amount)
from 
tdw_owner.restated_product rp,
tdw_owner.margin_fact mf,
tdw_owner.day_dim dd
where 
dd.fiscal_year_number in (2008) AND
dd.fiscal_month_number in (1, 2, 3) AND
rp.business_division_code in ('F01', 'C33', 'I99') AND
mf.invoice_day_key = dd.day_key AND
mf.restated_product_key = rp.restated_product_key
group by 
dd.quarter_descr,
rp.business_division_code,
rp.business_division_descr,
rp.business_group_code,
rp.business_group_descr,
rp.minor_line_code,
rp.minor_line_descr
order by 
dd.quarter_descr,
rp.business_division_code,
rp.business_division_descr,
rp.business_group_code,
rp.business_group_descr,
rp.minor_line_code,
rp.minor_line_descr