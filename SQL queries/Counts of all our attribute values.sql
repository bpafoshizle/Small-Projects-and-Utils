select count(*) from restated_market rm
where rm.usage_in_fact_flag = 'Y'; --34,152

select count(distinct rm.selling_group_code) from restated_market rm
where rm.usage_in_fact_flag = 'Y'; -- 177

select count (distinct rp.minor_line_code) from restated_product rp
where rp.usage_in_fact_flag = 'Y'; --571 

select count (distinct rp.sap_product_brand_code) from restated_product rp
where rp.usage_in_fact_flag = 'Y'; -- 33,575

select count(distinct il.inventory_location_code) from inventory_location il; -- 1,545

select count(distinct nrp.product_part_primal_descr) from TDW_OWNER.non_restated_product nrp; -- 178


select count(distinct nrp.product_code) from TDW_OWNER.non_restated_product nrp; -- 46,997

select count(distinct bdd.business_division_code) from tdw_owner.restated_mkt_prd_bus_div_dim bdd; -- 337

select count(distinct soa.so_type_code) from TDW_OWNER.dim_sales_order_attr soa; -- 15 