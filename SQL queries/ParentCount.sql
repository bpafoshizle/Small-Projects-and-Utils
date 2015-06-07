select distribution_channel_code as dc,
selling_group_code,
selling_group_descr,
count(distinct sales_grp_code) as a_parent_count
from
(select
bdx.distribution_channel_code, 
bdm.sales_grp_code,
rm.selling_group_code,
rm.selling_group_descr
from tdw_owner.business_division_master bdm
inner join TDW_OWNER.business_division_xref bdx on bdm.business_division_code = bdx.business_division_code
inner join TDW_OWNER.selling_group_sap_xref sgx on sgx.distribution_channel_code = bdx.distribution_channel_code
inner join (select distinct rm.selling_group_code, rm.selling_group_descr from TDW_OWNER.restated_market rm) rm on sgx.selling_group_code = rm.selling_group_code
order by bdx.distribution_channel_code)
group by distribution_channel_code,
selling_group_code,
selling_group_descr
having count(distinct sales_grp_code) > 1;