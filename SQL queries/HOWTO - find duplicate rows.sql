select t1.* from SAP_BSAD_EXTRACT t1
INNER JOIN (
select sde.BELNR_SAPDocNum,
count(sde.BELNR_SAPDocNum) as docCount
from SAP_BSAD_EXTRACT sde 
group by sde.BELNR_SAPDocNum
having (COUNT(sde.BELNR_SAPDocNum) > 1))
AS t2
ON t1.BELNR_SAPDocNum = t2.BELNR_SAPDocNum
ORDER BY t1.BELNR_SAPDocNum, GJAHR_FiscalYear




select
trim(dmc.code_val) AS CODE_VAL,
count(trim(dmc.code_val)) as code_count 
from tds_owner.dim_mstr_code dmc
where dmc.code_column_name in ('SHIPMENT_LOAD_TYPE_CODE', 'UNKNOWN')
group by trim(dmc.code_val) 
having count(trim(dmc.code_val)) > 1
order by trim(code_val);