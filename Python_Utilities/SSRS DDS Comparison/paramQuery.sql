SELECT DISTINCT 
ftv.level_number || '|' || ftv.report_level || '|' || ftv.hier_type as level_numname
,ftv.report_level as level_name
,      ftv.level_number as level_num
,      ftv.hier_type as hier_type
FROM   TDW_OWNER.bus_div_fin_hier_tree_vw ftv
ORDER BY ftv.report_level
,        ftv.level_number
,               ftv.hier_type