 select a.owner,
       a.segment_name,
       a.alloc_mb,
       b.used_mb
from       
( select owner, segment_name, bytes/1024/1024 as alloc_mb
  from dba_segments ) a,
( select owner, table_name, round(((num_rows * avg_row_len)/1024/1024),2) as used_mb 
  from dba_tables ) b
where a.owner = b.owner
and a.segment_name = b.table_name
and a.owner = 'TDW_OWNER'
and a.segment_name = 'GROUP_DIM';