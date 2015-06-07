SET LINESIZE 100
COLUMN spid FORMAT A10
COLUMN username FORMAT A10
COLUMN program FORMAT A45

SELECT s.inst_id,
       s.status,
       s.osuser,
       s.sql_address,
       s.sid,
       s.serial#,
       p.spid,
       s.username,
       s.machine,
       s.program,
       sql.sql_fulltext
FROM   gv$session s
       INNER JOIN gv$process p ON p.addr = s.paddr AND p.inst_id = s.inst_id
       INNER JOIN v$sql sql on s.sql_address = sql.address
WHERE  s.type != 'BACKGROUND'
AND s.USERNAME in('ERPT_MGR')
AND s.OSUSER in('millerbarr', 'MILLERBARR','SYSTEM')
AND s.PROGRAM in ('MSReportBuilder.exe','ReportingServicesService.exe')
ORDER BY OSUSER;