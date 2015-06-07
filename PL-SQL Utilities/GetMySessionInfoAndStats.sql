select n.name, s.value
from v$statname n, v$sesstat s, v$session a
where n.statistic# = s.statistic#
and value > 0 
and s.sid = a.sid
and a.audsid = userenv('sessionid')
order by n.class, n.name;