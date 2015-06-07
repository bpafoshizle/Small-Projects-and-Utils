select 
   TO_CHAR(sys_context('USERENV','SESSIONID')) AS SESSION_ID
from dual;

select TO_CHAR(userenv('sessionid')) AS SESSION_ID from dual;