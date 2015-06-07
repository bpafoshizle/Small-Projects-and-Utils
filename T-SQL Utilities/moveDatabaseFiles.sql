use master 
	go
	sp_detach_db 'TSN_ORA_DW_SMALL'
	go
use master
  go
  sp_attach_db 'TSN_ORA_DW_SMALL','C:\Program Files\Microsoft SQL Server\MSSQL10_50.ISDEV\MSSQL\DATA\TSN_ORA_DW_SMALL.mdf','D:\MSSQL_Logs\TSN_ORA_DW_SMALL_log.ldf'
  go