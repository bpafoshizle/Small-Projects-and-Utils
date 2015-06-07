-- runas /user:tysonet\svc-SSRSLogging ssms.exe
-- pw: Lebagmij1
-- server: whqwpsqlvm06
-- database: ReportServer

/****** Get last 24 hours worth of executions for reports specified  ******/
SELECT c.Name,
	   els.[Parameters],
	   els.Format,	   
	   s.ExtensionSettings,
	   els.TimeStart
  FROM [ReportServer].[dbo].[ExecutionLogStorage] els
  INNER JOIN [Catalog] c on c.ItemId = els.ReportID
  INNER JOIN [Subscriptions] s on els.ReportID = s.Report_OID
  where Name in ('(BI_INVN_0001_A)Finished Goods Inventory', '(BI_INVN_0001_B)Finished Goods Inventory')
  and TimeStart > GETDATE()-1
  ORDER BY TimeStart DESC