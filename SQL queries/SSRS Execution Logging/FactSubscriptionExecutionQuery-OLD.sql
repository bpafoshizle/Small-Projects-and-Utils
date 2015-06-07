DECLARE @Begin binary(10)
DECLARE @End binary(10)

SELECT @Begin = ISNULL(MAX(LastMaxLSN), 0)
FROM logging.PackageExecution
	 WHERE RunComplete = 1
	
SELECT @End = MAX(LastMaxLSN) 
FROM logging.PackageExecution
	 WHERE RunComplete = 0

SELECT cdc.[__$start_lsn]
      ,cdc.[__$seqval]
      ,cdc.[__$operation]
      ,cdc.[__$update_mask]
      ,cdc.[EventType]
      ,cdc.[EventID]
      ,CAST(cdc.[EventData] AS UNIQUEIDENTIFIER) AS [EventData]
      ,cdc.[TimeEntered]
      ,cdc.[ProcessStart]
      ,cdc.[ProcessHeartbeat]
      ,cdc.[BatchID]
      ,(
		SELECT TOP 1 re.ExecutionLogKey
		FROM FactReportExecution re
			INNER JOIN DimReport r ON re.ReportKey = r.ReportKey
		WHERE r.ReportKey = s.SubscribedReportKey
			AND cdc.[TimeEntered] >= re.TimeStart
		ORDER BY re.TimeStart ASC
      ) AS ExecutionLogKey -- It always sets the TimeStart in the ExecutionLogStorageTable before inserting into the Event table 
      ,(
		SELECT TOP 1 re.ReportKey
		FROM FactReportExecution re
			INNER JOIN DimReport r ON re.ReportKey = r.ReportKey
		WHERE r.ReportKey = s.SubscribedReportKey
			AND cdc.[TimeEntered] >= re.TimeStart
		ORDER BY re.TimeStart ASC
      ) ReportKey
      ,(
		SELECT TOP 1 s_cdc.LastStatus
		FROM staging.Subscriptions_CDC s_cdc
		WHERE cdc.[TimeEntered] >= s_cdc.ModifiedDate
		ORDER BY s_cdc.ModifiedDate ASC, s_cdc.__$start_lsn DESC
      ) AS LastStatus
,cdc.[ContentStore]
FROM staging.Event_CDC cdc
	INNER JOIN DimSubscription s ON cdc.[EventData] = s.SubscriptionID and cdc.ContentStore = s.ContentStore
WHERE cdc.__$start_lsn > @Begin
	AND
	cdc.__$start_lsn <= @End
	AND __$operation = 1 --pull just the deletes as that will have the complete record from the event table when it was removed.