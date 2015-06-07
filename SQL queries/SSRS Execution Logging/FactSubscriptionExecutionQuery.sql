DECLARE @Begin binary(10)
DECLARE @End binary(10)
declare @utcDateTime as datetime;
declare @localDateTime as datetime;
declare @minuteOffsetFromUTC as integer;
set @utcDateTime = getutcdate();
set @localDateTime = getdate();
set @minuteOffsetFromUTC = datediff(minute, @localDateTime, @utcDateTime);

SELECT @Begin = ISNULL(MAX(LastMaxLSNReportServer), 0)
FROM logging.PackageExecution
	 WHERE RunComplete = 1
	
SELECT @End = MAX(LastMaxLSNReportServer) 
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
		SELECT TOP 1 s_cdc.LastStatus
		FROM staging.Subscriptions_CDC s_cdc
		WHERE cdc.[EventData] = s_cdc.SubscriptionID
		and cdc.ContentStore = s_cdc.ContentStore 
		/* When a subscription fires, cdc.[TimeEntered] is always after the ModifiedDate is changed 
		on the subscription table. The most recent ModifiedDate on the Subscription_CDC
		table for a given subscription before the TimeEntered and with the highest start_LSN is guaranteed to have
		the correct LastStatus for that subscription launch */
		and cdc.[TimeEntered] >= DATEADD(minute, @minuteOffsetFromUTC, s_cdc.ModifiedDate) --convert s_cdc.ModifiedDate to UTC time
		ORDER BY s_cdc.ModifiedDate DESC, s_cdc.__$start_lsn DESC
      ) AS LastStatus
,cdc.[ContentStore]
FROM staging.Event_CDC cdc
	INNER JOIN DimSubscription s ON cdc.[EventData] = s.SubscriptionID and cdc.ContentStore = s.ContentStore
WHERE cdc.__$start_lsn > @Begin
                AND cdc.ContentStore = 'ReportServer'
	AND
	cdc.__$start_lsn <= @End
	AND __$operation = 1 --pull just the deletes as that will have the complete record from the event table when it was removed.