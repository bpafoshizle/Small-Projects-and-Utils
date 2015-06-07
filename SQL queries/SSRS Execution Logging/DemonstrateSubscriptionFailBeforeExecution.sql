DECLARE @Begin binary(10);
DECLARE @End binary(10);
declare @utcDateTime as datetime;
declare @localDateTime as datetime;
declare @minuteOffsetFromUTC as integer;
set @utcDateTime = getutcdate();
set @localDateTime = getdate();
set @minuteOffsetFromUTC = datediff(minute, @localDateTime, @utcDateTime);

SELECT @Begin = ISNULL(MAX(LastMaxLSN), 0)
FROM logging.PackageExecution
	 WHERE RunComplete = 1
	
SELECT @End = MAX(LastMaxLSN) 
FROM logging.PackageExecution
	 WHERE RunComplete = 0

select
s.SubscribedReportKey,
cdc.[ProcessStart],
(
		SELECT TOP 1 re.ExecutionLogKey
		FROM staging.FactReportExecution re
			INNER JOIN DimReport r ON re.ReportKey = r.ReportKey
		WHERE r.ReportKey = s.SubscribedReportKey
			  AND DATEADD(minute, @minuteOffsetFromUTC, re.TimeStart) >= cdc.[ProcessStart]
			  AND DATEADD(minute, @minuteOffsetFromUTC, re.TimeStart) < DATEADD(millisecond, 500, cdc.[ProcessStart])
			--AND cast(SWITCHOFFSET(cast(re.TimeStart as DATETIMEOFFSET), @minuteOffsetFromUTC) as datetime) >= cdc.[ProcessStart] 
			--AND cast(SWITCHOFFSET(cast(re.TimeStart as DATETIMEOFFSET), @minuteOffsetFromUTC) as datetime) < dateadd(ms, 500, cdc.[ProcessStart]) 
		ORDER BY re.TimeStart ASC
      ) AS ExecutionLogKey
FROM staging.Event_CDC cdc
	INNER JOIN DimSubscription s ON cdc.[EventData] = s.SubscriptionID and cdc.ContentStore = s.ContentStore
WHERE cdc.__$start_lsn > @Begin
	AND
	cdc.__$start_lsn <= @End
	AND __$operation = 1 --pull just the deletes as that will have the complete record from the event table when it was removed.
order by TimeEntered ASC

select DATEADD(minute, @minuteOffsetFromUTC, TimeStart) as utc_time_start,
--cast(SWITCHOFFSET(cast(TimeStart as DATETIMEOFFSET), @minuteOffsetFromUTC) as datetime) as utc_time_start,
* from staging.FactReportExecution
where TimeStart > '10-08-2009 00:00:00'
order by TimeStart ASC