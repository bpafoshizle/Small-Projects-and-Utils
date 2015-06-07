DECLARE @oldAvgAvailableTime time, @newAvgAvailableTime time

SELECT
      @oldAvgAvailableTime = CAST(CAST(AVG(CAST(CAST(CAST(EndTime As time) As DateTime) As float)) As DateTime) As time)
  FROM [TRA_STG_MSS].[JA].[JobDailyControl]
  WHERE AsOfRunDayName not in ('Saturday', 'Friday')
  AND AsOfRunDate >= '2011-07-01'
  AND AsOfRunDate < '2011-11-01';
  
SELECT
      @newAvgAvailableTime = CAST(CAST(AVG(CAST(CAST(CAST(EndTime As time) As DateTime) As float)) As DateTime) As time)
  FROM [TRA_STG_MSS].[JA].[JobDailyControl]
  WHERE AsOfRunDayName not in ('Saturday', 'Friday')
  AND AsOfRunDate >= '2011-11-01'
  AND AsOfRunDate < GETDATE();

DECLARE @floatOldTime float, @floatNewTime float
select @floatOldTime = CAST(CAST(@oldAvgAvailableTime AS datetime) As float), @floatNewTime = CAST(CAST(@newAvgAvailableTime As datetime) As Float)

select @oldAvgAvailableTime as [Previous Avg Available Time], @newAvgAvailableTime as [Current Avg Available Time], ROUND(((@floatNewTime-@floatOldTime)/@floatOldTime)*100, 0) As PercentDiff
