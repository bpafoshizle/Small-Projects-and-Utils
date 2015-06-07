declare @utcDateTime as datetime;
declare @localDateTime as datetime;
declare @hourOffsetFromUTC as integer;
set @utcDateTime = getutcdate();
set @localDateTime = getdate();
set @hourOffsetFromUTC = datediff(hour, @localDateTime, @utcDateTime);

select cast(SWITCHOFFSET(cast('2009-10-08 14:41:13.827' as DATETIMEOFFSET), @hourOffsetFromUTC * 60) as datetime)