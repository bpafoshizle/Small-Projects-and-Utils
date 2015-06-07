SET NOCOUNT ON 
SELECT size / 128.0 as fileSize, 
 file_id as fileId,
 FILEPROPERTY(name, 'SpaceUsed') / 128.0 as fileUsed,
 CASE WHEN max_size = -1 OR max_size = 268435456 THEN -1 ELSE max_size / 128 END as fileMaxSize,
 CASE WHEN growth = 0 THEN 0 ELSE 1 END as IsAutoGrow, 
 is_percent_growth as isPercentGrowth, 
 growth as fileGrowth, 
 SUBSTRING(physical_name,1,1) as drive 
 FROM sys.database_files where type IN (0,1) and is_read_only = 0