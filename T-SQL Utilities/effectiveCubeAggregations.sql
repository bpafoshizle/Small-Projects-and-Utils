 SELECT distinct AGGREGATION_NAME,
	   SUBSTRING(aggregation_path,0,CHARINDEX('.',aggregation_path)) as SERVER_NAME,
	   SUBSTRING(aggregation_path,CHARINDEX('.',aggregation_path)+1,
	   CHARINDEX('.',aggregation_path,CHARINDEX('.',aggregation_path)+1)-
	   CHARINDEX('.',aggregation_path)-1) as DATABASE_NAME,
	   REVERSE(SUBSTRING(REVERSE(aggregation_path),CHARINDEX('.',REVERSE(aggregation_path))+1,
	   CHARINDEX('.',REVERSE(aggregation_path),CHARINDEX('.',REVERSE(aggregation_path))+1)-
	   CHARINDEX('.',REVERSE(aggregation_path))-1)) as MEASURE_GROUP,
	   SUM(f.AGGREGATION_COUNT) as AGG_HIT_CNT
  FROM [SSASLogging].[dbo].[AGGREGATION_FACT] f,
	   [SSASLogging].[dbo].[AGGREGATION_DIM] d
WHERE f.AGGREGATION_KEY = d.AGGREGATION_KEY
and f.DATE_KEY > 20110601
and SUBSTRING(aggregation_path,CHARINDEX('.',aggregation_path)+1,
	   CHARINDEX('.',aggregation_path,CHARINDEX('.',aggregation_path)+1)-
	   CHARINDEX('.',aggregation_path)-1) = 'Tyson Retail Analytics'
group by AGGREGATION_NAME,
	   SUBSTRING(aggregation_path,0,CHARINDEX('.',aggregation_path)),
	   SUBSTRING(aggregation_path,CHARINDEX('.',aggregation_path)+1,
	   CHARINDEX('.',aggregation_path,CHARINDEX('.',aggregation_path)+1)-
	   CHARINDEX('.',aggregation_path)-1),
	   REVERSE(SUBSTRING(REVERSE(aggregation_path),CHARINDEX('.',REVERSE(aggregation_path))+1,
	   CHARINDEX('.',REVERSE(aggregation_path),CHARINDEX('.',REVERSE(aggregation_path))+1)-
	   CHARINDEX('.',REVERSE(aggregation_path))-1))
order by AGG_HIT_CNT desc
