with TotalIndexProcessingTimeByDay AS 
(
	select
	bpl.StartDateTime,
	SUM(CAST(CAST(CAST(bptl.EndDateTime - bptl.StartDateTime As time) As DateTime) As float)) totalIndexProcessingTime
	from [SSISManager].audit.BatchPackageLog bpl
	inner join [SSISManager].audit.Package p on bpl.PackageID = p.PackageID
	inner join [SSISManager].audit.BatchLog bl on bpl.BatchLogID = bl.BatchLogID
	inner join [SSISManager].audit.BatchPackageTaskLog bptl on bpl.BatchPackageLogID = bptl.BatchPackageLogID
	where bpl.ParentBatchPackageLogID <> bpl.BatchPackageLogID
	and bpl.StartDateTime >= '2012-01-01'--Cast(GETDATE() as date)
	and bl.ApplicationGroup in ('BI TRA Cube Master Controller')
	and p.PackageName <>  'Tyson_BI_TRA_Cube_Master'
	and (sourceName like '%Process Indexes on%'
	or sourceName in ('Analysis Services Execute DDL Task - Process Index - DSI', 'Analysis Services Processing Task - Process Index on WSI MG', 'Analysis Services Execute DDL Task - Process Index AFG') )
	group by bpl.StartDateTime
	--order by bpl.StartDateTime
)
select
CAST(CAST(AVG(totalIndexProcessingTime) AS DateTime) AS Time) AS averageIndexProcessingTime
from TotalIndexProcessingTimeByDay