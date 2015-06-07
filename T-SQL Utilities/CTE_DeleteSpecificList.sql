WITH problematicShipTos AS
(
	SELECT sim_ship_to_num
	FROM [SIM_UNRAVEL_MD].[dbo].[SHIP_TO_VIEW_AS_IS_ALL]
	GROUP BY sim_ship_to_num
	having COUNT(*) > 3
),
linesToDelete AS (
select 
	SHIP_TO_VIEW_AS_IS_ALL_key 			
	from SHIP_TO_VIEW_AS_IS_ALL s
	inner join problematicShipTos p
		on s.sim_ship_to_num = p.SIM_SHIP_TO_NUM
	where eff_begin_date = '2011-12-13 00:00:00.000'
)
delete s
from dbo.SHIP_TO_VIEW_AS_IS_ALL s
inner join linesToDelete d
on s.SHIP_TO_VIEW_AS_IS_ALL_key = d.SHIP_TO_VIEW_AS_IS_ALL_key