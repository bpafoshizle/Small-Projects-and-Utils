CREATE PROCEDURE "PTT_APP"."PTT.sp::LIFEBOAT" (IN order_create_date varchar(25),
												IN planned_pgi_days_low varchar(25), IN planned_pgi_days_high varchar(25),
												IN billed_ind varchar(25),
												IN pgi_status varchar(25),
												IN multi_delv varchar(50),											
												IN rejected_ind varchar(25),
												IN SAP_DELV_RELEVANT_ORDER_TYPE_IND varchar(25),
												IN has_delivery_ind varchar(25),
												IN TM_LOAD_DATE_MISMATCH_IND varchar(25),
												IN days_until_load_date_low varchar(25), IN days_until_load_date_high varchar(25),
												IN sap_planning_status varchar(25),
												IN multi_fu_single_plant_ind varchar(25),
												IN has_shipment_ind varchar(25),
												IN sap_concept_cust_desc varchar(35),
												IN business_unit varchar(30),
												IN confirmed_qty_discrep_ind varchar(25),
												IN sap_billing_date_start varchar(10),
												IN sap_billing_date_end varchar(10),
												IN MULTI_DELV_DATE_SINGLE_PLANT_IND varchar(50),
												IN MULTI_LOAD_DATE_SINGLE_PLANT_IND varchar(50),
												IN MULTI_ROUTE_SINGLE_PLANT_IND varchar(50),
												IN MULTI_PLANT_FOR_NON_REJECTED_LINES_IND varchar(50),
												IN MISSING_PLANT_FOR_NON_REJECTED_LINE_IND varchar(50),
												IN order_fuzzy_week_start varchar(25),
												IN order_fuzzy_week_end varchar(25),
												IN planned_pgi_date_start varchar(10),
												IN planned_pgi_date_end varchar(10)
												)
	LANGUAGE SQLSCRIPT
	SQL SECURITY INVOKER
	DEFAULT SCHEMA PTT_OWNER
	READS SQL DATA
AS
BEGIN

--drop procedure "PTT_APP"."PTT.sp::LIFEBOAT";
--call "PTT_APP"."PTT.sp::LIFEBOAT"('2015-10-11', 'NA', '-2', 'Not Billed', 'Not PGIed', 'NA', 'Not Rejected', 'Delv Relevant', 'Has Delivery', 'NA','NA','NA','NA', 'NA', 'NA','NA','NA','NA','NA','NA','NA','NA','NA','NA', 'NA', 'NA','NA', 'NA','NA')	

with partnerNums as (
	select
		p.VBELN as SAP_SALES_ORDER_NUM,
		p.POSNR as SAP_LINE_ITEM_NUM,
		max(map(p.PARVW,'AG',p.KUNNR,NULL)) as SAP_SOLD_TO,
		max(map(p.PARVW,'RG',p.KUNNR,NULL)) as SAP_PAYER,
		max(map(p.PARVW,'WE',p.KUNNR,NULL)) as SAP_SHIP_TO,
		max(map(p.PARVW,'1D',p.KUNNR,NULL)) as SAP_CONCEPT_CUSTOMER,
		max(map(p.PARVW,'Z0',p.KUNNR,NULL)) as SAP_CSR,
		max(map(p.PARVW,'RT',p.KUNNR,NULL)) as SAP_ROUTE,
		max(map(p.PARVW,'ZP',p.KUNNR,NULL)) as SAP_SALES_POSITION
	from ECCSLT.VBPA p
	group by p.VBELN,
	p.POSNR
)
,partners as (
	select
		sap_sales_order_num,
		sap_line_item_num,
		sap_sold_to,
		soldto.Name1 as SAP_SOLD_TO_DESC,
		sap_payer,
		payer.Name1 as SAP_PAYER_DESC,
		sap_ship_to,
		shipto.Name1 as SAP_SHIP_TO_DESC,
		sap_concept_customer,
		cc.Name1 as SAP_CONCEPT_CUST_DESC,
		sap_csr,
		csr.Name1 as SAP_CSR_DESC,
		sap_route,
		rte.name1 as sap_route_desc,
		sap_sales_position,
		spos.name1 as sap_sales_position_desc
	from partnerNums p
	left join ECCSLT.KNA1 soldto on soldto.kunnr = p.SAP_SOLD_TO
	left join ECCSLT.KNA1 shipto on shipto.kunnr = p.SAP_SHIP_TO
	left join ECCSLT.KNA1 payer on payer.kunnr = p.SAP_PAYER
	left join ECCSLT.KNA1 cc on cc.kunnr = p.SAP_CONCEPT_CUSTOMER
	left join ECCSLT.KNA1 csr on csr.kunnr = p.SAP_CSR
	left join ECCSLT.KNA1 rte on rte.kunnr = p.SAP_ROUTE
	left join ECCSLT.KNA1 spos on spos.kunnr = p.SAP_SALES_POSITION
)
,partnerHeader as (
       select
              *
       from partners
       where SAP_LINE_ITEM_NUM = '000000'
)
,prices as (
	select  
		SAP_DTL.VBELN as sap_sales_order_num,
		SAP_DTL.POSNR as sap_line_item_num,
		sum(konv.KWERT) as sap_line_net_value
	FROM    ECCSLT.VBAK SAP_HDR
	INNER JOIN    ECCSLT.VBAP SAP_DTL on (SAP_HDR.VBELN = SAP_DTL.VBELN)
	LEFT JOIN ECCSLT.KONV KONV
			ON SAP_HDR.KNUMV = KONV.KNUMV
			AND SAP_DTL.POSNR = KONV.KPOSN
			AND KONV.KSCHL in ('YLPF','YLPD','YLST','YLDL','YVL5','YVL4','YVP5','YVP4','YVB4','YVB3','YVT2','YVT1','YVSC','YLMN','YLMD',   -- List Prices
					 'YVD6', 'YVD7', 'YVD8', 'YVDP', 'YVS1', 'YVS2', 'YVS3', -- Discounts
					 'YDPL', 'YHPF', 'YSAO', 'YDSS', 'YDLU', 'YHPU', 'YDPU', 'YDSA', 'YHFT', 'YFRT', 'YHAF', 'YSIF', 'YDFR', -- Allowances
                    'YHHF', 'YDSW', 'YDDA', 'YSDR', 'YHFA', 'YDFA', 'YSFC', 'YDSP', 'YHTG', 'YSTG', 'YDCD', 'YHHC', 'YSSH', 'YSWA')
	      AND KONV.KINAK = ''
	group by SAP_DTL.VBELN,
		SAP_DTL.POSNR
)
,sapPODocflow as (
	select
		docFlow.VBELN as SAP_PO_NUM,
		docFlow.POSNN as SAP_PO_LINE_ITEM_NUM,
		docFlow.VBELV as SAP_SALES_ORDER_NUM,
		docFlow.POSNV as SAP_LINE_ITEM_NUM	
	from eccslt.vbfa docFlow
	where docFlow.VBTYP_N = 'V'
)
,sapDeliveryDocFlow as (
	select
		docFlow.VBELN as SAP_DELV_NUM,
		docFlow.POSNN as SAP_DELV_LINE,
		docFlow.VBELV as SAP_SALES_ORDER_NUM,
		docFlow.POSNV as SAP_LINE_ITEM_NUM
	from eccslt.vbfa docFlow
	where docFlow.VBTYP_N = 'J'
	and left(docFlow.POSNN, 1) <> '9'
)
,sapShipmentDocFlow as (
	select
		docFlow.VBELN as SAP_SHIPMENT_NUM,
		docFlow.VBELV as SAP_DELV_NUM,
		docFlow.POSNV as SAP_DELV_LINE
	from eccslt.vbfa docFlow
	LEFT JOIN ECCSLT.VBUK status
		on docFlow.VBELN = status.VBELN
	where docFlow.VBTYP_N = '8'
	group by docFlow.VBELN, docFlow.VBELV, docFlow.POSNV
)
,sapBillingDocFlow as (
	select
		docFlow.VBELN as SAP_BILLING_NUM,
		cast(docFlow.ERDAT as date) as SAP_BILLING_DATE,
		--cast(max(cast(docFlow.VBELN as int)) as varchar) as SAP_BILLING_NUM,
		--max(cast(docFlow.ERDAT as date)) as SAP_BILLING_DATE,
		docFlow.VBELV as SAP_ORDER_OR_DELV_NUM--,
		--docFlow.POSNV as SAP_ORDER_OR_DELV_LINE
	from eccslt.vbfa docFlow
	LEFT JOIN ECCSLT.VBUK status
		on docFlow.VBELN = status.VBELN
	where docFlow.VBTYP_N = 'M'
	--and docFlow.POSNN = '000010'
	group by docFlow.VBELN, docFlow.ERDAT, docFlow.VBELV--, docFlow.POSNV
)
,sapPickingDocFlow as (
	select
		docFlow.VBELN as PICK_NUM,
		docFlow.ERDAT as PICK_DATE,
		--docFlow.ERZET as PICK_TIME,
		docFlow.VBELV as SAP_DELV_NUM--,
		--docFlow.POSNV as SAP_DELV_LINE
	from eccslt.vbfa docFlow
	LEFT JOIN ECCSLT.VBUK status
		on docFlow.VBELN = status.VBELN
	where docFlow.VBTYP_N = 'Q'
	--and left(docFlow.POSNN, 1) <> '9'
	--and left(docFlow.posnv, 1) <> '9'
	group by docFlow.VBELN, docFlow.VBELV, docFlow.ERDAT--, docFlow.POSNV
)
,sapPGIDocFlow as (
	select
		docFlow.VBELN as PGI_NUM,
		docFlow.ERDAT as PGI_DATE,
		docFlow.ERZET as PGI_TIME,
		docFlow.VBELV as SAP_DELV_NUM
	from eccslt.vbfa docFlow
	LEFT JOIN ECCSLT.VBUK status
		on docFlow.VBELN = status.VBELN
	where docFlow.VBTYP_N = 'R'
	group by docFlow.VBELN, docFlow.ERDAT, docFlow.ERZET, docFlow.VBELV
)
,listPrices as (
	select  
		SAP_DTL.VBELN as sap_sales_order_num,
		SAP_DTL.POSNR as sap_line_item_num,
		sum(konv.KWERT) as sap_line_list_value
	FROM    ECCSLT.VBAK SAP_HDR
	INNER JOIN    ECCSLT.VBAP SAP_DTL on (SAP_HDR.VBELN = SAP_DTL.VBELN)
	LEFT JOIN ECCSLT.KONV KONV
			ON SAP_HDR.KNUMV = KONV.KNUMV
			AND SAP_DTL.POSNR = KONV.KPOSN
			AND KONV.KSCHL in ('YLPF','YLPD','YLST','YLDL','YVL5','YVL4','YVP5','YVP4','YVB4','YVB3','YVT2','YVT1','YVSC','YLMN','YLMD')   -- List Prices
	      AND KONV.KINAK = ''
	group by SAP_DTL.VBELN,
		SAP_DTL.POSNR
)
,sapTransDates as (
       select
              del.vbeln as sap_sales_order_num,
              del.posnr as sap_line_item_num,              
              del.vrkme as sap_line_order_qty_uom,
              del.banfn as sap_purchase_req_num,
              del.bnfpo as sap_purchase_req_item_num,
              sum(del.wmeng) as order_qty,
              sum(del.bmeng) as confirmed_qty,
              max(case when coalesce(del.wmeng, 0) <> 0 then del.edatu else to_date('1900-01-01') end) as sap_ordered_line_delv_date,
              max(case when coalesce(del.bmeng, 0) <> 0 then del.edatu else to_date('1900-01-01') end) as sap_confirmed_line_delv_date,
              coalesce(
                     max(case when coalesce(del.bmeng, 0) <> 0 then del.edatu else null end),
                     --max(case when coalesce(del.wmeng, 0) <> 0 then del.edatu else null end), 
                     max(del.edatu))      
              as sap_line_delv_date,
              max(case when coalesce(del.wmeng, 0) <> 0 then del.lddat else to_date('1900-01-01') end) as sap_ordered_line_load_date,
              max(case when coalesce(del.bmeng, 0) <> 0 then del.lddat else to_date('1900-01-01') end) as sap_confirmed_line_load_date,
              coalesce(
                     max(case when coalesce(del.bmeng, 0) <> 0 then del.lddat else null end),
                     --max(case when coalesce(del.wmeng, 0) <> 0 then del.lddat else null end),
                     max(del.lddat))
              as sap_line_load_date,                            
              max(case when coalesce(del.wmeng, 0) <> 0 then del.lduhr else to_date('000000') end) as sap_ordered_line_load_hour,
              max(case when coalesce(del.bmeng, 0) <> 0 then del.lduhr else to_date('000000') end) as sap_confirmed_line_load_hour,
              coalesce(
                     max(case when coalesce(del.bmeng, 0) <> 0 then del.lduhr else null end),
                     max(del.lduhr))
              as sap_line_load_hour,                                          
              max(case when coalesce(del.wmeng, 0) <> 0 then del.mbdat else to_date('1900-01-01') end) as sap_ordered_line_mtrl_avail_date,
              max(case when coalesce(del.bmeng, 0) <> 0 then del.mbdat else to_date('1900-01-01') end) as sap_confirmed_line_mtrl_avail_date,
              coalesce(
                     max(case when coalesce(del.bmeng, 0) <> 0 then del.mbdat else null end),
                     --max(case when coalesce(del.wmeng, 0) <> 0 then del.lddat else null end),
                     max(del.mbdat))
              as sap_line_mtrl_avail_date,
              max(case when coalesce(del.wmeng, 0) <> 0 then del.wadat else to_date('1900-01-01') end) as sap_ordered_planned_pgi_date,
              max(case when coalesce(del.bmeng, 0) <> 0 then del.wadat else to_date('1900-01-01') end) as sap_confirmed_planned_pgi_date,
              cast(coalesce(
                     max(case when coalesce(del.bmeng, 0) <> 0 then del.wadat else null end),
                     --max(case when coalesce(del.wmeng, 0) <> 0 then del.lddat else null end),
                     max(del.wadat)) as date)
              as sap_line_planned_pgi_date,
              count(*) as sched_line_count
       from eccslt.vbep del
       group by del.vbeln, del.posnr, del.vrkme, del.banfn, del.bnfpo
)
,sapDelivery as (
	select
		hdr.vbeln as sap_delv_num,
		dtl.vgbel as sap_sales_order_or_po_num,
		dtl.vgpos as sap_line_item_num,
		hdr.erdat as sap_delv_create_date,
		hdr.erzet as sap_delv_create_time,
		hdr.lddat as sap_delv_load_date,
		hdr.lfdat as sap_delv_req_date,
		cast(hdr.wadat as date) as PLANNED_PGI_DATE,
		cast(hdr.wadat_ist as date) as PGI_DATE,
		--pgidf.PGI_TIME,
		status.GBSTK as OVERALL_DELIVERY_STATUS, -- A means open deliveries
		case when status.WBSTK = 'C' or coalesce(hdr.wadat_ist, '00000000') <> '00000000' then 'PGIed' else 'Not PGIed' end as PGI_STATUS, -- PGI_STATUS -- C means PGIed
		hdr.ZZPICKDATE as PICK_DATE_Z,
		pdf.PICK_DATE as PICK_DATE,
		case when coalesce(pdf.PICK_DATE, '00000000') <> '00000000' then 'Picked' else 'Not Picked' end as PICK_STATUS, -- A is available for picking, aka not picked. C is complete/aka picked.
		--case when coalesce(hdr.ZZPICKDATE, '00000000') <> '00000000' or coalesce(pdf.PICK_DATE, '00000000') <> '00000000' then 'Picked' else 'Not Picked' end as PICK_STATUS, -- A is available for picking, aka not picked. C is complete/aka picked.
		status.CMGST as SAP_DELV_CREDIT_BLOCK_STATUS,
		dtl.PODREL as sap_delv_pod_flag,
		hdr.PODAT as sap_delv_pod_date,
		case when coalesce(hdr.PODAT, '00000000') <> '00000000' then 'POD Received' else 'POD Not Received' end as POD_STATUS,
		sum(dtl.lfimg) as sap_delv_qty
	from eccslt.lips dtl
	inner join eccslt.likp hdr
		on dtl.vbeln = hdr.vbeln
	LEFT JOIN ECCSLT.VBUK status
		on hdr.VBELN = status.VBELN
	left join sapPickingDocFlow pdf
		on hdr.VBELN = pdf.SAP_DELV_NUM
		--and dtl.posnr = pdf.SAP_DELV_LINE
	--left join sapPGIDocFlow pgidf
	--	on hdr.VBELN = pgidf.SAP_DELV_NUM
	group by
		hdr.vbeln,
		dtl.vgbel,
		dtl.vgpos,
		hdr.erdat,
		hdr.erzet,
		hdr.lddat,
		hdr.lfdat,
		hdr.wadat,
		hdr.wadat_ist,
		hdr.ZZPICKDATE,
		pdf.PICK_DATE,
		status.GBSTK,
		status.WBSTK,
		status.KOSTK,
		status.CMGST,
		dtl.PODREL,
		hdr.PODAT
	order by dtl.vgbel, dtl.vgpos
)
,FU_to_FO as (
       select distinct 
             ite.FU_ROOT_KEY,
             ite.PARENT_KEY,
             hdr.TOR_CAT,
             hdr.TOR_TYPE,
             hdr.TOR_ID as FO_NUM,
             hdr.ZZCOST_LCL as SAP_TM_STANDARD_FRT_COST_LOCAL,
             hdr.ZZCOST_DOCUM as SAP_TM_STANDARD_FRT_COST_DOC,
             hdr.TSP_SCAC as SAP_TM_CARRIER,
             hdr.lifecycle as SAP_TM_LIFECYCLE,        
			 hdr.ZZSTDFRTSTL as SAP_TM_FRT_COST_STATUS,
             cast(ZZDDT as date) as SAP_TM_PLANNED_PGI_DATE,
             cast(ZZPGI_TIMESTAMP as timestamp) as SAP_TM_FO_PGI_DATE
       from "TMSLT"."/SCMTMS/D_TORITE" ite
       inner join "TMSLT"."/SCMTMS/D_TORROT" hdr
             on ite.PARENT_KEY = hdr.DB_KEY
             and hdr.TOR_CAT = 'TO'
       where ite.LOCAL_ITEM = 'R'
       and hdr.lifecycle <> '10'
)
,tmOtrConsumptionStatus as (
	select
		base_btd_id as sap_padded_sales_order_num,
		trq_type as otr_type,
		--01=Not Consumed, 02=Consumed Partially, 03=Consumed Completely
		map(CONSUMPTION, '01', 'Not Consumed', '02', 'Partially Consumed', '03', 'Consumed', CONSUMPTION) as OTR_CONSUMPTION_STATUS
	from "TMSLT"."/SCMTMS/D_TRQROT"
)
,tmShipmentTimeStatus as (
	select
		hdr.DB_KEY as FU_ID,
		hdr.TOR_ID as FU_NUM,
		hdr.TOR_TYPE as FU_TYPE,
		fufo.FU_ROOT_KEY,
		fufo.TOR_CAT,
		fufo.TOR_TYPE,
		fufo.FO_NUM,
		fufo.SAP_TM_STANDARD_FRT_COST_LOCAL,
        fufo.SAP_TM_STANDARD_FRT_COST_DOC,
        fufo.SAP_TM_CARRIER,
        fufo.SAP_TM_LIFECYCLE,        
		fufo.SAP_TM_FRT_COST_STATUS,  		
		fufo.SAP_TM_PLANNED_PGI_DATE,
		fufo.SAP_TM_FO_PGI_DATE,
		hdr.shipperid as SHIPPING_POINT,
		hdr.LABELTXT as SAP_ORDER_OR_DELV_NUM,
		hdr.BLK_EXEC as EXECUTION_BLOCK,
		hdr.EXEC_GRP_ID as TM_EXEC_GRP_ID,		
		peg.exec_grp_desc as TM_EXEC_GRP_DESC,	
		peg.LOAD_PLANNER as TM_EXEC_GRP_LOAD_PLANNER,	
      	hdr.EXEC_ORG_ID as TM_EXEC_ORG_ID,
      	peo.exec_org_desc as TM_EXEC_ORG_DESC,      	
      	hdr.ZZLC_NUM as SAP_TM_LETTER_OF_CREDIT_NUM,
      	hdr.ZZLC_LSDATE as SAP_TM_LATEST_SAILING_DATE,
      	hdr.GRO_WEI_VAL as SAP_TM_GROSS_WEIGHT,
      	hdr.GRO_VOL_VAL as SAP_TM_GROSS_VOLUME,
      	first_stop.LOG_LOCID as SAP_TM_SHIP_LOCN,
		case when PLAN_STATUS_ROOT = '03' then 'Planned' else 'Not Planned' end as SAP_PLANNING_STATUS,
		UTCTOLOCAL(cast(first_stop.SEL_TIME as timestamp), 'CST') as SAP_SHIP_LOAD_TIME_LOCAL,
		cast(first_stop.SEL_TIME as timestamp) as SAP_SHIP_LOAD_TIME_UTC,
		cast(map(first_stop.ASSGN_START, 0, 99991231, first_stop.ASSGN_START) as date) as SAP_TRANS_PLANNING_DATE,
		hdr.TRANSSRVLVL_CODE as SAP_TM_SERVICE_LEVEL,
		first_stop.stop_seq_pos as first_stop_seq_pos,
		first_stop.stop_cat as first_stop_cat,
		last_stop.stop_seq_pos as last_stop_seq_pos,
		last_stop.stop_cat as last_stop_cat
	from "TMSLT"."/SCMTMS/D_TORROT" hdr
	left join "TMSLT"."/SCMTMS/D_TORSTP" as first_stop
		on hdr.DB_KEY = first_stop.PARENT_KEY
		and first_stop.stop_role = 'FP'
		and first_stop.stop_seq_pos = 'F'
		and first_stop.stop_cat = 'O'
		and first_stop.SEL_TIME <> 21130060000		
	inner join "TMSLT"."/SCMTMS/D_TORSTP" as last_stop
		on hdr.DB_KEY = last_stop.PARENT_KEY
		and last_stop.STOP_ROLE = 'FD'
		and last_stop.stop_seq_pos = 'L'
		and last_stop.stop_cat = 'I'
	left join ptt_app.pln_exec_org peo
		on hdr.exec_org_id = peo.exec_org_id		
	left join ptt_app.pln_exec_grp peg
		on hdr.exec_grp_id = peg.exec_grp_id		
	left join fu_to_fo fufo
		on hdr.DB_KEY = fufo.FU_ROOT_KEY
	where hdr.TOR_CAT = 'FU'
)
,matPlant as (
	select mp.*
	from   eccslt.marc mp
	where  mp.werks = '0300'
)
, matDistChannel as (
    select mdc.*
    from   eccslt.mvke mdc
    where  mdc.VKORG = 'TYUS'
    and    mdc.vtweg = '99'
)
, matBU as (
    select 
            mtx.prctr as profit_center,
            mtx.wwmbu as business_unit_code,
            t.bezek as business_unit_descr,
            mtx.wwmbu  || ' - ' || t.bezek as business_unit
    from    eccslt.ZFNSBUPCMATRIX mtx
    left join   eccslt.T25C9 t on (t.wwmbu = mtx.wwmbu)
    where   t.SPRAS = 'E'    
)
,material as (
    select      
		    m.matnr,        
            --mp.*,
            mdc.mvgr5 as trade_pricing_tier,
            mdc.mvgr1 as trade_spend_cat,
            mb.business_unit,
            mb.profit_center,
            mbr.bezei as material_branch,
            mse.bezei as material_section--,
            -- TDC --
            --aus.atwrt as target_dist_channel,
            --cwnt.atwtb as target_dist_channel_desc
            -- TDC --            
    from    eccslt.mara m
    left join   matPlant mp on (m.matnr = mp.matnr)
    left join   matDistChannel mdc on (m.matnr = mdc.matnr)
    left join   matBU mb on (mp.prctr = mb.profit_center) 
    left join   eccslt.TVM2T mbr on (mbr.mvgr2 = mdc.mvgr2)
    left join   eccslt.TVM3T mse on (mse.mvgr3 = mdc.mvgr3)
    -- TDC --
    --left join 	eccslt.ausp aus on (m.matnr = aus.objek)
    --left join 	eccslt.cawn cwn on (aus.atwrt = cwn.atwrt and aus.atinn = cwn.atinn)
    --left join	eccslt.cawnt cwnt on (cwn.atzhl = cwnt.atzhl and cwn.atinn = cwnt.atinn)    
    -- TDC --     
    where m.MTART IN ('ZFG', 'ZFGI', 'ZSWP', 'ZUNV')
    and   mbr.spras = 'E'
    and   mse.spras = 'E'
)
,multiPlants as (
       select
             hdr.vbeln,
             count(distinct dtl.werks) as plantCount
       from eccslt.vbak hdr
       inner join eccslt.vbap dtl
             on hdr.vbeln = dtl.vbeln
       where hdr.auart in (
             'YOR',
             'YTP',
             'YNP',
             'YXC',
             'YXX',
             'YEX',
             'YDO',
             'YMD',
             'YMC',
             'YMS',
             'YSA'
       )
       and dtl.werks <> ''
       and dtl.abgru = ''
       group by hdr.vbeln
       having count(distinct dtl.werks) > 1
)
,multiPaymentTerms as (
	select
		hdr.vbeln,
		count(distinct coalesce(vbkd_dtl.zterm, vbkd_hdr.zterm)) as paymentTermCount
	from eccslt.vbak hdr
	inner join eccslt.vbap dtl
		on hdr.vbeln = dtl.vbeln
	left join eccslt.vbkd vbkd_hdr
		on hdr.vbeln = vbkd_hdr.vbeln
		and vbkd_hdr.posnr = '000000'
	left join eccslt.vbkd vbkd_dtl
		on hdr.vbeln = vbkd_dtl.vbeln
		and dtl.posnr = vbkd_dtl.posnr
	where hdr.auart in (
             'YOR',
             'YTP',
             'YNP',
             'YXC',
             'YXX',
             'YEX',
             'YDO',
             'YMD',
             'YMC',
             'YMS',
             'YSA'
       )
       and dtl.abgru = ''
	group by hdr.vbeln
	having count(distinct coalesce(vbkd_dtl.zterm, vbkd_hdr.zterm)) > 1
)
,nasty as (
	select distinct objky
	from  eccslt.nast 
	where nast.kappl = 'V2'
	and nast.vstat = 0
)
,sapThirdPartyPOs as (
	select
		hdr.ebeln as sap_po_num,
		to_date(hdr.aedat) as sap_po_created_date,
		dtl.ebelp as sap_po_line_item_num,
		dtl.matnr as sap_mtrl_num,
	    dtl.banfn as sap_purchase_req_num,
	    dtl.bnfpo as sap_purchase_req_item_num
	from eccslt.ekko hdr
	inner join eccslt.ekpo dtl
		on hdr.ebeln = dtl.ebeln
	where hdr.bsart in ('NB')
	and hdr.loekz = ''
	and dtl.loekz = ''
	and dtl.pstyp = '5'
	and dtl.knttp = 'X'
	and dtl.banfn <> ''
)
,salesAreaDesc as (
	select distinct
		sap_sales_org || '-' || sap_dist_channel || '-' || sap_division as sap_sales_area,
		trim(sap_dist_channel_desc) || ' - ' || trim(sap_division_code_desc) as sap_sales_area_desc
	from ptt_owner.sell_grp_to_sales_area_xref
	order by trim(sap_dist_channel_desc) || ' - ' || trim(sap_division_code_desc)
)
,sapOrders as (
       select
             SAP_HDR.BSTNK as CUST_PO_NUM,
             COALESCE(SAP_HDR.AUART, '') as SAP_ORDER_TYPE_CODE,
             case when COALESCE(SAP_HDR.AUART, '') not in ('YMD', 'YMC', 'YMS', 'YXX') then 'Delv Relevant' else 'Not Delv Relevant' end as SAP_DELV_RELEVANT_ORDER_TYPE_IND,
       		 SAP_HDR.VSBED as SAP_TRANS_TYPE_CODE_RAW,
       		 map(
       		 	SAP_HDR.VSBED,
       		 	'TL', 'TL - Truck Load',
       		 	'TC', 'TC - Consolidated Truck',
       		 	'CL', 'CL - CPU Truck Load',
       		 	'CC', 'CC - CPU Consolidated Truck',
       		 	'RL', 'RL - Rail Load',
       		 	'AL', 'AL - Air Load',
       		 	'SD', 'SD - Store Delivery',
       		 	'ZC', 'ZC - Zero Transit Export Container',
       		 	'ZR', 'ZR - Zero Transit Rail',
       		 	'CR', 'CR - CPU Rail',
       		 	'CS', 'CS - CPU Source Loaded',
       		 	'ZT', 'ZT - Zero Transit Truck',
       		 	'XC', 'XC - Export Container',
       		 	'ZX', 'ZX - Non Transportation No Del',
       		 	'LL', 'LL - True LTL Carrier Load',
       		 	'PL', 'PL - Parcel Load',
       		 	SAP_HDR.VSBED) as SAP_TRANS_TYPE_CODE,
             TO_DATE(SAP_HDR.ERDAT) as SAP_ORDER_CREATE_DATE,      
             cast(cast(SAP_HDR.ERZET as time) as varchar) as SAP_ORDER_CREATE_TIME,  
             cast(hour(cast(SAP_HDR.ERZET as time)) as int) as SAP_ORDER_CREATE_HOUR,  
             TO_DATE(SAP_HDR.AUDAT) as SAP_ORDER_DATE,
             add_days(TO_DATE(SAP_HDR.AUDAT), 6-mod(weekday(TO_DATE(SAP_HDR.AUDAT))+1,7)) as order_week_ending_date,
			 add_days(current_date, 6-mod(weekday(current_date)+1,7)) as current_week_ending_date,
			 days_between(add_days(current_date, 6-mod(weekday(current_date)+1,7)), add_days(TO_DATE(SAP_HDR.AUDAT), 6-mod(weekday(TO_DATE(SAP_HDR.AUDAT))+1,7)))/7 as order_fuzzy_week_num,            
             TO_DATE(SAP_HDR.VDATU) as SAP_ORDER_DELV_DATE,
             SAP_HDR.VBELN as SAP_SALES_ORDER_NUM,
			 SAP_HDR.VKORG || '-' ||SAP_HDR.VTWEG|| '-' ||SAP_HDR.SPART as SAP_SALES_AREA,
			 sad.sap_sales_area_desc,
       		 SAP_HDR.KUNNR as SAP_SOLD_TO_NUM,
       		 ptnrHdr.SAP_SOLD_TO_DESC,
       		 ptnrHdr.sap_concept_customer,
			 ptnrHdr.SAP_CONCEPT_CUST_DESC,	
			 ptnrHdr.SAP_CSR,
			 ptnrHdr.SAP_CSR_DESC,		         
             ptnrHdr.SAP_SHIP_TO,
             ptnrHdr.SAP_SHIP_TO_DESC,
             ptnrHdr.SAP_PAYER,
             ptnrHdr.SAP_PAYER_DESC,
             ptnrHdr.sap_route as sap_route_ptnr,
             ptnrHdr.sap_route_desc as sap_route_ptnr_desc,
             ptnrHdr.sap_sales_position,
             ptnrHdr.sap_sales_position_desc,
             coalesce(ptnr.SAP_SHIP_TO, ptnrHdr.SAP_SHIP_TO) as SAP_SHIP_TO_LINE,
             coalesce(ptnr.SAP_SHIP_TO_DESC, ptnrHdr.SAP_SHIP_TO_DESC) as SAP_SHIP_TO_DESC_LINE,             
             coalesce(ptnr.SAP_PAYER, ptnrHdr.SAP_PAYER) as SAP_PAYER_LINE,             
             coalesce(ptnr.SAP_PAYER_DESC, ptnrHdr.SAP_PAYER_DESC) as SAP_PAYER_DESC_LINE,
             coalesce(ptnr.sap_route, ptnrHdr.sap_route) as SAP_ROUTE_PTNR_LINE,
             coalesce(ptnr.sap_route_desc, ptnrHdr.sap_route_desc) as SAP_ROUTE_PTNR_DESC_LINE,
             coalesce(ptnr.sap_sales_position, ptnrHdr.sap_sales_position) as sap_sales_position_line,
             coalesce(ptnr.sap_sales_position_desc, ptnrHdr.sap_sales_position_desc) as sap_sales_position_line_desc,
             SAP_DTL.POSNR as sap_line_item_num,
             row_number() over(partition by sap_hdr.vbeln order by sap_line_load_date, sap_dtl.posnr) as line_ranker,
             row_number() over(partition by sap_hdr.vbeln, sap_dtl.posnr order by sap_dtl.posnr) as sub_line_ranker,
             SAP_DTL.MATNR as MTRL_NUM,  
             SAP_DTL.LPRIO as ORDER_DELIVERY_PRIORITY,
			 sa.lprio as sold_to_sales_area_delv_priority,
			 case when SAP_DTL.LPRIO <> sa.lprio then 'Delivery Priority Mismatch' else 'Delivery Priority Match' end as delv_priority_mismatch_ind,
			 sap_hdr.vkgrp as sap_sales_group,
			 sap_hdr.vkbur as sap_sales_office,
             SAP_DTL.ROUTE as SAP_ROUTE,
             map(
             	SAP_DTL.ABGRU, 
             	'', 'Not Rejected', 
             	'Z0', 'Z0 - Customer Cancellation',
             	'Z1', 'Z1 - Item Not Authorized',
             	'Z2', 'Z2 - Wrong Product',
             	'Z3', 'Z3 - Zero Available at Delivery',
             	'Z4', 'Z4 - Zero Available at Order',
             	'Z5', 'Z5 - Cobb',
             	'Z6', 'Z6 - Close Open Item',
             	'Z7', 'Z7 - Deleted Material',
             	'Z8', 'Z8 - Credit',
             	'Z9', 'Z9 - Material Substitution',
             	'ZA', 'ZA - Duplicate Order',
             	'ZB', 'ZB - Incorrect Sales Area',
             	SAP_DTL.ABGRU)
             as REJECTION_REASON,             
             map(SAP_DTL.ABGRU, '', 'Not Rejected', 'Rejected') as REJECTED_IND, 
             case when SAP_DTL.ABGRU = '' then 1 else 0 end as not_rejected_flag,
             case when sum(case when SAP_DTL.ABGRU = '' then 1 else 0 end) over(partition by sap_hdr.vbeln) = 0 then 'Order Fully Rejected' else 'Order Not Fully Rejected' end as order_fully_rejected_ind,
             makt.MAKTX as sap_mtrl_desc,
             SAP_DTL.WERKS || ' - ' || plt.name1 as SAP_ORDER_PLANT,
             ztfi.plant_type,
             to_date(SAP_HDR.ERDAT) as loadDate,
             po.sap_po_num,
             po.sap_po_line_item_num,
             sapTD.sap_purchase_req_num,
             po.sap_purchase_req_num as sap_purchase_req_num_po,
             sapTD.sap_purchase_req_item_num,
             po.sap_purchase_req_item_num as sap_purchase_req_item_num_po,
             sapTD.sap_line_planned_pgi_date,
             sapTD.sap_line_load_hour,
             to_date(coalesce(case when to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') = '' then '9999-12-31' else to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') end, '9999-12-31')) as SAP_LINE_LOAD_DATE,                          
             to_char(to_date(sapTD.sap_line_delv_date), 'YYYY-MM-DD') as SAP_LINE_DELV_DATE,
             to_char(to_date(sapTD.sap_line_mtrl_avail_date), 'YYYY-MM-DD') as SAP_MTRL_AVAIL_DATE,  
             days_between(current_date, to_date(coalesce(case when to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') = '' then '9999-12-31' else to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') end, '9999-12-31'))) as DAYS_UNTIL_LOAD_DATE,  
             days_between(TO_DATE(SAP_HDR.ERDAT), to_date(coalesce(case when to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') = '' then '9999-12-31' else to_char(to_date(sapTD.sap_line_load_date), 'YYYY-MM-DD') end, '9999-12-31'))) as DAYS_BETWEEN_LOAD_DATE,  
             days_between(TO_DATE(SAP_HDR.ERDAT), current_date) as DAYS_IN_SYSTEM, 
             lp.sap_line_list_value,
             prc.sap_line_net_value as sap_line_net_value_dup,      
             case when prc.sap_line_net_value <= 0 or coalesce(lp.sap_line_list_value,-1) <= 0 then 1 else 0 end as MATERIAL_NOT_PRICED_FLAG,
             case when sum(case when prc.sap_line_net_value <= 0 or coalesce(lp.sap_line_list_value,-1) <= 0 then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Material Not Priced' else 'All Materials Priced' end as MATERIAL_NOT_PRICED_IND,             
             case when (prc.sap_line_net_value <= 0 or coalesce(lp.sap_line_list_value,-1) <= 0) and SAP_DTL.ABGRU = '' then 1 else 0 end as MATERIAL_NOT_PRICED_NOT_REJECTED_FLAG,
			 case when sum(case when (prc.sap_line_net_value <= 0 or coalesce(lp.sap_line_list_value,-1) <= 0) and SAP_DTL.ABGRU = '' then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Material Not Priced and Not Rejected' else 'All Materials Priced or Rejected' end as MATERIAL_NOT_PRICED_NOT_REJECTED_PRICED_IND,                                       
             map(status.LFSTK, 'A', 'No Delivery', status.LFSTK) as SAP_SO_DELV_STATUS, -- A means no deliveries
			 map(status.FKSAK, 'A', 'Not Billed', status.FKSAK)  as SAP_SO_BILL_STATUS, -- A means not billed			
			 map(status.GBSTK, 'C', 'Sales Order Complete', 'A', 'Sales Order Incomplete', status.GBSTK) as SAP_SO_STATUS,
			 coalesce(map(
			 	status.CMGST,
			 	'', 'Not Executed',
			 	'A', 'A - Passed',
			 	'B', 'B - Failed',
			 	'C', 'C - Partially Passed',
			 	'D', 'D - Manually Released',
			 	status.CMGST), '')
			 as SAP_ORDER_CREDIT_BLOCK_STATUS,
			 case when coalesce(status.CMGST, '') in ('', 'A', 'D') then 'No Credit Block' else 'Credit Block' end as sales_order_credit_block_ind,		
			 coalesce(map(
			 	coalesce(delv.SAP_DELV_CREDIT_BLOCK_STATUS, delvPO.SAP_DELV_CREDIT_BLOCK_STATUS),
			 	'', 'Not Executed',
			 	'A', 'A - Passed',
			 	'B', 'B - Failed',
			 	'C', 'C - Partially Passed',
			 	'D', 'D - Manually Released',
			 	coalesce(delv.SAP_DELV_CREDIT_BLOCK_STATUS, delvPO.SAP_DELV_CREDIT_BLOCK_STATUS)), '') as SAP_DELV_CREDIT_BLOCK_STATUS,
			 case when coalesce(delv.SAP_DELV_CREDIT_BLOCK_STATUS, delvPO.SAP_DELV_CREDIT_BLOCK_STATUS, '') in ('', 'A', 'D') then 'No Credit Block' else 'Credit Block' end as sap_delv_credit_block_ind,				 	
             map(SAP_HDR.LIFSK, 'Y1', 'Default', 'Y2', 'CSR', 'Y3', 'ATP', 'Y4', 'Credit', SAP_HDR.LIFSK) as SAP_DELV_BLOCK,
		  	 map(
				   SAP_HDR.FAKSK,
				   '01','01 - Calculation Missing',
				   '02','02 - Compl Confirm Missng',
				   '03','03 - Prices Incomplete',
				   '04','04 - Check Terms of Paymt',
				   '05','05 - Check Terms of Dlv',
				   '08','08 - Check Credit Memo',
				   '09','09 - Check Debit Memo',
				   'YD','YD - Multiple Deliveries',
				   'YF','YF - No Freight Order',
				   'YM','YM - Missing Price',
				   'YT','YT - Payment Terms',
				   'YX','YX - Price Mismatch',
				   'YZ','YZ - Miscellaneous Block',
				   'Z1','Z1 - Inactive Customer',
				   'Z3','Z3 - Settlmnt Apprvl Rqrd',
				   'Z6','Z6 - Negative Line Item',
				   'Z7','Z7 - EIM Billing Block',
				   'Z8','Z8 - Check Cobb Pricing',
				   SAP_HDR.FAKSK
			 ) as SAP_BILLING_BLOCK,
			 delv_df.SAP_DELV_NUM as sap_delv_num_df,
			 delv_df.SAP_DELV_LINE as sap_delv_line_df,
			 coalesce(delv.PLANNED_PGI_DATE, delvPO.PLANNED_PGI_DATE) as sap_delv_planned_pgi_date,
			 coalesce(delv.PGI_DATE, delvPO.PGI_DATE) as PGI_DATE,
			 map(
			 	coalesce(delv.OVERALL_DELIVERY_STATUS, delvPO.OVERALL_DELIVERY_STATUS),
			 	'', 'Not Relevant',
			 	'A', 'A - No Delivery',
			 	'B', 'B - Partial Delivery',
			 	'C', 'C - Delivery Complete',
			 	coalesce(delv.OVERALL_DELIVERY_STATUS, delvPO.OVERALL_DELIVERY_STATUS)
			 ) as OVERALL_DELIVERY_STATUS,
			 
			 coalesce(delv.sap_delv_pod_flag, delvPO.sap_delv_pod_flag) as sap_delv_pod_flag,
			 coalesce(delv.sap_delv_pod_date, delvPO.sap_delv_pod_date) as sap_delv_pod_date,
			 
			 coalesce(delv.PGI_STATUS, delvPO.PGI_STATUS, 'Not PGIed') as DELV_PGI_STATUS,
			 case when sum(case when  coalesce(delv.PGI_STATUS, delvPO.PGI_STATUS, 'Not PGIed') = 'PGIed' then 1 else 0 end) over(partition by sap_hdr.vbeln, sap_dtl.werks) > 0 then 'PGIed' else 'Not PGIed' end as PGI_STATUS,
			 coalesce(delv.PICK_DATE, delvPO.PICK_DATE) as PICK_DATE,
			 coalesce(delv.PICK_DATE_Z, delvPO.PICK_DATE_Z) as PICK_DATE_Z,
			 coalesce(delv.PICK_STATUS, delvPO.PICK_STATUS, 'Not Picked') as DELV_PICK_STATUS,
			 case when sum(case when coalesce(delv.PICK_STATUS, delvPO.PICK_STATUS, 'Not Picked') = 'Picked' then 1 else 0 end) over(partition by sap_hdr.vbeln, sap_dtl.werks) > 0 then 'Picked' else 'Not Picked' end as PICK_STATUS,
			 coalesce(delv.sap_delv_num, delvPO.sap_delv_num, '') as sap_delv_num,
			 coalesce(delv.sap_delv_create_date, delvPO.sap_delv_create_date) as sap_delv_create_date,
			 coalesce(delv.sap_delv_create_time, delvPO.sap_delv_create_time) as sap_delv_create_time,
			 coalesce(delv.sap_delv_load_date, delvPO.sap_delv_load_date) as sap_delv_load_date,
			 coalesce(delv.sap_delv_req_date, delvPO.sap_delv_req_date) as sap_delv_req_date,
			 case when coalesce(delv.sap_delv_num, delv_df.SAP_DELV_NUM, delvPO.sap_delv_num) is not null then 'Has Delivery' else 'No Delivery' end as HAS_DELIVERY_IND,			 
			 ship_df.SAP_SHIPMENT_NUM,
			 coalesce(bill_df.SAP_BILLING_DATE, bill_df_order.SAP_BILLING_DATE) as SAP_BILLING_DATE,
			 coalesce(bill_df.sap_billing_num, bill_df_order.sap_billing_num, '') as sap_billing_num,
			 case when coalesce(bill_df.sap_billing_num, bill_df_order.sap_billing_num) is not null then 'Billed' else 'Not Billed' end as BILLED_IND,
			 case when ship_df.SAP_SHIPMENT_NUM is not null then 'Has Shipment' else 'No Shipment' end as HAS_SHIPMENT_IND,
			 case when sapTD.sched_line_count > 1 then 1 else 0 end as MULTI_SCHED_LINE_FLAG,
             case when sum(case when sapTD.sched_line_count > 1 then 1 else 0 end) > 0 then 'Multiple Sched Lines' else 'Not Multiple Sched Lines' end as MULTI_SCHED_LINE_IND,
             case when sum(sapTD.confirmed_qty) <> sum(SAP_DTL.KWMENG) then 1 else 0 end as CONFIRMED_QTY_DISCREP_FLAG,
             sum(case when sum(sapTD.confirmed_qty) <> sum(SAP_DTL.KWMENG) then 1 else 0 end) over(partition by sap_hdr.vbeln) as confirmed_qty_issue_line_count,
             case when sum(case when sum(sapTD.confirmed_qty) <> sum(SAP_DTL.KWMENG) then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Confirmed Quantity Off' else 'Confirmed Quantity Match' end as CONFIRMED_QTY_DISCREP_IND,
             sapTD.sched_line_count,
--             case when coalesce(delv.sap_delv_num, '') <> coalesce(delv_df.sap_delv_num, '') then 'Doc Flow Discrep' else 'Doc Flow Match' end as doc_flow_discrep_ind,                                                
             case when SAP_DTL.MTVFP = 'Z1' then 'Made to Order' when SAP_DTL.MTVFP = '02' then 'Made to Stock' else 'Other' end as made_to_order_flag,
             case when sum(sapTD.confirmed_qty) = 0 then 1 else 0 end as zero_confirmed_flag,             
             case when sum(sapTD.confirmed_qty) <> 0 and sum(sapTD.confirmed_qty) <> sum(SAP_DTL.KWMENG) then 1 else 0 end as partially_confirmed_flag,                          
             case when sum(sapTD.confirmed_qty) = 0 then 'Zero Confirmed'
             	  when sum(sapTD.confirmed_qty) <> sum(SAP_DTL.KWMENG) then 'Partially Confirmed'
             	  else 'Fully Confirmed'
             end as line_confirmed_ind,
             case when (sum(sapTD.confirmed_qty) = sum(SAP_DTL.KWMENG) and sum(sapTD.confirmed_qty) <> 0) or SAP_DTL.ABGRU <> '' or SAP_DTL.MTVFP = 'Z1' then 1 else 0 end as confirmed_flag,                
             case when sum(case when (sum(sapTD.confirmed_qty) = sum(SAP_DTL.KWMENG) and sum(sapTD.confirmed_qty) <> 0) or SAP_DTL.ABGRU <> '' or SAP_DTL.MTVFP = 'Z1' then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Order Totally Confirmed' else 'Order Not Totally Confirmed' end as order_confirmed_ind,                                                                                                 
             case when sum(sapTD.confirmed_qty) = 0 and SAP_DTL.ABGRU = '' and SAP_DTL.MTVFP <> 'Z1' then 1 else 0 end as conf_qty_zero_not_rejected_flag,                
             case when sum(case when sum(sapTD.confirmed_qty) = 0 and SAP_DTL.ABGRU = '' and SAP_DTL.MTVFP <> 'Z1' then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'MTS Confirmed Quantity 0 Line Not Rejected' else 'Total Order Confirmed Quantity Ok' end as confirmed_qty_zero_not_rejected_ind,             
             tmOtr.otr_type,
             tmOtr.OTR_CONSUMPTION_STATUS,
             tmSt.FU_NUM,
             tmSt.SAP_TRANS_PLANNING_DATE,
             tmSt.FO_NUM,
			 --tmSt.SAP_TM_INLAND_CAR_NUM,
             tmSt.SAP_TM_PLANNED_PGI_DATE,
             tmSt.FU_TYPE,            
             case when SAP_TRANS_PLANNING_DATE <> sapTD.sap_line_load_date then 'TM Load Date Mismatch' else 'TM Load Date Match' end as TM_LOAD_DATE_MISMATCH_IND,
             map(upper(tmSt.EXECUTION_BLOCK), 'X', 'Blocked', 'Not Blocked') as TM_EXECUTION_BLOCK,                   
             tmSt.SHIPPING_POINT,
             tmSt.SAP_SHIP_LOAD_TIME_UTC,
			 tmSt.SAP_SHIP_LOAD_TIME_LOCAL,
			 UTCTOLOCAL(cast(tmSt.SAP_SHIP_LOAD_TIME_UTC as timestamp), adrc.TIME_ZONE) as SAP_SHIP_LOAD_TIME_PLANT,
			 tmSt.TM_EXEC_GRP_ID,
			 tmSt.TM_EXEC_GRP_DESC,	
			 tmSt.TM_EXEC_GRP_LOAD_PLANNER,				 
      		 tmSt.TM_EXEC_ORG_ID,	
			 tmSt.TM_EXEC_ORG_DESC,
			 adrc.TIME_ZONE as sap_plant_time_zone,
			 coalesce(tmSt.SAP_PLANNING_STATUS, 'Not Planned') as SAP_PLANNING_STATUS,
			 case when tmSt.SAP_PLANNING_STATUS is not null then 'In TM' else 'Not in TM' end as IN_TM_IND,
			 case when coalesce(tmSt.SAP_PLANNING_STATUS, 'Not in TM') <> 'Planned' and SAP_DTL.ABGRU = '' then 1 else 0 end as not_planned_or_rejected_flag,
			 case when sum(case when coalesce(tmSt.SAP_PLANNING_STATUS, 'Not in TM') <> 'Planned' and SAP_DTL.ABGRU = '' then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Not All Lines Planned or Rejected' else 'All Lines Planned or Rejected' end as planned_or_rejected_ind,			 
			 case when sap_dtl.werks in ('8665',
										'2388',
										'2391',
										'2392',
										'8009',
										'8103',
										'8106',
										'8096',
										'8094',
										'8858',
										'8882',
										'8857',
										'2678',
										'8808',
										'2682',
										'8979') 
					and COALESCE(SAP_HDR.AUART, '') <> 'YTP'
					then 1--'Aquisition Plant with non-YTP Order Type'
					else 0--''
			 end as non_ytp_order_for_aquisition_plant_flag,			 
			 case when sum(case when sap_dtl.werks in ('8665',
										'2388',
										'2391',
										'2392',
										'8009',
										'8103',
										'8106',
										'8096',
										'8094',
										'8858',
										'8882',
										'8857',
										'2678',
										'8808',
										'2682',
										'8979') 
									and COALESCE(SAP_HDR.AUART, '') <> 'YTP'
									then 1--'Acquisition Plant with non-YTP Order Type'
									else 0--''
							end) over(partition by sap_hdr.vbeln) > 0 then 'Acquisition Plant with non-YTP Order Type' else ''
			 end as non_ytp_order_for_aquisition_plant_ind,			 
             mat.profit_center,
             mat.business_unit,
             mat.trade_pricing_tier,
             mat.trade_spend_cat,
             mat.material_branch,
             mat.material_section,	
			 --mat.target_dist_channel,
			 --mat.target_dist_channel_desc,			 
             SAP_HDR.BSARK  as CUST_PO_TYPE,    -- gabelj
             SAP_HDR.NETWR  as SAP_NET_VALUE,   -- gabelj
			 SAP_HDR.ZZBTGEW as SAP_TOTAL_WGT,
			 SAP_HDR.ZZVOLUM as SAP_TOTAL_VOLM,
			 case when mpt.vbeln is not null then 'Multiple Payment Terms' else 'Single Payment Terms' end as MULTI_PAY_TERMS_FOR_NON_REJECTED_LINES_IND,
			 case when mp.vbeln is not null then 'Multiple Plants' else 'Single Plant' end as MULTI_PLANT_FOR_NON_REJECTED_LINES_IND,
			 case when sap_dtl.werks = '' and SAP_DTL.ABGRU = '' then 1 else 0 end as missing_plant_for_non_rejected_line_flag,
			 case when sum(case when sap_dtl.werks = '' and SAP_DTL.ABGRU = '' then 1 else 0 end) over(partition by sap_hdr.vbeln) > 0 then 'Plant Missing for Non-Rejected Line' else 'All Lines Have Plant' end as missing_plant_for_non_rejected_line_ind,                          
             coalesce(tmdvsp.LDTM_HR_START, tmdv.LDTM_HR_START, 0) as tm_delv_hour_start,           
             coalesce(tmdvsp.DELV_CREATION_BUS_HRS_FROM_LDTM, tmdv.DELV_CREATION_BUS_HRS_FROM_LDTM, 0) as tm_delv_creation_bus_hrs_from_ldtm,                        
             sap_dtl.VRKME as sap_order_qty_uom,
			 to_date(coalesce(tmSt.SAP_TM_PLANNED_PGI_DATE, delv.planned_pgi_date, delvPO.planned_pgi_date, sapTD.sap_line_planned_pgi_date)) as planned_pgi_date,
			 days_between(current_date, to_date(coalesce(tmSt.SAP_TM_PLANNED_PGI_DATE, delv.planned_pgi_date, delvPO.planned_pgi_date, sapTD.sap_line_planned_pgi_date))) as days_until_planned_pgi_date,
			 days_between(current_date, to_date(sapTD.sap_line_planned_pgi_date)) as days_until_line_planned_pgi_date,
 			 days_between(current_date, to_date(coalesce(delv.planned_pgi_date, delvPO.planned_pgi_date))) as days_until_delv_planned_pgi_date,
 			 case when nasty.objky is not null then 'Not Sent to OCS' else '' end as NOT_SENT_TO_OCS_IND, 			 
             sum(SAP_DTL.KWMENG) as sap_order_qty_base,
             sum(sapTD.confirmed_qty) as confirmed_qty_base,
             sum(sap_dtl.NTGEW) as sap_line_net_weight,
             sum(sap_dtl.BRGEW) as sap_line_gross_weight,
             sum( 
				CASE
					WHEN SAP_DTL.VRKME = 'CS' THEN SAP_DTL.KWMENG
					WHEN SAP_DTL.VRKME = 'LB' THEN
				    	CASE
				        	WHEN marm_lb.UMREZ = 0 then null 
				        	ELSE SAP_DTL.KWMENG/(marm_lb.UMREN/marm_lb.UMREZ)
				     	END
					WHEN SAP_DTL.VRKME = 'EA' THEN 
						CASE 
							WHEN marm_ea.UMREZ = 0 then null
							ELSE SAP_DTL.KWMENG/(marm_ea.UMREN/marm_ea.UMREZ)
				     	END
				  	ELSE SAP_DTL.KWMENG
				END) as sap_order_qty_dup,
			
			 sum(
				CASE 
					WHEN SAP_DTL.VRKME = 'TON' THEN SAP_DTL.KWMENG*2000 ELSE 
					CASE
					   WHEN SAP_DTL.VRKME = 'CS' THEN SAP_DTL.KWMENG
					   WHEN SAP_DTL.VRKME = 'LB' THEN
					        CASE
					           WHEN marm_lb.UMREZ = 0 then null 
					           ELSE SAP_DTL.KWMENG/(marm_lb.UMREN/marm_lb.UMREZ)
					           END
					        WHEN SAP_DTL.VRKME = 'EA' THEN 
					   	    CASE 
					                WHEN marm_ea.UMREZ = 0 then null
					                ELSE SAP_DTL.KWMENG/(marm_ea.UMREN/marm_ea.UMREZ)
					            END
					        ELSE SAP_DTL.KWMENG
					END * CASE WHEN marm_lb.UMREZ = 0 THEN NULL ELSE marm_lb.UMREN/marm_lb.UMREZ END 
				END) as sap_order_lbs_dup,		
			 
			 sum(             
				CASE
					WHEN SAP_DTL.VRKME = 'CS' THEN sapTD.confirmed_qty
					WHEN SAP_DTL.VRKME = 'LB' THEN
				    	CASE
				        	WHEN marm_lb.UMREZ = 0 then null 
				        	ELSE sapTD.confirmed_qty/(marm_lb.UMREN/marm_lb.UMREZ)
				     	END
					WHEN SAP_DTL.VRKME = 'EA' THEN 
						CASE 
							WHEN marm_ea.UMREZ = 0 then null
							ELSE sapTD.confirmed_qty/(marm_ea.UMREN/marm_ea.UMREZ)
				     	END
				  	ELSE sapTD.confirmed_qty
				END) as confirmed_qty,
			
			 sum(
				CASE 
					WHEN SAP_DTL.VRKME = 'TON' THEN sapTD.confirmed_qty*2000 ELSE 
					CASE
					   WHEN SAP_DTL.VRKME = 'CS' THEN sapTD.confirmed_qty
					   WHEN SAP_DTL.VRKME = 'LB' THEN
					        CASE
					           WHEN marm_lb.UMREZ = 0 then null 
					           ELSE sapTD.confirmed_qty/(marm_lb.UMREN/marm_lb.UMREZ)
					           END
					        WHEN SAP_DTL.VRKME = 'EA' THEN 
					   	    CASE 
					                WHEN marm_ea.UMREZ = 0 then null
					                ELSE sapTD.confirmed_qty/(marm_ea.UMREN/marm_ea.UMREZ)
					            END
					        ELSE sapTD.confirmed_qty
					END * CASE WHEN marm_lb.UMREZ = 0 THEN NULL ELSE marm_lb.UMREN/marm_lb.UMREZ END 
				END) as sap_confirmed_lbs,                    
             
             
             
             sum(
            	 CASE
					WHEN SAP_DTL.VRKME = 'CS' THEN coalesce(delv.sap_delv_qty, delvPO.sap_delv_qty)
					WHEN SAP_DTL.VRKME = 'LB' THEN
				    	CASE
				        	WHEN marm_lb.UMREZ = 0 then null 
				        	ELSE coalesce(delv.sap_delv_qty, delvPO.sap_delv_qty)/(marm_lb.UMREN/marm_lb.UMREZ)
				     	END
					WHEN SAP_DTL.VRKME = 'EA' THEN 
						CASE 
							WHEN marm_ea.UMREZ = 0 then null
							ELSE coalesce(delv.sap_delv_qty, delvPO.sap_delv_qty)/(marm_ea.UMREN/marm_ea.UMREZ)
				     	END
				  	ELSE coalesce(delv.sap_delv_qty, delvPO.sap_delv_qty)
				END             
             ) as sap_delv_qty,
             count(*) as MTRL_CNT,
             count(distinct FU_ID) as FU_COUNT
       FROM    ECCSLT.VBAK SAP_HDR
       INNER JOIN ECCSLT.VBAP SAP_DTL on (SAP_HDR.VBELN = SAP_DTL.VBELN) 
	   left join eccslt.knvv sa
		    on SAP_HDR.kunnr = sa.kunnr
		    and SAP_HDR.vkorg = sa.vkorg
		    and SAP_HDR.vtweg = sa.vtweg
		    and SAP_HDR.spart = sa.spart           
	   LEFT JOIN  ECCSLT.MARA MARA on (SAP_DTL.MATNR = MARA.MATNR)
       LEFT JOIN ECCSLT.VBUK status
       		on SAP_HDR.VBELN = status.VBELN
       LEFT JOIN ECCSLT.MAKT makt
			ON SAP_DTL.MATNR = makt.MATNR
			and MAKT.SPRAS = 'E'
		LEFT JOIN ECCSLT.MARM marm_lb
		    ON SAP_DTL.MATNR = marm_lb.MATNR
		    and marm_lb.MEINH = 'LB'
		LEFT JOIN ECCSLT.MARM marm_ea
		    ON SAP_DTL.MATNR = marm_ea.MATNR
		    and marm_ea.MEINH = 'EA'			
       left join partnerHeader ptnrHdr on (SAP_HDR.VBELN = ptnrHdr.SAP_SALES_ORDER_NUM)
       left join partners ptnr
       		on SAP_HDR.VBELN = ptnr.SAP_SALES_ORDER_NUM 
       		and SAP_DTL.POSNR = ptnr.SAP_LINE_ITEM_NUM
       left join sapTransDates sapTD 
       		on sap_hdr.vbeln = sapTD.sap_sales_order_num
       		and sap_dtl.posnr = sapTD.sap_line_item_num
       left join sapDelivery delv
       		on SAP_HDR.vbeln = delv.sap_sales_order_or_po_num
       		and SAP_DTL.POSNR = delv.sap_line_item_num
	   left join sapDeliveryDocFlow delv_df
			on SAP_HDR.vbeln = delv_df.sap_sales_order_num
			and SAP_DTL.posnr = delv_df.sap_line_item_num
	   left join sapShipmentDocFlow ship_df
			on delv_df.sap_delv_num = ship_df.sap_delv_num
	   left join sapBillingDocFlow bill_df
		    on delv.sap_delv_num = bill_df.SAP_ORDER_OR_DELV_NUM
	   left join sapBillingDocFlow bill_df_order
		    on sap_hdr.vbeln = bill_df_order.SAP_ORDER_OR_DELV_NUM
	   left join tmShipmentTimeStatus tmSt
	   		on coalesce(cast(cast(delv.sap_delv_num as bigint) as varchar), sap_hdr.vbeln) = tmSt.SAP_ORDER_OR_DELV_NUM
	   left join tmOtrConsumptionStatus tmOtr
	   		on sap_padded_sales_order_num = lpad(SAP_HDR.VBELN, 35, 0)
	   left join eccslt.t001w plt
	   		on sap_dtl.werks = plt.werks
	   left join eccslt.adrc adrc
	   		on plt.ADRNR = adrc.ADDRNUMBER	   		
	   left join material mat
	   		on sap_dtl.matnr = mat.matnr
	   left join prices prc
	   		on sap_hdr.vbeln = prc.sap_sales_order_num
	   		and sap_dtl.posnr = prc.sap_line_item_num
	   left join listPrices lp
	   		on sap_hdr.vbeln = lp.sap_sales_order_num
	   		and sap_dtl.posnr = lp.sap_line_item_num
	   left join multiPlants mp
	   		on sap_hdr.vbeln = mp.vbeln
	   left join multiPaymentTerms mpt
	   		on sap_hdr.vbeln = mpt.vbeln
	   left join ptt_app.TM_DELV_SCHEDULE_VARIANT tmdv
			on tmOtr.OTR_TYPE = tmdv.OTR_TYPE
			and tmSt.FU_TYPE = tmdv.FU_TYPE
			and tmdv.SHIPPING_POINT = ''
	   left join ptt_app.TM_DELV_SCHEDULE_VARIANT tmdvsp
	   		on tmOtr.OTR_TYPE = tmdvsp.OTR_TYPE
	   		and tmSt.FU_TYPE = tmdvsp.FU_TYPE
	   		and tmSt.SHIPPING_POINT = tmdvsp.SHIPPING_POINT
	   		and tmdvsp.SHIPPING_POINT <> ''
	   left join eccslt.ztfiplants ztfi
	   		on sap_dtl.werks = ztfi.plant
	   left join nasty
	   		on coalesce(delv.sap_delv_num, delv_df.sap_delv_num) = nasty.objky
	   left join sapThirdPartyPOs po
	   		on sapTD.sap_purchase_req_num = po.sap_purchase_req_num
	   		and right(sapTD.sap_purchase_req_item_num,5) = po.sap_purchase_req_item_num	   					
	   left join sapDelivery delvPO
			on po.sap_po_num = delvPO.sap_sales_order_or_po_num
			and po.sap_po_line_item_num = right(delvPO.sap_line_item_num,5)
	   left join salesAreaDesc sad
	   		on SAP_HDR.VKORG || '-' ||SAP_HDR.VTWEG|| '-' ||SAP_HDR.SPART = sad.sap_sales_area			
	   where sap_hdr.auart in ('YOR',
							  'YTP',
							  'YNP',
							  'YXC',
							  'YXX',
							  'YEX',
							  'YDO',
							  'YMD',
							  'YMC',
							  'YMS',
							  'YSA'
			)			
       group by
       		 SAP_HDR.BSTNK,
       		 SAP_HDR.AUART,
       		 SAP_HDR.VSBED,
       		 SAP_HDR.ERDAT,
       		 SAP_HDR.ERZET,
             SAP_HDR.AUDAT,
             SAP_HDR.VDATU,
             SAP_HDR.VBELN,
             --uprc.sap_sales_order_num,
             SAP_HDR.VKORG || '-' ||SAP_HDR.VTWEG|| '-' ||SAP_HDR.SPART,
             sad.sap_sales_area_desc,
			 SAP_HDR.KUNNR,
       		 ptnrHdr.SAP_SOLD_TO_DESC,
       		 ptnrHdr.sap_concept_customer,
			 ptnrHdr.SAP_CONCEPT_CUST_DESC,	
			 ptnrHdr.SAP_CSR,
			 ptnrHdr.SAP_CSR_DESC,           
             ptnrHdr.SAP_SHIP_TO,
             ptnrHdr.SAP_SHIP_TO_DESC,
             ptnrHdr.SAP_PAYER,
             ptnrHdr.SAP_PAYER_DESC,
			 ptnrHdr.sap_route, 
			 ptnrHdr.sap_route_desc,
			 ptnrHdr.sap_sales_position,
			 ptnrHdr.sap_sales_position_desc,
             ptnr.SAP_SHIP_TO,
             ptnr.SAP_SHIP_TO_DESC,
             ptnr.SAP_PAYER,
             ptnr.SAP_PAYER_DESC,
			 ptnr.sap_route, 
			 ptnr.sap_route_desc,
 			 ptnr.sap_sales_position, 
 			 ptnr.sap_sales_position_desc,
             SAP_DTL.POSNR,
             SAP_DTL.MATNR,
             SAP_DTL.LPRIO,
			 sa.lprio,
			 sap_hdr.vkgrp,
			 sap_hdr.vkbur, 			 
             SAP_DTL.ROUTE,        
             makt.MAKTX,
             SAP_DTL.WERKS,
             SAP_HDR.ERDAT,             
             po.sap_po_num,
             po.sap_po_line_item_num,                         
             sapTD.sap_purchase_req_num,             
             sapTD.sap_purchase_req_item_num,               
             po.sap_purchase_req_num,
             po.sap_purchase_req_item_num,                                     
             sapTD.SAP_LINE_LOAD_HOUR,
             sapTD.sap_line_planned_pgi_date,
             sapTD.sap_line_load_date,
             sapTD.sap_line_delv_date,
             sapTD.sap_line_mtrl_avail_date,
             status.LFSTK,
             status.FKSAK,
             delv_df.SAP_DELV_NUM,
			 delv_df.SAP_DELV_LINE,
			 delv.PLANNED_PGI_DATE,
			 delv.PGI_DATE,
			 delv.OVERALL_DELIVERY_STATUS,
			 delv.PGI_STATUS,
			 delv.PICK_DATE,
			 delv.PICK_DATE_Z,
			 delv.PICK_STATUS,
			 delv.sap_delv_num,
			 delv.sap_delv_create_date,
			 delv.sap_delv_create_time,
			 delv.sap_delv_load_date,
			 delv.sap_delv_req_date,
			 delv.SAP_DELV_CREDIT_BLOCK_STATUS,			
			 delv.sap_delv_pod_flag,
			 delv.sap_delv_pod_date, 			  
			 delvPO.PLANNED_PGI_DATE,
			 delvPO.PGI_DATE,
			 delvPO.OVERALL_DELIVERY_STATUS,
			 delvPO.PGI_STATUS,
			 delvPO.PICK_DATE,
			 delvPO.PICK_DATE_Z,
			 delvPO.PICK_STATUS,
			 delvPO.sap_delv_num,
			 delvPO.sap_delv_create_date,
			 delvPO.sap_delv_create_time,
			 delvPO.sap_delv_load_date,
			 delvPO.sap_delv_req_date,	
			 delvPO.SAP_DELV_CREDIT_BLOCK_STATUS,					 
			 delvPO.sap_delv_pod_flag,
			 delvPO.sap_delv_pod_date, 	 
			 ship_df.SAP_SHIPMENT_NUM,
			 bill_df.sap_billing_num,
			 bill_df.sap_billing_date,
			 bill_df_order.sap_billing_num,
			 bill_df_order.sap_billing_date,
			 case when sapTD.sched_line_count > 1 then 1 else 0 end,
			 sapTD.sched_line_count,
			 tmOtr.OTR_TYPE,
			 tmOtr.OTR_CONSUMPTION_STATUS,
			 tmSt.FU_NUM,			 
			 tmSt.SAP_TRANS_PLANNING_DATE,
             tmSt.FO_NUM,
			 --tmSt.SAP_TM_INLAND_CAR_NUM,             
             tmSt.SAP_TM_PLANNED_PGI_DATE,			
             tmSt.EXECUTION_BLOCK,
			 tmSt.FU_TYPE,
			 tmSt.SHIPPING_POINT,
			 tmSt.SAP_SHIP_LOAD_TIME_UTC,
			 tmSt.SAP_SHIP_LOAD_TIME_LOCAL,
			 tmSt.TM_EXEC_GRP_ID,
			 tmSt.TM_EXEC_GRP_DESC,	
			 tmSt.TM_EXEC_GRP_LOAD_PLANNER,				 
      		 tmSt.TM_EXEC_ORG_ID,	
			 tmSt.TM_EXEC_ORG_DESC,			 
			 adrc.TIME_ZONE,
			 tmSt.SAP_PLANNING_STATUS,
			 SAP_DTL.ABGRU,
			 SAP_DTL.MTVFP,
			 status.GBSTK,
			 status.CMGST,
             SAP_HDR.LIFSK,
             SAP_HDR.FAKSK,
             mat.profit_center,
             mat.business_unit,
             mat.trade_pricing_tier,
             mat.trade_spend_cat,
             mat.material_branch,
             mat.material_section,
			 --mat.target_dist_channel,
			 --mat.target_dist_channel_desc,	             
             SAP_HDR.BSARK,             -- gabelj
             SAP_HDR.NETWR,             -- gabelj
			 SAP_HDR.ZZBTGEW,
			 SAP_HDR.ZZVOLUM,
 			 lp.sap_line_list_value,
			 prc.sap_line_net_value,
			 mp.vbeln,
			 mpt.vbeln,			 			 
			 tmdvsp.LDTM_HR_START, 
			 tmdv.LDTM_HR_START,           
             tmdvsp.DELV_CREATION_BUS_HRS_FROM_LDTM,
			 tmdv.DELV_CREATION_BUS_HRS_FROM_LDTM,
			 SAP_DTL.VRKME,
			 plt.name1,
			 ztfi.short_name,
			 ztfi.plant_type,
			 nasty.objky
 )
 ,multiDelvs as (
	select
		sap_sales_order_num,
		sap_order_plant,
		count(distinct sap_delv_num) as delv_count	
	from sapOrders
	where sap_delv_num <> ''
	and rejected_ind = 'Not Rejected'
	group by sap_sales_order_num,
	sap_order_plant
	having count(distinct sap_delv_num) > 1
)
,multiDelvSOs as (
	select distinct sap_sales_order_num	from multiDelvs
)
,multiFUs as (
	select
		sap_sales_order_num,
		sap_order_plant,
		count(distinct fu_num) as fu_count	
	from sapOrders
	where coalesce(fu_num,'') <> ''
	and rejected_ind = 'Not Rejected'
	group by sap_sales_order_num,
	sap_order_plant
	having count(distinct fu_num) > 1
)
,multiFUSOs as (
	select distinct sap_sales_order_num from multiFUs
)
,multiRoutes as (
	select
		sap_sales_order_num,
		sap_order_plant,
		count(distinct sap_route) route_count
	from sapOrders
	where coalesce(sap_route, '') <> ''
	and rejected_ind = 'Not Rejected'
	group by sap_sales_order_num,
		sap_order_plant
	having count(distinct sap_route) > 1
)
,multiRouteSOs as (
	select distinct sap_sales_order_num from multiRoutes
)
,multiDelvDate as (
	select
		sap_sales_order_num,
		sap_order_plant,
		count(distinct sap_line_delv_date) delv_date_count
	from sapOrders
	where coalesce(sap_line_delv_date, '9999-12-31') <> '9999-12-31'
	and rejected_ind = 'Not Rejected'
	group by sap_sales_order_num,
		sap_order_plant
	having count(distinct sap_line_delv_date) > 1
)
,multiDelvDateSOs as (
	select distinct sap_sales_order_num from multiDelvDate
)
,multiLoadDate as (
	select
		sap_sales_order_num,
		sap_order_plant,
		count(distinct sap_line_load_date) load_date_count
	from sapOrders
	where coalesce(sap_line_load_date, '9999-12-31') <> '9999-12-31'
	and rejected_ind = 'Not Rejected'
	group by sap_sales_order_num,
		sap_order_plant
	having count(distinct sap_line_load_date) > 1
)
,multiLoadDateSOs as (
	select distinct sap_sales_order_num from multiLoadDate
)
,sapOrdersTimeToDelvCreation as (
 	select
 		o.*,
		current_timestamp as report_run_time,
 		case when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
 			 then ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48))
 			 when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
 			 then ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24))
 			 else ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))
 		end as bus_delv_creation_time,
 		ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)) as raw_delv_creation_time,
    	round(case when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
 			 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
 			 when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
 			 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
 			 else SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
 		end, 1) as bus_hours_until_delivery_creation,
 		
 		
 		case when round(case when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
 			 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
 			 when weekday(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
 			 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
 			 else SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
 		end, 1) <= 0 then 'Delivery Creation Time Past' else 'Delivery Creation Time Future' end as DELV_CREATION_TIME_PAST_IND,
 		
  		round(SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(SAP_LINE_LOAD_DATE, (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600, 1) as raw_hours_until_delivery_creation,
  	
  	
	 	case when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
	 			 then ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48))
	 			 when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
	 			 then ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24))
	 			 else ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))
	 		end as bus_delv_creation_time_tm,
	 	ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, cast('2015-12-31' as date)), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)) as raw_delv_creation_time_tm,
	    round(case when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
	 		 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
	 		 when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
	 		 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
	 		 else SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
	 	end, 1) as bus_hours_until_delivery_creation_tm,
	 	case when round(case when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
	 		 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
	 		 when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
	 		 then SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
	 		 else SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
	 	end, 1) <= 0 then 'Delivery Creation Time Past' else 'Delivery Creation Time Future' end as DELV_CREATION_TIME_PAST_IND_TM, 		
	  	round(SECONDS_BETWEEN(current_timestamp, ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600, 1) as raw_hours_until_delivery_creation_tm,	
  	
  	
  		round(case when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
	 		 then SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
	 		 when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
	 		 then SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
	 		 else SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
	 	end, 1) as create_bus_hours_before_delv_creation_tm,
	 	
	 	
	 	case when 
		 	round(case when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 6
		 		 then SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*48)))/3600
		 		 when weekday(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm))) = 5
		 		 then SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)), (-3600*24)))/3600
		 		 else SECONDS_BETWEEN(sap_order_create_date, ADD_SECONDS(coalesce(SAP_TRANS_PLANNING_DATE, '2015-12-31'), (3600*tm_delv_hour_start)-(3600*tm_delv_creation_bus_hrs_from_ldtm)))/3600
		 	end, 1) < 0 then 'After Delv Creation Window' else 'Before Delv Creation Window' end as order_created_after_delv_window_ind,  		
		 
		 case when 
		  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
	  		--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
	  	< 0 then 0 else 
		  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
		  	--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
		  	--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
		  	--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
	 	end as business_days_between_load_date,
	 	
	 	
		case when 
		  	(floor(seconds_between(current_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(current_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(current_date) < 0 then 1 else 0 end)*2)
	  		--(case when weekday(current_date) in (5,6) then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(current_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
	  	< 0 then 0 else 
		  	(floor(seconds_between(current_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(current_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(current_date) < 0 then 1 else 0 end)*2)
		  	--(case when weekday(current_date) in (5,6) then 1 else 0 end)
		  	--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
		  	--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(current_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
	 	end as business_days_until_load_date,	 	
	 	
	 	case when
	 		case when 
		  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
	  		--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		  	< 0 then 0 else 
			  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
			  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
			  	--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  		+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  		-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		 	end < 3 then 'Short Lead Time' else 'Not Short Lead Time' 
		 end as short_lead_time_ind,
	 	
	 	case when 
		  	(floor(seconds_between(sap_order_create_date, sap_order_delv_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_order_delv_date)/604800) + case when weekday(sap_order_delv_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
	  		-(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
	  		-(case when weekday(sap_order_delv_date) = 5 then 1 else 0 end)
	  		-(case when weekday(sap_order_delv_date) = 6 then 2 else 0 end)
	  	< 0 then 0 else 
		  	(floor(seconds_between(sap_order_create_date, sap_order_delv_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_order_delv_date)/604800) + case when weekday(sap_order_delv_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
		  	-(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
		  	-(case when weekday(sap_order_delv_date) = 5 then 1 else 0 end)
		  	-(case when weekday(sap_order_delv_date) = 6 then 2 else 0 end)
	 	end as business_days_between_delv_date,
 		
 		case when md.sap_sales_order_num is not null then 'Multiple Deliveries for Single Plant' else 'Not Multiple Deliveries for Single Plant' end as MULTI_DELV_SINGLE_PLANT_IND,
  		case when mfu.sap_sales_order_num is not null then 'Multiple FUs for Single Plant' else 'Not Multiple FUs for Single Plant' end as MULTI_FU_SINGLE_PLANT_IND,  
 		case when mrte.sap_sales_order_num is not null then 'Multiple Routes for Single Plant' else 'Not Multiple Routes for Single Plant' end as MULTI_ROUTE_SINGLE_PLANT_IND,
 		case when mdd.sap_sales_order_num is not null then 'Multiple Delv Dates for Single Plant' else 'Not Multiple Delv Dates for Single Plant' end as MULTI_DELV_DATE_SINGLE_PLANT_IND,
 		case when mld.sap_sales_order_num is not null then 'Multiple Load Dates for Single Plant' else 'Not Multiple Load Dates for Single Plant' end as MULTI_LOAD_DATE_SINGLE_PLANT_IND,
 		case when line_ranker = 1 then 1 else 0 end as sap_sales_order_count,
 		
 		
 		 case when
	 		case when 
		  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
	  		--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		  	< 0 then 0 else 
			  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
			  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
			  	--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  		+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  		-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		 	end < 3 and line_ranker = 1 then 1 else 0 
		 end as late_order_count,
		 		 	
		 case when
	 		case when
		  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
		  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
	  		--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
	  		--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  	+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  	-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		  	< 0 then 0 else
			  	(floor(seconds_between(sap_order_create_date, sap_line_load_date)/86400))
			  	-((floor(seconds_between(sap_order_create_date, sap_line_load_date)/604800) + case when weekday(sap_line_load_date) - weekday(sap_order_create_date) < 0 then 1 else 0 end)*2)
			  	--(case when weekday(sap_order_create_date) in (5,6) then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 5 then 1 else 0 end)
			  	--(case when weekday(sap_line_load_date) = 6 then 2 else 0 end)
		  		+(case when weekday(sap_order_create_date) = 6 then 1 else 0 end)
		  		-(case when weekday(sap_line_load_date) = 6 then 1 else 0 end)
		 	end < 3 then sap_order_qty_dup else 0
		 end as late_order_qty,
		 
		 case when sub_line_ranker = 1 then sap_line_net_value_dup else 0 end as sap_line_net_value,
		 case when sub_line_ranker = 1 then sap_order_lbs_dup else 0 end as sap_order_lbs,
		 case when sub_line_ranker = 1 then sap_order_qty_dup else 0 end as sap_order_qty
		 
		 
 	from sapOrders o
 	left join multiDelvSOs md
 		on o.sap_sales_order_num = md.sap_sales_order_num
 	left join multiFUSOs mfu
 		on o.sap_sales_order_num = mfu.sap_sales_order_num
 	left join multiRouteSOs mrte
 		on o.sap_sales_order_num = mrte.sap_sales_order_num
 	left join multiDelvDateSOs mdd
 		on o.sap_sales_order_num = mdd.sap_sales_order_num
 	left join multiLoadDateSOs mld
 		on o.sap_sales_order_num = mld.sap_sales_order_num
 ) 
select
	*
from sapOrdersTimeToDelvCreation
where coalesce(sap_order_create_date, '9999-12-31') >= cast(case when :order_create_date = 'NA' then '2015-10-11' else :order_create_date end as date)
and coalesce(days_until_planned_pgi_date, 999) between
	case when :planned_pgi_days_low = 'NA' then coalesce(days_until_planned_pgi_date, 999)
		else cast(:planned_pgi_days_low as int)
	end 
	and
	case when :planned_pgi_days_high = 'NA' then coalesce(days_until_planned_pgi_date, 999)
		else cast(:planned_pgi_days_high as int)
	end 
and coalesce(billed_ind, '') = case when :billed_ind = 'NA' then coalesce(billed_ind, '') else :billed_ind end
and coalesce(pgi_status, '') = case when :pgi_status = 'NA' then coalesce(pgi_status, '') else :pgi_status end
and coalesce(multi_delv_single_plant_ind, '') = case when :multi_delv = 'NA' then coalesce(multi_delv_single_plant_ind, '') else :multi_delv end
and coalesce(rejected_ind, '') = case when :rejected_ind = 'NA' then coalesce(rejected_ind, '') else :rejected_ind end 
and coalesce(SAP_DELV_RELEVANT_ORDER_TYPE_IND, '') = case when :SAP_DELV_RELEVANT_ORDER_TYPE_IND = 'NA' then coalesce(SAP_DELV_RELEVANT_ORDER_TYPE_IND, '') else :SAP_DELV_RELEVANT_ORDER_TYPE_IND end
and coalesce(has_delivery_ind, '') = case when :has_delivery_ind = 'NA' then coalesce(has_delivery_ind, '') else :has_delivery_ind end
and coalesce(TM_LOAD_DATE_MISMATCH_IND, '') = case when :TM_LOAD_DATE_MISMATCH_IND = 'NA' then coalesce(TM_LOAD_DATE_MISMATCH_IND, '') else :TM_LOAD_DATE_MISMATCH_IND end
and coalesce(business_days_until_load_date, 999) between
	case when :days_until_load_date_low = 'NA' then coalesce(business_days_until_load_date, 999)
		else cast(:days_until_load_date_low as int)
	end
	and
	case when :days_until_load_date_high = 'NA' then coalesce(business_days_until_load_date, 999)
		else cast(:days_until_load_date_high as int)
	end
and coalesce(sap_planning_status, '') = case when :sap_planning_status = 'NA' then coalesce(sap_planning_status, '') else :sap_planning_status end
and coalesce(multi_fu_single_plant_ind, '') = case when :multi_fu_single_plant_ind = 'NA' then coalesce(multi_fu_single_plant_ind, '') else :multi_fu_single_plant_ind end
and coalesce(has_shipment_ind, '')  = case when :has_shipment_ind = 'NA' then coalesce(has_shipment_ind, '') else :has_shipment_ind end
and coalesce(sap_concept_cust_desc, '') = case when :sap_concept_cust_desc = 'NA' then coalesce(sap_concept_cust_desc, '') else :sap_concept_cust_desc end
and coalesce(business_unit, '') = case when :business_unit = 'NA' then coalesce(business_unit, '') else :business_unit end
and coalesce(confirmed_qty_discrep_ind, '') = case when :confirmed_qty_discrep_ind = 'NA' then coalesce(confirmed_qty_discrep_ind, '') else :confirmed_qty_discrep_ind end
and coalesce(sap_billing_date, '9999-12-31') between
	case when :sap_billing_date_start = 'NA' then coalesce(sap_billing_date, '9999-12-31')
		else cast(:sap_billing_date_start as date)
	end 
	and
	case when :sap_billing_date_end = 'NA' then coalesce(sap_billing_date, '9999-12-31')
		else cast(:sap_billing_date_end as date)
	end
	
and coalesce(MULTI_DELV_DATE_SINGLE_PLANT_IND, '') = case when :MULTI_DELV_DATE_SINGLE_PLANT_IND = 'NA' then coalesce(MULTI_DELV_DATE_SINGLE_PLANT_IND, '') else :MULTI_DELV_DATE_SINGLE_PLANT_IND end
and coalesce(MULTI_LOAD_DATE_SINGLE_PLANT_IND, '') = case when :MULTI_LOAD_DATE_SINGLE_PLANT_IND = 'NA' then coalesce(MULTI_LOAD_DATE_SINGLE_PLANT_IND, '') else :MULTI_LOAD_DATE_SINGLE_PLANT_IND end
and coalesce(MULTI_ROUTE_SINGLE_PLANT_IND, '') = case when :MULTI_ROUTE_SINGLE_PLANT_IND = 'NA' then coalesce(MULTI_ROUTE_SINGLE_PLANT_IND, '') else :MULTI_ROUTE_SINGLE_PLANT_IND end
and coalesce(MULTI_PLANT_FOR_NON_REJECTED_LINES_IND, '') = case when :MULTI_PLANT_FOR_NON_REJECTED_LINES_IND = 'NA' then coalesce(MULTI_PLANT_FOR_NON_REJECTED_LINES_IND, '') else :MULTI_PLANT_FOR_NON_REJECTED_LINES_IND end
and coalesce(MISSING_PLANT_FOR_NON_REJECTED_LINE_IND, '') = case when :MISSING_PLANT_FOR_NON_REJECTED_LINE_IND = 'NA' then coalesce(MISSING_PLANT_FOR_NON_REJECTED_LINE_IND, '') else :MISSING_PLANT_FOR_NON_REJECTED_LINE_IND end

and coalesce(order_fuzzy_week_num, 999) between
	case when :order_fuzzy_week_start = 'NA' then coalesce(order_fuzzy_week_num, 999)
		else cast(:order_fuzzy_week_start as int)
	end
	and
	case when :order_fuzzy_week_end = 'NA' then coalesce(order_fuzzy_week_num, 999)
		else cast(:order_fuzzy_week_end as int)
	end
and cast(coalesce(planned_pgi_date, '9999-12-31') as date) between
	case when :planned_pgi_date_start = 'NA' then cast(coalesce(planned_pgi_date, '9999-12-31') as date)
		else cast(:planned_pgi_date_start as date)
	end
	and
	case when :planned_pgi_date_end = 'NA' then cast(coalesce(planned_pgi_date, '9999-12-31') as date)
		else cast(:planned_pgi_date_end as date)
	end;
end;