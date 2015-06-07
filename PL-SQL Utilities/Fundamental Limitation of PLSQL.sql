DECLARE 

  v_Query varchar2(32767);
  type r_cursor is ref cursor;
  resultSet r_cursor;
  
  FUNCTION returnDynamicSQL(pSellingGroup in nclob,
                            pDivision in nclob,
                            pRegion in nclob,
                            pPrimaryBroker in nclob,
                            pShipTo in nclob,
                            pWeekBegin in date,
                            pWeekEnd in date,
                            pGrouping in varchar2)
  RETURN VARCHAR2
                                              
  IS
  
      -- SQL String
      qSelectString varchar2(10000) := '';
      qFromString varchar2(1000) := '';
      qWhereString varchar2(5000) := '';
      qGroupByString varchar2(10000) := '';
      qQuery varchar2(32767) := '';
  
  BEGIN
      
      /* ------------------------ Select Portion ------------------------ */
      qSelectString := 'select rm.selling_group_code,
                        rm.selling_group_descr,
                        ''('' || rm.selling_group_code || '')'' || rm.selling_group_descr AS selling_group_code_descr';
      
      -- Add division
      if(pGrouping = 'DV' or pGrouping = 'RG' or pGrouping = 'PB' or pGrouping = 'ST') then
      qSelectString := qSelectString || ', rm.division_code,
                        rm.division_descr,
                        ''('' || rm.division_code || '')'' || rm.division_descr AS division_code_descr';
      end if;
      
      -- Add region
      if(pGrouping = 'RG' or pGrouping = 'PB' or pGrouping = 'ST') then
      qSelectString := qSelectString || ', rm.region_code,
                         rm.region_descr,
                         ''('' || rm.region_code || '')'' || rm.region_descr AS region_code_descr';
      end if;
      
      -- Add primary broker
      if(pGrouping = 'PB' or pGrouping = 'ST') then
      qSelectString := qSelectString || ', rm.primary_broker_code,
                        rm.primary_broker_descr,
                        ''('' || rm.primary_broker_code || '')'' || rm.primary_broker_descr AS primary_broker_code_descr';
      end if;
      
      -- Add ship to
      if(pGrouping = 'ST') then
      qSelectString := qSelectString || ', rm.ship_to_number,
                        rm.ship_to_name,
                        ''('' || rm.ship_to_number || '')'' || rm.ship_to_name AS ship_to_num_name';
      end if;
      
      -- Always selected
      qSelectString := qSelectString || ', rp.material_number_value,
                sum(nvl(mfv.pounds_shipped, 0)) AS SHIPPED_POUNDS, 
                sum(nvl(mfv.pounds_sample, 0)) AS SAMPLE_POUNDS,
                sum(nvl(mfv.pounds_shipped, 0) - nvl(mfv.pounds_sample, 0)) as NET_POUNDS,
                sum(nvl(mfv.primary_brokerage, 0)) AS PRIMARY_BROKERAGE,
                sum(nvl(mfv.secondary_brokerage, 0)) AS SECONDARY_BROKERAGE,
                sum(nvl(mfv.total_brokerage, 0)) AS BROKERAGE_TOTAL';
      
      /* ------------------------ From portion ------------------------ */
      qFromString := ' from tdw_owner.margin_fact_view mfv
              inner join tdw_owner.restated_market rm on mfv.restated_market_key = rm.restated_market_key
              inner join tdw_owner.restated_product rp on mfv.restated_product_key = rp.restated_product_key
              inner join tdw_owner.day_dim dd on mfv.invoice_day_key = dd.day_key';
              
              if(pSellingGroup <> ' ') then
                qFromString := qFromString || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| pSellingGroup ||''','',''))) sgc on rm.selling_group_code = sgc.val';
              end if;
              
              if(pDivision <> ' ') then
                qFromString := qFromString || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| pDivision ||''','',''))) dc on rm.division_code = dc.val';
              end if;
              
              if(pRegion <> ' ') then
                qFromString := qFromString || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST(''' || pRegion || ''','',''))) rc on rm.region_code = rc.val';
              end if;
              
              if(pPrimaryBroker <> ' ') then
                qFromString := qFromString || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST(''' || pPrimaryBroker || ''','',''))) pbc on rm.primary_broker_code = pbc.val';
              end if;
        
              if(pShipTo <> ' ') then
                qFromString := qFromString || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST(''' || pShipTo || ''','',''))) stn on rm.ship_to_number = stn.val';
              end if;
      
      /* ------------------------ Where Portion ------------------------ */
        qWhereString := ' where dd.week_ending_date between ''' || pWeekBegin || ''' and ''' || pWeekEnd || '''';
        
      
      /* ------------------------ Group by portion ------------------------ */
      qGroupByString := ' group by rm.selling_group_code,
                 rm.selling_group_descr,
                           ''('' || rm.selling_group_code || '')'' || rm.selling_group_descr';
        
      -- Add division
      if(pGrouping = 'DV' or pGrouping = 'RG' or pGrouping = 'PB' or pGrouping = 'ST') then
        qGroupByString := qGroupByString || ', rm.division_code,
                          rm.division_descr,
                          ''('' || rm.division_code || '')'' || rm.division_descr';
      end if;
    
      -- Add region
      if(pGrouping = 'RG' or pGrouping = 'PB' or pGrouping = 'ST') then
        qGroupByString := qGroupByString || ', rm.region_code,
                           rm.region_descr,
                           ''('' || rm.region_code || '')'' || rm.region_descr';
      end if;
    
      -- Add primary broker
      if(pGrouping = 'PB' or pGrouping = 'ST') then
        qGroupByString := qGroupByString || ', rm.primary_broker_code,
                          rm.primary_broker_descr,
                          ''('' || rm.primary_broker_code || '')'' || rm.primary_broker_descr';
      end if;
    
      -- Add ship to
      if(pGrouping = 'ST') then
        qGroupByString := qGroupByString || ', rm.ship_to_number,
                          rm.ship_to_name,
                          ''('' || rm.ship_to_number || '')'' || rm.ship_to_name';
      end if;
      
      -- Always group by material number
      qGroupByString := qGroupByString || ', rp.material_number_value';
      
      qQuery := qSelectString || qFromString || qWhereString || qGroupByString;
  
      return qQuery;
  END;
BEGIN

v_Query := returnDynamicSQL('125000','141','137','867','26719','03-APR-10','03-APR-10','ST'); 

open resultSet for v_Query;

/* Could loop through and fetch each row, but you have to know ahead of time 
   what columns will be returned. With the Dynamic SQL, the dataset is variable. */
  
END;