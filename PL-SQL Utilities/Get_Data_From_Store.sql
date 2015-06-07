 set serveroutput on;
 
 DECLARE
 
            p_WE_Date            VARCHAR2(40) := 'Current';
            p_Group_On           VARCHAR2(40) := 'AL';
            p_Sales_Group        NCLOB    := 'ALL SALES GROUPS';            --'020,022,030,040''ALL SALES GROUPS';;
            p_Sales_Unit         NCLOB    := 'ALL SALES UNITS';             --'ALL SALES UNITS''F01,F03,F04,F08'
            p_Business_Unit      NCLOB    := 'BUBBFO';          --'BUBBFO';'ALL BUSINESS UNITS';
            p_Allocated_Location NCLOB    := '21;W57';                    --'ALL LOCN';
            p_Master_Group       VARCHAR2(40) := 'NONE';
            p_Product_Group      NCLOB    := '004';                     --'032''ALL PGC';            
            p_Brand_Excl         NCLOB    := ' ';

            


   sql_Query            VARCHAR2(32700); 
   strSelect            VARCHAR2(9200);
   strFrom              VARCHAR2(1000);
   strInnerJoin         VARCHAR2(11000);
   strWhere             VARCHAR2(8700);
   strGroupBy           VARCHAR2(800);
   strGroupBy2          VARCHAR2(800);
   strOrderBy           VARCHAR2(1000);
 
   strGroupOn           VARCHAR2(1000);   
   strGroupOnDescr      VARCHAR2(1000);
   

   
BEGIN
 
  /*******************  Group Report On **********************/
 
  If p_Group_On = 'SG' then
      strGroupOn := 'RP.SALES_GRP_CODE';
      strGroupOnDescr := 'RP.SALES_GRP_DESCR';
  elsif p_Group_On = 'SU' then
      strGroupOn := 'RP.SALES_UNIT_CODE';
      strGroupOnDescr := 'RP.SALES_UNIT_DESCR';
  elsif p_Group_On = 'BU' then
      strGroupOn := 'RP.BUS_UNIT_CODE';
      strGroupOnDescr := 'RP.BUS_UNIT_DESCR'; 
  elsif p_Group_On = 'AL' then  
      strGroupON      := ' ''LOCN'' ';
      strGroupOnDescr := ' CASE ' ||
                      ' WHEN SHORTAGE_SIM_LOCN(RP.RESTATED_PRODUCT_KEY) IS NULL ' ||
                      ' THEN ''NPM'' ' ||
                      ' ELSE TRIM(SHORTAGE_SIM_LOCN(RP.RESTATED_PRODUCT_KEY)) ' ||
                      ' END ';

  elsif p_Group_On = 'MG' then
      strGroupOn := 'GD.GRP_CODE';
      strGroupOnDescr := 'GD.GRP_DESCR';
  end if; 

  /******************* SELECT **********************/  

    strSelect := ' SELECT SUB.WEEK_ENDING_DATE    AS WEEK_ENDING_DATE, ' ||
                 ' SUB.GROUP_ON                   AS GROUP_ON, '||
                 ' SUB.GROUP_ON_DESCR             AS GROUP_ON_DESCR, ' ||
                 ' SUB.SORT_ORD_NUM               AS SORT_ORD_NUM, ' ||
                 ' SUB.PRODUCT_GROUP_CODE         AS PRODUCT_GROUP_CODE, ' ||
                 ' SUB.PRODUCT_GROUP_DESCR        AS PRODUCT_GROUP_DESCR, ' ||
                 ' ''('' || SUB.PRODUCT_GROUP_CODE|| '') ''|| SUB.PRODUCT_GROUP_DESCR AS PRODUCT_GROUP, ' ||
                 ' SUM(SUB.ORDER_POUNDS)          AS ORDER_POUNDS, ' ||
                 ' SUM(SUB.INVOICE_POUNDS)        AS INVOICE_POUNDS, ' ||
                 ' SUM(SUB.ORDER_POUNDS) - SUM(SUB.INVOICE_POUNDS) AS SHORT_POUNDS ' ||
                 ' FROM ' ||
                          ' (SELECT DISTINCT DD.WEEK_ENDING_DATE AS WEEK_ENDING_DATE, ' ||
                          strGroupOn || ' AS GROUP_ON, ' ||
                          strGroupOnDescr || ' AS GROUP_ON_DESCR, ';
    If p_group_on = 'MG' then
       strSelect := strSelect || ' CASE ' || 
                                 ' WHEN GD.GRP_CODE = ''NONGROUP'' THEN 999 ' ||
                                 ' ELSE GD.SORT_ORD_NUM ' ||
                                 ' END  AS SORT_ORD_NUM, ';
    else
       strSelect := strSelect || ' ''1'' AS SORT_ORD_NUM, ';
    end if;
    
    strSelect := strSelect || ' RP.PRODUCT_GROUP_CODE  AS PRODUCT_GROUP_CODE, ' ||
                              ' RP.PRODUCT_GROUP_DESCR AS PRODUCT_GROUP_DESCR, ' ||
                              ' SUM(MF.POUNDS_ORDERED) AS ORDER_POUNDS, ' ||
                              ' SUM(MF.POUNDS_SHIPPED) AS INVOICE_POUNDS, ' ||
                              ' RP.BRAND_CODE ';    
   
 /******************* FROM **********************/   
        
     strFrom  :=  ' FROM TDW_OWNER.MARGIN_FACT_VIEW MF ';
     
 /**************** INNER JOINS *****************/    
     strInnerJoin := ' INNER JOIN TDW_OWNER.RESTATED_PRODUCT RP ON MF.RESTATED_PRODUCT_KEY   = RP.RESTATED_PRODUCT_KEY ' ||
                     ' INNER JOIN TDW_OWNER.DAY_DIM DD ON MF.INVOICE_DAY_KEY        = DD.DAY_KEY '  ;                     
                     --' INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Sales_Group ||''','',''))) SG_SET ON RP.SALES_GRP_CODE = SG_Set.VAL ' ||    
                     --' INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Sales_Unit ||''','',''))) SU_SET ON RP.SALES_UNIT_CODE = SU_Set.VAL ' ||     
                     --' INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Business_Unit ||''','',''))) BU_SET ON RP.BUS_UNIT_CODE = BU_Set.VAL ' ||
                     --' INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Allocated_Location ||''','',''))) AL_SET ON SHORTAGE_SIM_LOCN(RP.RESTATED_PRODUCT_KEY) = AL_Set.VAL ' ||
                     --' INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Product_Group ||''','',''))) PG_SET ON RP.PRODUCT_GROUP_CODE = PG_Set.VAL ';
   
     
     If p_Group_On = 'MG' then                                                             
       strInnerJoin := strInnerJoin || '  INNER JOIN TDW_OWNER.RESTATED_PRODUCT_GROUP_FACT GF ON RP.RESTATED_PRODUCT_KEY = GF.RESTATED_PROD_KEY'||
                                  '  INNER JOIN  TDW_OWNER.GROUP_DIM GD ON GF.GRP_DIM_KEY = GD.GRP_DIM_KEY ' ||
                                  '  INNER JOIN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Master_Group ||''', '',''))) MG_Set ON GD.MSTR_GRP_CODE = MG_Set.VAL ';
     end if;
     
     IF(p_Sales_Group <> 'ALL SALES GROUPS') THEN
              strInnerJoin := strInnerJoin || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Sales_Group ||''','',''))) SGC_SET ON RP.SALES_GRP_CODE = SGC_SET.VAL';
     END IF;
     
     IF(p_Sales_Unit <> 'ALL SALES UNITS') THEN
              strInnerJoin := strInnerJoin || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Sales_Unit ||''','',''))) SUC_SET ON RP.SALES_UNIT_CODE = SUC_SET.VAL';
     END IF;
     
     IF(p_Business_Unit <> 'ALL BUSINESS UNITS') THEN
              strInnerJoin := strInnerJoin || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Business_Unit ||''','',''))) BUC_SET ON RP.BUS_UNIT_CODE = BUC_SET.VAL';
     END IF;
     
     IF(p_Product_Group <> 'ALL PGC') THEN
              strInnerJoin := strInnerJoin || ' inner join (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Product_Group ||''','',''))) PGC_SET ON RP.PRODUCT_GROUP_CODE = PGC_SET.VAL';
     END IF;

  /******************* WHERE **********************/   
  
   strWhere := ' WHERE DD.WEEK_ENDING_DATE     = eval_date(''' || p_WE_Date || ''',''WE'') ';    
   
    IF (INSTR(p_Brand_Excl,' ',1,1)) = 0 THEN
       strWhere := strWhere ||
           ' AND  NOT BRAND_CODE IN (SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Brand_Excl ||''','',''))) ';
     end if;     

    IF (p_Allocated_Location <>'All Allocated Producing Locations') THEN
       strWhere := strWhere ||
           ' AND ' ||
               ' CASE ' ||
                 ' WHEN SHORTAGE_SIM_LOCN(RP.RESTATED_PRODUCT_KEY) IS NULL ' ||
                 ' THEN ''NPM'' ' ||
                 ' ELSE TRIM(SHORTAGE_SIM_LOCN(RP.RESTATED_PRODUCT_KEY)) ' ||
               ' END ' ||
           ' IN (SELECT TRIM(VAL) FROM TABLE(UFN_PARSECHARACTERCSVLIST('''|| p_Allocated_Location ||''','';''))) ';
    END IF;     
  
 /******************* GROUP BY *********************/   
  
   strGroupBy := ' GROUP BY  RP.BRAND_CODE, ' ||
                 '       DD.WEEK_ENDING_DATE, ' ||
                         strGroupOn || ', ' ||
                         strGroupOnDescr || ', ';
   If p_Group_On = 'MG' then
      strGroupBy := strGroupBy || ' SORT_ORD_NUM, ';
   else 
       strGroupBy := strGroupBy || '  ''1'' , ';
   end if;
   
   strGroupBy := strGroupBy ||
                 '       RP.PRODUCT_GROUP_CODE, ' ||
                 '       RP.PRODUCT_GROUP_DESCR ' ;

   
   strGroupBy := strGroupBy || ') SUB ';
   
  /********************************************/
   
  strGroupBy2 := ' GROUP BY  SUB.WEEK_ENDING_DATE,' ||
                 '           SUB.GROUP_ON, ' ||
                 '           SUB.GROUP_ON_DESCR, ' ||                           
                 '           SORT_ORD_NUM, ' ||
                 ' ''('' || SUB.PRODUCT_GROUP_CODE|| '') ''|| SUB.PRODUCT_GROUP_DESCR, ' ||
                 '       SUB.PRODUCT_GROUP_CODE, ' ||
                 '       SUB.PRODUCT_GROUP_DESCR ' ;
  
  /********************* ORDER BY *******************/

    strOrderBy := ' ORDER BY SORT_ORD_NUM, GROUP_ON, PRODUCT_GROUP ASC';    
  
 /*********************** QUERY *********************/
  sql_Query :=  strSelect ||  strFrom || strInnerJoin || strWhere || strGroupBy || strGroupBy2 || strOrderBy;

  dbms_output.put_line(sql_Query);
      

END;  
