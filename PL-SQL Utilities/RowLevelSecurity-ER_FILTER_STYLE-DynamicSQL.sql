set serveroutput on;

DECLARE 
  pUserID VARCHAR2(20) := 'JILEKJ';
  pFilterName VARCHAR2(40) := 'RESTATED_MARKET_SIM_DIM';
  
  filterText VARCHAR(4000);
  queryOutput VARCHAR2(10000) := 'select * from some_table where ';
  
BEGIN

  /* Get the proper filter from the ER Filters table based on supplied username 
     and the filter_name (report_name) you want */ 
  select EF.FILTER_DESCR 
  into filterText 
  from ERPT_OWNER.ER_TABLE_FILTERS EF 
  where EF.FILTER_NAME = pFilterName and EF.FILTER_USERID = pUserID;
  
  /* Combine filter to where clause */
  queryOutput := queryOutput || filterText;
  
  dbms_output.put_line(queryOutput);
  
END;
  
  