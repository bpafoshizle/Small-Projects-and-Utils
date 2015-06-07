set serveroutput on;
 
 DECLARE 
    cursor rcSellingGroupCodes is SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('125000',','));
    cursor rcDivisionCodes is SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('141',','));
    cursor rcRegionCodes is SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('137',','));
    cursor rcPrimaryBroker is SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('867',','));
    cursor rcShipTo is SELECT VAL FROM TABLE(UFN_PARSECHARACTERCSVLIST('26719',','));
    
    sgCode varchar2(20);
    dCode varchar2(10);
    rCode varchar2(10);
    pbCode varchar2(10);
    stCode varchar2(20);
    
    counter number(38) := 0;
 
 BEGIN
  for sgRec in rcSellingGroupCodes loop
    dbms_output.put_line('Selling Group | ' || sgRec.val);
  end loop;
  
  for rcRec in rcDivisionCodes loop
    dbms_output.put_line('Division      | ' || rcRec.val);
  end loop;

END;