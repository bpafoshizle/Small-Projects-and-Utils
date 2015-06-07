set serveroutput on;
declare
c_cursor SYS_REFCURSOR;
type rec is record
                (
                Sort1 varchar2(20),
				Sort2 varchar2(20),
				Time_Level varchar2(20),
                Market_Detail varchar2(50),
				Product_Detail varchar2(50),
				Product_Detail_2 varchar2(50),
				Product_Detail_3 varchar2(50),
				CASES varchar2(15),
				POUNDS varchar2(15),
				NET_SALES varchar2(15),
				NET_SALES_PER_POUND varchar2(15)
                );
                rs rec;

begin

dbms_output.enable(1000000);

SP_RS_SALES_0178_TEST ('QTACURFY','131', '015', '038', '037', '999', '0', 'No', '0', 'No', '0', 'No', '0', 'No', '0All', 'No', '120000', '14-Aug-10', 'Customer', 'Custom', c_cursor);
         
loop

                                fetch c_cursor into rs;
                                exit when c_cursor%notfound;
                                dbms_output.put_line(rs.Sort1|| ' ' ||rs.Sort2||' '||rs.Time_Level||' '||rs.Market_Detail||' '||rs.Product_Detail||' '||rs.Product_Detail_2||' '||rs.Product_Detail_3||' '||rs.CASES||' '||rs.POUNDS||' '||rs.NET_SALES||' '||rs.NET_SALES_PER_POUND);
                end loop;
 
close c_cursor;

end;
