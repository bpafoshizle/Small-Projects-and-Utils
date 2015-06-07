import java.sql.*;
 
public class TestDBOracle {
 
  public static void main(String[] args) 
      throws ClassNotFoundException, SQLException
  {
    Class.forName("oracle.jdbc.driver.OracleDriver");
    //
    // or
    // DriverManager.registerDriver 
    //        (new oracle.jdbc.driver.OracleDriver());
 
        String url = "jdbc:oracle:thin:@dbptsn3:1531:ptsn3";
    //               jdbc:oracle:thin:@//host:port/service
    // or 
    // String url = "jdbc:oracle:thin:@server.local:1521:prodsid";
    //               jdbc:oracle:thin:@host:port:SID
           
    Connection conn = 
         DriverManager.getConnection(url,"cogrpt", "cogrpt");
 
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    ResultSet rset = 
         stmt.executeQuery("SELECT '1.1' AS env, rr.COGIPF_MODEL, rr.COGIPF_REPORTNAME, rr.COGIPF_REPORTPATH, " +
						   "MAX(rr.COGIPF_LOCALTIMESTAMP) AS LAST_RUN_DATE FROM COGIPF_RUNREPORT rr " +
						   "WHERE rr.COGIPF_MODEL <> 'Financials' " +
						   "GROUP BY rr.COGIPF_MODEL, rr.COGIPF_REPORTNAME, rr.COGIPF_REPORTPATH " +
							"HAVING MAX(rr.COGIPF_LOCALTIMESTAMP) BETWEEN '" +
							args[0] + "'" + " AND (sysdate - " + args[1] + ") " +
							"--The above line limits the result set to only reports that have haven't been run " +
							"--since the last time the decommission program executed and some day relative to " +
							"--sysdate. In this case, that hasn't been run in the last 4 months. " +
							"--The '29-JUL-2008' is not necessary, it is just to ignore reports that have " +
							"--already been disabled in the past so that the decommission program runs quicker. " +
							"ORDER BY LAST_RUN_DATE"); 
    while (rset.next()) {
         System.out.println (rset.getString(4));  
    }
    stmt.close();
    System.out.println ("Ok.");  
  }
}