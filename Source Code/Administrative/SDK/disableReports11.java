/**
 * disableReport.java
 *
 * Copyright © 2005 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1013456) - CRN SDK Java Sample to disable a report
 */
import com.cognos.developer.schemas.bibus._2.*;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;

public class disableReports11 {
		private CognosReportNetServiceLocator service = null;
		private CognosReportNetPortType oCrn = null;
		private CognosReportNetService crn  = null;
		
		// Variable that contains the default URL for CRN Content Manager.
		private String endPoint = "http://reprd2:9300/p2pd/servlet/dispatch";

		public disableReports11(String userName, String password)
		{
			//Connect to ReportNet
			try
			{
				String nameSpaceID = "ADS";
				String endPoint = "http://reprd2:9300/p2pd/servlet/dispatch";
				
				service = new CognosReportNetServiceLocator();
				oCrn = service.getCognosReportNetPort(new java.net.URL(endPoint));

				// use this logon code to logon
				// must be a user with full access to the report.
				quickLogon(nameSpaceID,userName,password);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				//System.out.println("hey1");
				System.exit(0);
			}
		}
		
		public void disableMyReports(String startDate, int daysBack, BufferedReader console) throws IOException
		{
			BufferedWriter errorWriter = new BufferedWriter(new FileWriter(new File("objectsNotFound11.txt")));
			BufferedWriter successWriter = new BufferedWriter(new FileWriter(new File("objectsFound11.txt")));

			PropEnum props[] = new PropEnum[] {PropEnum.searchPath,
											   PropEnum.disabled};
			Sort sOpt[] = new Sort[]{};
			QueryOptions qOpt = new QueryOptions();
			
			ArrayList reportPaths;
			ArrayList exceptionPaths;
			
			try
			{
				reportPaths = getPathToDisableList(startDate, daysBack);
				
				/* Debugging */
				reportPaths = new ArrayList();
				reportPaths.add("/content/package[@name='Product Cost']/report[@name='(1205) Product Trend']");
				/*Debugging */
				
				exceptionPaths = getExceptionPathList();
				
				writePathToDisableList(reportPaths);
			
				System.out.println("Please check the file: objectsToDisable.txt in the program executable folder " +
								  "to make sure everything looks good. Do you want to proceed? Enter 'yes' or 'no' (case insensitive).");
				
				String userChoice = console.readLine();
				
				if(userChoice.equalsIgnoreCase("yes"))
				{
					System.out.println("User entered 'yes'");
					String path;

					for (int i = 0; i < reportPaths.size(); i++)
					{
						path = (String)reportPaths.get(i);
						
						// Only disable the report if it is not in the exception list
						if(!checkAgainstExceptions(path, exceptionPaths))
						{
							System.out.println("Exception not caught");
							if((i%100)==0)
							{
								System.out.println(i);
							}
							try
							{
								// retrieve the report object from content store
								BaseClass report[] = oCrn.query(path, props, sOpt, qOpt);
								if (report.length > 0)
								{
									BooleanProp bp=new BooleanProp();
									bp.setValue(true);
									report[0].setDisabled(bp);
									BaseClass [] disabledReport =oCrn.update(report);
									successWriter.write(path + "\n");
								}
								else
								{
									errorWriter.write(path + "\n");
								}

							}
							catch (Exception e)
							{
								//System.out.println(e);
								//System.out.println("hey5");
								errorWriter.write(path + "\n");
							}
						}
					}
				}

				errorWriter.close();
				successWriter.close();
				
			}
			catch (Exception e)
			{
				e.printStackTrace();
				//System.out.println("hey2");
				System.exit(0);
			}
		}
		
		/**
		 * Method to test whether the path is in our exception list
		 */
		private boolean checkAgainstExceptions(String path, ArrayList exceptionPaths)
		{
			return exceptionPaths.contains(path);
		}
		
		/**
		 * Method to query the oracle content store and retrieve a list of report paths to disable
		 */
		private ArrayList getPathToDisableList(String startDate, int daysBack) 
			throws ClassNotFoundException, SQLException
		{
			ArrayList reportPaths = new ArrayList();
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
									startDate + "'" + " AND (sysdate - " + daysBack + ") " +
									"--The above line limits the result set to only reports that have haven't been run " +
									"--since the last time the decommission program executed and some day relative to " +
									"--sysdate. In this case, that hasn't been run in the last 4 months. " +
									"--The '29-JUL-2008' is not necessary, it is just to ignore reports that have " +
									"--already been disabled in the past so that the decommission program runs quicker. " +
									"ORDER BY LAST_RUN_DATE"); 
			while (rset.next()) {
				 reportPaths.add(rset.getString(4));
				 //System.out.println(rset.getString(4));
			}
			stmt.close();
			//System.out.println ("Ok.");  
			return reportPaths;
		}
		
		/**
		 * Method to read the exception report paths from a file
		 * located in the same directory as the excutable
		 */
		private ArrayList getExceptionPathList() throws IOException
		{
			BufferedReader reader = new BufferedReader(new FileReader(new File("exceptions.txt")));
			ArrayList temp = new ArrayList();
			
			String path = reader.readLine();
			
			while(path != null)
			{
				temp.add(path);
				path = reader.readLine();
			}
			
			reader.close();
			
			return temp;
		}
		
		/** 
		 * Method to write out all the paths of the reports that will be disabled
		 */
		private void writePathToDisableList(ArrayList reportPaths) throws IOException
		{
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File("objectsToDisable.txt")));
			
			for(int i = 0; i < reportPaths.size(); i++)
			{
				writer.write((String)reportPaths.get(i));
				writer.newLine();
			}
			
			writer.close();
		}
		
		//this method will login the user to ReportNet
		public String quickLogon(String namespace, String uid, String pwd)
		{
			StringBuffer credentialXML = new StringBuffer();

			credentialXML.append("<credential>");
			credentialXML.append("<namespace>").append(namespace).append("</namespace>");
			credentialXML.append("<username>").append(uid).append("</username>");
			credentialXML.append("<password>").append(pwd).append("</password>");
			credentialXML.append("</credential>");

			String encodedCredentials = credentialXML.toString();

			try
			{
				oCrn.logon(encodedCredentials, new String[]{});
			}
			catch (Exception e)
			{
				//System.out.println("hey3");
				e.printStackTrace();
			}
			return ("Logon successful as " + uid);
		}
		
		/**
		 * Command line arguments: Date of the last run (used to 
		 * limit searched data and make the query faster, but not necessary 
		 * to get the correct result set) in the form dd-mmm-yyyy
		 * and number of days to look back from the current date.
		 */
		public static void main(String[] args) throws IOException
		{
		
			if(!(args.length < 2))
			{
				try
				{
					//  open up standard input and get login information
					BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
					System.out.println("Username: ");
					String user = console.readLine();
					System.out.println("Password: ");
					String pass = console.readLine();
					
					disableReports11 disabler = new disableReports11(user, pass);
					disabler.disableMyReports(args[0], Integer.parseInt(args[1]), console);
				}
				catch(Exception e)
				{
					System.out.println("Usage: java disableReports11 startDate(dd-mmm-yyyy), daysBack");
					//System.out.println("hey4");
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Usage: java disableReports11 startDate(dd-mmm-yyyy), daysBack");
			}

			
		}
	}
