/**
 * disableReport.java
 *
 * Copyright © 2005 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1013456) - SDK Java Sample to disable a report 
 */
import com.cognos.developer.schemas.bibus._3.*;
public class disableReport {
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetPortType oCrn = null;
	private CognosReportNetService crn  = null;
				
		public disableReport(String sendPoint)
		{
			//Connect to ReportNet
			try
			{
				service = new CognosReportNetServiceLocator();
				oCrn = service.getCognosReportNetPort(new java.net.URL(sendPoint));
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		public void disableMyReport(String reportSearchPath) 
		{
			PropEnum props[] = new PropEnum[] {PropEnum.searchPath,
											   PropEnum.disabled};
			Sort sOpt[] = new Sort[]{}; 
			QueryOptions qOpt = new QueryOptions(); 
			try
			{
				// retrieve the report object from content store 
				BaseClass report[] = oCrn.query(reportSearchPath, props, sOpt, qOpt);
				if (report.length == 0){
					System.out.println("Report is not found. Check the search Path.");
				}else{
				BooleanProp bp=new BooleanProp();
				bp.setValue(true);
				report[0].setDisabled(bp);
				
				BaseClass [] disabledReport =oCrn.update(report);
				System.out.println("Report is disabled");
				}
				
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
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
				System.out.println(e);
			}
			return ("Logon successful as " + uid);
		}
		public static void main(String[] args) 
		{
			// Variable that contains the default URL for dispatcher.			
			String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";

			// must be a user with full access to the report.
			String userName = "admin";
			String password = "password";
			String nameSpaceID = "SDK";
			
			// search path to the report to disable
			String reportSearchPath = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']";
			
			disableReport myReport = new disableReport(reportNetEndPoint);
			// use this logon code to logon 
			myReport.quickLogon(nameSpaceID,userName,password);
			
			myReport.disableMyReport(reportSearchPath);
		}
	}
