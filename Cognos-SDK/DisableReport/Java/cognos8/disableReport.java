/**
 * disableReport.java
 *
 * Copyright © 2005 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1013456) - SDK Java Sample to disable a report 
 */
import java.net.MalformedURLException;
import javax.xml.rpc.ServiceException;
import com.cognos.developer.schemas.bibus._3.*;

public class disableReport {
	private ContentManagerService_Port cmService = null;
				
		public disableReport(String sendPoint)
		{
			ContentManagerService_ServiceLocator cmServiceLocator = new ContentManagerService_ServiceLocator();
			try {
				cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(sendPoint));
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (ServiceException e) {
				e.printStackTrace();
			}
		}
		public void disableMyReport(String reportSearchPath) 
		{
			PropEnum props[] = new PropEnum[] {PropEnum.searchPath,
											   PropEnum.disabled};
			Sort sOpt[] = new Sort[]{}; 
			QueryOptions qOpt = new QueryOptions(); 
			SearchPathMultipleObject spMulti = new SearchPathMultipleObject(reportSearchPath);
			try
			{
				// retrieve the report object from content store 
				BaseClass report[] = cmService.query(spMulti, props, sOpt, qOpt);
				if (report.length == 0){
					System.out.println("Report is not found. Check the search Path.");
				}else{
				BooleanProp bp=new BooleanProp();
				bp.setValue(true);
				report[0].setDisabled(bp);
				
				BaseClass [] disabledReport =cmService.update(report, new UpdateOptions());
				System.out.println("Report is disabled");
				}
				
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
		}
		
		//This method loggs the user to Cognos 8
		public String quickLogon(String namespace, String uid, String pwd)
		{
			StringBuffer credentialXML = new StringBuffer();

			credentialXML.append("<credential>");
			credentialXML.append("<namespace>").append(namespace).append("</namespace>");
			credentialXML.append("<username>").append(uid).append("</username>");
			credentialXML.append("<password>").append(pwd).append("</password>");
			credentialXML.append("</credential>");

			String encodedCredentials = credentialXML.toString();
		    XmlEncodedXML xmlCredentials = new XmlEncodedXML();
		    xmlCredentials.setValue(encodedCredentials);
		 
		   try
			{		
				cmService.logon(xmlCredentials,null );
			}
			catch (Exception e)
			{
				System.out.println(e);
			}
			
			return ("Logon successful as " + uid);
		}
	
		
		public static void main(String[] args) 
		{
			// Variable that contains the default URL for Dispatcher.			
			String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";

			// must be a user with full access to the report.
			String userName = "admin";
			String password = "password";
			String nameSpaceID = "SDK";
			
			// search path to the report to disable
			String reportSearchPath = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']";
			
			disableReport myReport = new disableReport(endPoint);
			// use this logon code to logon 
			myReport.quickLogon(nameSpaceID,userName,password);
			
			myReport.disableMyReport(reportSearchPath);
		}
	}
