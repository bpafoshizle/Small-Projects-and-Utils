package cognos8;
/**
 * ReportProperties.java
 *
 * Copyright © 2004 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 * 
 * Description: (KB 1008110) - SDK Java Sample to Change a Report's Output Versions and Run History Property Settings
 *
 * 		This code sample demonstrates how to change a report's
 * 		'report output versions' property setting to 5 days and
 * 		its 'run history' property setting to 10 occurrences.
 *
 */

import com.cognos.developer.schemas.bibus._3.*;

import java.math.BigInteger;

public class ReportProperties
{

	private ContentManagerService_Port cmService = null;
	
	public void connectToReportServer (String endPoint) throws Exception
	{
		
		ContentManagerService_ServiceLocator cmServiceLocator = new ContentManagerService_ServiceLocator();
		try
		{
			cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(endPoint));
			
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
	
	public void logon(String namespace, String uid, String pwd) throws Exception
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
			cmService.logon(new XmlEncodedXML(encodedCredentials), null);
		}
		catch (Exception ex)
		{
			System.out.println("exception thrown " + ex);
			return;
		}

		System.out.println("Logon successful as " + uid);
	}

	public void setReportOutputVersionsAndRunHistorySettings(String reportSearchPath, String reportOutputVersionsDuration, int runHistoryOccurrences) throws Exception
	{
		PropEnum props[] = new PropEnum[]{PropEnum.searchPath};
										  
		BaseClass[] report = cmService.query(new SearchPathMultipleObject(reportSearchPath),props, new Sort[]{}, new QueryOptions());

		for(int i = 0; i < report.length; i++)
		{
			RetentionRuleArrayProp retentionRules = new  RetentionRuleArrayProp();
			RetentionRule[] outputRule = new RetentionRule[2];

			outputRule[0] = new RetentionRule();
			outputRule[0].setObjectClass(ClassEnum.fromString("reportVersion"));
			outputRule[0].setProp(PropEnum.creationTime);
			outputRule[0].setMaxDuration(reportOutputVersionsDuration);
				
			outputRule[1] = new RetentionRule();
			outputRule[1].setObjectClass(ClassEnum.fromString("history"));
			outputRule[1].setProp(PropEnum.creationTime);
			BigInteger max = BigInteger.valueOf(runHistoryOccurrences);
			outputRule[1].setMaxObjects(max);
				
			retentionRules.setValue(outputRule);
			((Report)report[i]).setRetentions(retentionRules);
			cmService.update(report,new UpdateOptions());
		}
	}
	
	public static void main(String args[])
	{
		// connection to the Cognos service
		String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";
		// log in as a System Administrator to ensure you have the
		// necessary permissions to change the report property settings
		String namespaceID = "namespaceID";
		String userID = "userID";
		String password = "password";
		
		
		
		// search path of the report whose properties will be changed
		String reportSearchPath = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']";
		//This format indicates 5 days.  The format for 5 months would be P5M.
		String reportOutputVersionsDuration = "P2D";
		int runHistoryOccurrences = 20;
		ReportProperties test = new ReportProperties();
		
		try
		{
			test.connectToReportServer(endPoint);
			test.logon(namespaceID, userID, password);
			test.setReportOutputVersionsAndRunHistorySettings(reportSearchPath, reportOutputVersionsDuration, runHistoryOccurrences);
			System.out.println("\nDone.");
		}
		catch (Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
	}
}
