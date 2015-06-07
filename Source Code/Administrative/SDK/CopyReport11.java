/**
 * CopyReport11.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: KB (1010092) - CRN SDK Sample to copy reports from one users 'My Folders' to another 'My Folders'
 *
 */

import com.cognos.developer.schemas.bibus._2.*;

public class CopyReport11
{
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetPortType oCrn = null;
	private CognosReportNetService crn  = null;

	public CopyReport11(String sendPoint)
	{
		//Connect to ReportNet
		try
		{
			service = new CognosReportNetServiceLocator();
			oCrn = service.getCognosReportNetPort(new java.net.URL(sendPoint));
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}

	public void copyTheReport(String fromSearchPath, String toSearchPath)
	{
		try
		{
			// Will copy all reports in My Folders to the specified user's My Folders
			PropEnum props[] = new PropEnum[]{PropEnum.searchPath, PropEnum.objectClass,
									PropEnum.defaultName, PropEnum.portalPage};

			// Query the Content Store for all reports in My Folders for user userName
			BaseClass bc[] = oCrn.query(fromSearchPath, props, new Sort[]{}, new QueryOptions());

			if (bc != null && bc.length > 0)
			{
				//Display all objects in My Folders for user userName
				System.out.println("Objects in My Folders for user " + fromSearchPath);
				for (int i=0; i<bc.length; i++)
				{
					Report toAdd = (Report)bc[i];
					System.out.println(toAdd.getObjectClass().getValue().getValue() + " - " + toAdd.getDefaultName().getValue());
				}
				CopyOptions co = new CopyOptions();
				// NOTE: This will replace existing reports.
				co.setUpdateAction(UpdateActionEnum.replace);
				oCrn.copy(bc,toSearchPath,co);
			}
			else
			{
				System.out.println("No objects found in My Folders for user " + fromSearchPath);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
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

    public static void main(String args[])
	{
		// Variable that contains the default URL for CRN Content Manager.
		String reportNetEndPoint = "http://reprd2:9300/crn11mr2/cgi-bin/cognos.cgi";

		// the searchPath's of the my Folders for the users to copy from and to.
		String searchPathFrom = "CAMID(\"ADS:u:cn=miller\\, barret,ou=new,ou=groups,ou=general\")/folder[@name='My Folders']//report";
		String searchPathTo =   "CAMID(\"ADS:u:cn=haury\\, jesse, ou=new,ou=groups,ou=general\")/folder[@name='My Folders']";
		// log on to ReportNet, - you must logon as an administrator
		// in order to have permission to copy objects to other users.
		String userName = "CognosReportNet";
		String password = "En(Rp(CGNS10";
		String nameSpaceID = "ADS";

		CopyReport11 obj = new CopyReport11(reportNetEndPoint);

		String logonResults = obj.quickLogon(nameSpaceID, userName, password);
		System.out.println(logonResults);

	    // open the local .csv file and read the 1st row.
	    obj.copyTheReport(searchPathFrom,searchPathTo);  // change format to (PDF, CSV, HTML, HTMLFragment, XML)
	    System.out.println("Copy reports complete.");
	}
}

