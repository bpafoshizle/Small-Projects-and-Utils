/**
 * SecurityOverview.java
 * Description: KB #1011543 - How to use the Cognos Software Development Kit to define all Groups/Roles and their respective Users
 */

 // Barret - editiing to test source safe changes.

import java.io.*;
import com.cognos.developer.schemas.bibus._3.*;

public class SecurityOverview
{

	private ContentManagerServiceStub cmStub = null;

	public SecurityOverview(String sendPoint)
	{
		 ContentManagerService_ServiceLocator cmServiceLocator = new ContentManagerService_ServiceLocator();
	      try {
			cmStub = new ContentManagerServiceStub(new java.net.URL(sendPoint),cmServiceLocator);

			// Set the Axis request timeout
			// in milliseconds, 0 turns the timeout off
			cmStub.setTimeout(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public Account[] getMembers(String targetSearchPath)
	{
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath, PropEnum.defaultName,
										   PropEnum.portalPage};
		Sort sOpt[] = new Sort[]{};
		QueryOptions qOpt = new QueryOptions();
		SearchPathMultipleObject spMulti = new SearchPathMultipleObject();
		spMulti.setValue(targetSearchPath);
		Account[] targetAccount = null;

		try
		{
			// get the target user's account object.
			BaseClass targets[] = cmStub.query(spMulti, props, sOpt, qOpt);
			// initialize targetAccount
			targetAccount = new Account[targets.length];
			for (int x=0;x<targetAccount.length;x++)
			{
				targetAccount[x] = (Account)targets[x];
			}

		}
		catch (Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
		return targetAccount;

	}

	//This method loggs the user to ReportNet
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

	   //Invoke the ContentManager service logon() method passing the credential string
	   //You will pass an empty string in the second argument. Optionally,
	   //you could pass the Role as an argument but for the purpose of this
	   //workshop don’t be concerned with Roles.

	   try
		{
			cmStub.logon(xmlCredentials,null );
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
		return ("Logon successful as " + uid);
	}

	public BaseClass[] getRoles(String nameSpaceID)
	{
		// build the searchpath to retrieve the roles
		String roles = "CAMID(\""+nameSpaceID+"\")//role";
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath,PropEnum.defaultName};
		BaseClass roleObjects[] = new BaseClass[]{};
		SearchPathMultipleObject spMulti = new SearchPathMultipleObject();
		spMulti.setValue(roles);

		try
		{
			roleObjects = cmStub.query(spMulti, props, new Sort[]{}, new QueryOptions());
		}
		catch (Exception e)
		{
		 	System.out.println(e);
		 	e.printStackTrace();
		}

		String[] roleSearchPaths = new String[roleObjects.length];

		if (!(roleObjects.length != 0))
		{
			System.out.println("There were no Roles Found\n");
		}

	 	return roleObjects;

	}

	public BaseClass[] getGroups(String nameSpaceID)
	{
		// build the searchpath to retrieve the Groups
		String groups = "CAMID(\""+nameSpaceID+"\")//group";
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath,PropEnum.defaultName};
		BaseClass groupObjects[] = new BaseClass[]{};
		SearchPathMultipleObject spMulti = new SearchPathMultipleObject();
		spMulti.setValue(groups);

		try
		{
			groupObjects = cmStub.query(spMulti, props, new Sort[]{}, new QueryOptions());
		}
		catch (Exception e)
		{
		 	System.out.println(e);
		 	e.printStackTrace();
		}

		String[] groupSearchPaths = new String[groupObjects.length];
		if (!(groupObjects.length != 0))
		{
			System.out.println("There were no Groups Found\n");
		}

	 	return groupObjects;

	}

	public static void main(String[] args)
	{
		// Variable that contains the default URL for CRN Content Manager.
		String reportNetEndPoint = "http://cogpbi1:9300/p2pd/servlet/dispatch";
		// must be a user with System administrator privledges.
		String userName = "CognosReportNet";
		String password = "En(Rp(CGNS10";
		String nameSpaceID = "ADS";
		String fileName = "/cognos/dev/performcentral/namespaceConfig/Cognos_Namespace_Configuration.txt";

		SecurityOverview so = new SecurityOverview(reportNetEndPoint);
		// use this logon code to logon as a system administrator
		so.quickLogon(nameSpaceID,userName,password);

		FileOutputStream fs=null;
		try
		{
			fs = new FileOutputStream(fileName);
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		PrintStream fout = new PrintStream(fs);

		// define the namespace to query - : means the Cognos namespace
		String nsToQuery = ":";

		BaseClass[] roleIDs = so.getRoles(nsToQuery);
		BaseClass[] groupIDs = so.getGroups(nsToQuery);
		Account[] targetAccount=null;

		for (int x=0;x<roleIDs.length;x++)
		{
			System.out.println("\nRole: "+roleIDs[x].getDefaultName().getValue());
			fout.println("\nRole: "+roleIDs[x].getDefaultName().getValue());
			targetAccount = so.getMembers("expandMembers("+roleIDs[x].getSearchPath().getValue()+")");
			// Now print out the retrieved info
			if (targetAccount.length!=0)
			{
				for (int i=0;i<targetAccount.length;i++)
				{
					targetAccount[i]= (Account)targetAccount[i];
					fout.println("      "+targetAccount[i].getDefaultName().getValue());
					System.out.println("      "+targetAccount[i].getDefaultName().getValue());
				}
			}
			else
			{
				System.out.println("No Members Found");
				fout.println("No Members Found");
			}
		}

		for (int x=0;x<groupIDs.length;x++)
		{
			// do not perform on groups that do not have specific members defined
			String allusers = "All Authenticated Users";
			String everyone = "Everyone";
			boolean test1 = groupIDs[x].getDefaultName().getValue().equals(allusers);
			boolean test2 = groupIDs[x].getDefaultName().getValue().equals(everyone);
			if ( !test1 && !test2  )
			{
				System.out.println("\nGroup: "+groupIDs[x].getDefaultName().getValue());
				fout.println("\nGroup: "+groupIDs[x].getDefaultName().getValue());
				targetAccount = so.getMembers("expandMembers("+groupIDs[x].getSearchPath().getValue()+")");
				// Now print out the retrieved info
				if (targetAccount.length!=0)
				{
					for (int i=0;i<targetAccount.length;i++)
					{
						targetAccount[i]= (Account)targetAccount[i];
						fout.println("      "+targetAccount[i].getDefaultName().getValue());
						System.out.println("      "+targetAccount[i].getDefaultName().getValue());
					}
				}
				else
				{
					System.out.println("No Members Found");
					fout.println("No Members Found");
				}
			}
		}


	}
}
