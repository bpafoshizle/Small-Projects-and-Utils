package crn;
/**
 * SecurityOverview.java
 * 
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: KB #1011543 - How to use the Cognos Software Development Kit to define all Groups/Roles and their respective Users
 */

import java.io.*;

import com.cognos.developer.schemas.bibus._2.*;

public class SecurityOverview 
{
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetBindingStub oCrn = null;
	private CognosReportNetService crn  = null;
		
	public SecurityOverview(String sendPoint)
	{
		//Connect to ReportNet
		try
		{
			service = new CognosReportNetServiceLocator();
			oCrn = new CognosReportNetBindingStub(new java.net.URL(sendPoint), service);

			// Set the Axis request timeout
			oCrn.setTimeout(0);  // in milliseconds, 0 turns the timeout off
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
	public Account[] getMembers(String targetSearchPath) 
	{
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath, PropEnum.defaultName,
										   PropEnum.portalPage};
		Sort sOpt[] = new Sort[]{}; 
		QueryOptions qOpt = new QueryOptions(); 
		Account[] targetAccount = null;
		try
		{
			// get the target user's account object. 
			BaseClass targets[] = oCrn.query(targetSearchPath, props, sOpt, qOpt);
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
		//this method will login the user to ReportNet
	public void quickLogon(String namespace, String uid, String pwd)
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
	}
	
	public BaseClass[] getRoles(String nameSpaceID)
	{
		// build the searchpath to retrieve the roles
		String roles = "CAMID(\""+nameSpaceID+"\")//role";
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath,PropEnum.defaultName};
		BaseClass roleObjects[] = new BaseClass[]{};

		try
		{
			roleObjects = oCrn.query(roles, props, new Sort[]{}, new QueryOptions());
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

		try
		{
			groupObjects = oCrn.query(groups, props, new Sort[]{}, new QueryOptions());
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
		String reportNetEndPoint = "http://localhost/crn/cgi-bin/cognos.cgi";
		// must be a user with System administrator privledges.
		String userName = "administrator"; 
		String password = "password";
		String nameSpaceID = "SDK";
		String fileName = "C:/Cognos Namespace Configuration.txt";

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
