/**
 * CopyPersonalPages.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1011181) - How to define all exisitng Groups and Roles and then perform an action on the associated members. 
 * 
 */

import com.cognos.developer.schemas.bibus._2.*;

public class CopyPersonalPages 
{
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetBindingStub oCrn = null;
	private CognosReportNetService crn  = null;
	private String templateSearchPath = null;
	
	/**
	 * Connect to ReportNet
	 */
	public CopyPersonalPages(String sendPoint)
	{		
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
	
	/**
	 * This method will add/update the user's My Pages
	 */
	public void updatePages(String templateSearchPath, String targetSearchPath) 
	{
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath,
										   PropEnum.portalPage};
		Sort sOpt[] = new Sort[]{}; 
		QueryOptions qOpt = new QueryOptions(); 
		try
		{
			// get the template user's Account object. 
			BaseClass template[] = oCrn.query(templateSearchPath, props, sOpt, qOpt);
			Account templateAccount = (Account)template[0];
			// grab their portal page. 
			AnyTypeProp templatePortalPage = templateAccount.getPortalPage(); 
			
			// check we have template pages to copy
			if (templatePortalPage.getValue().lastIndexOf("</mypage>")!=-1)
			{
					
				// get the target user's account object. 
				BaseClass targets[] = oCrn.query(targetSearchPath, props, sOpt, qOpt);
				
				if (targets != null && targets.length > 0)
				{
				
				// initialize targetAccount
				Account targetAccount = (Account)targets[0];
				
				// update the target accounts' portal page with the portal page from the template user. 
				for(int i=0;i<targets.length;i++)
				{
					targetAccount = (Account)targets[i]; 
					
					// now grab the target users portal page
					AnyTypeProp targetAccountPortalPage = targetAccount.getPortalPage(); 
					//Since we do not want to overwrite any existing portal pages, we need to merge the two
					// Note that if using this to distribute a template, it will work fine the first time
					// and will create duplicates thereafter. 
					
					// this will check to see if user has their own pages and if one of them
					// has the title identifying the template. If so, we need to overwrite them.
					// In this example, the template title is "New Page"
					if ( targetAccountPortalPage.getValue() != null) 
					{
							if (targetAccountPortalPage.getValue().indexOf("New Page")>0)
							// Note: if user has mulitple pages, the portal pages value will need to be
							// parsed to extract only pages that are to remain. Additional logic is required.
		
							{
								// do nothing since the page will be overwritten below
							}
							else if (targetAccountPortalPage.getValue().endsWith("</mypage>")) 
								// ie. target user has existing pages but none are the template
								// we will therefore merge them
							{
								String temp1 =  templatePortalPage.getValue();
								int index = temp1.lastIndexOf("</mypage>");
								temp1 = temp1.substring(0,index);
								String temp2 = targetAccountPortalPage.getValue();
								index = temp2.indexOf(">");
								temp2 = temp2.substring(index+1);
								String finalPortalPages = temp1.concat(temp2);
								templatePortalPage.setValue(finalPortalPages);	
							}					
					}
					targetAccount.setPortalPage(templatePortalPage);
					System.out.println("updated my pages for user: " 
							+ targetAccount.getSearchPath().getValue());
				
				}			
					targets = oCrn.update(targets);
				
				}
			}//if (templatePortalPage.getValue().lastIndexOf("</mypage>")!=-1)
			
			System.out.println("update complete");
		}
		catch (Exception e)
		{
			System.out.println(e);
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Logon to ReportNet.
	 *
	 *@param	namespace - Namespace id
	 *@param	uid - User id
	 *@param    pwd - password
	 */
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
	
	
	/**
	 * @return Returns an array of searchPaths of all the roles in nameSpaceID
	 */
	public String[] getRoles(String nameSpaceID)
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
		if (roleObjects.length != 0)
		{
		 	System.out.println("Found the following Roles belonging to "+ nameSpaceID);
		
			for (int x=0;x<roleObjects.length;x++)
			{
				roleSearchPaths[x] = roleObjects[x].getSearchPath().getValue();
				System.out.println(roleObjects[x].getDefaultName().getValue()); 	
			}
		 	System.out.println("Done getting Role CAMIDs\n\n"); 	
		}
		else
		{
			System.out.println("There were no Roles Found"); 
		}
			
	 	return roleSearchPaths;
	 			
	}
	
	/**
	 * @return Returns an array of searchPaths of all the groups in nameSpaceID
	 */
	public String[] getGroups(String nameSpaceID)
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
		if (groupObjects.length != 0)
		{
		 	System.out.println("Found the following groups belonging to "+ nameSpaceID);
		
			for (int x=0;x<groupObjects.length;x++)
			{
				groupSearchPaths[x] = groupObjects[x].getSearchPath().getValue();
				System.out.println(groupObjects[x].getDefaultName().getValue()); 	
			}
		 	System.out.println("Done getting Group CAMIDs\n\n"); 	
		}
		else
		{
			System.out.println("There were no Groups Found\n"); 
		}
			
	 	return groupSearchPaths;

	}
	
	
	public static void main(String[] args) 
	{
		// Variable that contains the default URL for CRN Content Manager.			
		String reportNetEndPoint = "http://localhost/crn/cgi-bin/cognos.cgi";

		// must be a user with System administrator privledges.
		String userName = "admin"; 
		String password = "password";
		String nameSpaceID = "DLDAP";
			
		// search path to the user whose pages you want to copy 
		String templateSearchPath = "CAMID(\"DLDAP:u:uid=vv,cn=group1,ou=people\")";
		
		CopyPersonalPages up = new CopyPersonalPages(reportNetEndPoint);

		// use this logon code to logon as a system administrator
		up.quickLogon(nameSpaceID,userName,password);

		//set the template account searchPath
		up.setTemplateSearchPath(templateSearchPath);
		// we want to update pages by role and/or group at a time
		// we therefore need to get the roles/groups and build the 
		// appropriate searchpath.
		String[] roleIDs = up.getRoles(nameSpaceID);
		String[] groupIDs = up.getGroups(nameSpaceID);
		for (int x=0;x<roleIDs.length;x++)
		{
			up.updatePages(templateSearchPath,"expandMembers("+roleIDs[x]+")");
		}
		for (int x=0;x<groupIDs.length;x++)
		{
			up.updatePages(templateSearchPath,"expandMembers("+groupIDs[x]+")");
		}

	}
	/**
	 * @return Returns the templateSearchPath.
	 */
	public String getTemplateSearchPath() {
		return templateSearchPath;
	}
	/**
	 * @param templateSearchPath The templateSearchPath to set.
	 */
	public void setTemplateSearchPath(String templateSearchPath) {
		this.templateSearchPath = templateSearchPath;
	}
}
