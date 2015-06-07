package cognos8Compatible;
/**
 * CopyPersonalPages.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1011181) - How to define all exisitng Groups and Roles and then perform an action on the associated members
 */

import java.rmi.RemoteException;

import com.cognos.developer.schemas.bibus._3.*;

public class CopyPersonalPages 
{
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetPortType oCrn = null;
	private String templateSearchPath = null;
		
	/**
	 * Connect to Cognos 8
	 */
	public CopyPersonalPages(String endPoint)
	{
		try
		{
			service = new CognosReportNetServiceLocator();
			oCrn = service.getCognosReportNetPort(new java.net.URL(endPoint));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * This method will add/update the user's My Pages
	 */
	public void updatePages( String targetSearchPath, Pagelet templatePortalPage) 
	{
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath, 
				PropEnum.portalPages};
		Sort sOpt[] = new Sort[]{}; 
		QueryOptions qOpt = new QueryOptions();
		BaseClass targets[] = null;
				
		try {
			// get the target user's account object. 
			 targets = oCrn.query(targetSearchPath, props, sOpt, qOpt);
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		if (targets != null && targets.length > 0)
		{
		
		// update the target accounts' portal page with the portal page from the template user. 
		for(int i=0;i<targets.length;i++)
		{
			// initialize targetAccount
			Account targetAccount = (Account)targets[i];
			
			if (targetAccount.getSearchPath().getValue().equals(templateSearchPath))
				continue;
			// get the target users portal page
			BaseClass targetAccountPortalPages[] = targetAccount.getPortalPages().getValue(); 
			//Since we do not want to overwrite any existing portal pages, we need to merge the two
			if (targetAccountPortalPages == null)
			{
				//this user has not logged in yet and does not have Content or My Folders
			
				//copy the template page into the target user's My folders
				BaseClass page[] = copyPortalPage(
						targetAccount.getSearchPath().getValue() + "/folder[@name='My Folders']", 
						templatePortalPage);
				
				setPortalPages(new BaseClass[]{page[0]}, targetAccount);		
			}
			else
			// check to see if user has their own pages and if one of them
			// has the title identifying the template. If so, it will be overwriten.
			{
				boolean foundPage = false;
				for (int j=0; j<targetAccountPortalPages.length; j++)
					if ( targetAccountPortalPages[j] instanceof Pagelet &&
							targetAccountPortalPages[j].getSearchPath().getValue().indexOf(
									templatePortalPage.getDefaultName().getValue())>0)					
					{
						foundPage = true;
						// replace the target page with the template
						targetAccountPortalPages[j] = copyPortalPage(
								targetAccount.getSearchPath().getValue() + "/folder[@name='My Folders']", 
								templatePortalPage)[0];
						
						setPortalPages(targetAccountPortalPages, targetAccount);
						break;
					}
					
				if (!foundPage) 						
				{					
					//copy the template page into the target user's My folders
					BaseClass copies[] = copyPortalPage(
							targetAccount.getSearchPath().getValue() + "/folder[@name='My Folders']", 
							templatePortalPage);
					
					 //ie. target user has existing pages but none are the template
					// we will therefore merge them
					BaseClass newtargetAccountPortalPages[] = new BaseClass[targetAccountPortalPages.length + 1];
					System.arraycopy(targetAccountPortalPages, 0, newtargetAccountPortalPages, 0, targetAccountPortalPages.length);
					newtargetAccountPortalPages[newtargetAccountPortalPages.length -1] = (Pagelet)copies[0];
					
					setPortalPages(newtargetAccountPortalPages, targetAccount);
				}
			}
		}
		}
	}
	
	/**
	 * This method will add the template page to the portal pages of the user 
	 * and update the account
	 */
	public void setPortalPages(BaseClass[] portalPages, Account targetAccount)
	{
		BaseClassArrayProp bcap = new BaseClassArrayProp();
		bcap.setValue(portalPages);
		targetAccount.setPortalPages(bcap);
		
		//update the target account
		try {
			UpdateOptions uo = new UpdateOptions();
			uo.setReturnProperties(new PropEnum []{PropEnum.portalPages, PropEnum.portalPage});
			BaseClass temp[] = oCrn.update(new BaseClass[]{targetAccount},uo);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("updated my pages for user: " 
				+ targetAccount.getSearchPath().getValue());
		
	}
	
	/**
	 * This method will copy the template page to the user's My Folders
	 */
	public BaseClass[] copyPortalPage(String targetPath, Pagelet templatePortalPage)
	{
		//the pagelet to be added
		BaseClass copies[] = null;
		//copy the template page into the target user's My folders
		CopyOptions co= new CopyOptions();
		co.setRecursive(true);
		co.setUpdateAction(UpdateActionEnum.replace);
		try {
			copies = oCrn.copy(new BaseClass [] {templatePortalPage}, 
					targetPath, co);
		} catch (RemoteException e2) {
			e2.printStackTrace();
		}
		return copies;
	}
		
		
	/**
	 * Logon to Cognos 8.
	 *
	 *@param	namespace - Namespace id
	 *@param	uid - User id
	 *@param    pwd - password
	 */
	public String quickLogon(String namespace, String uid, String pwd)
	{
		try
		{
			StringBuffer credentialXML = new StringBuffer();

			credentialXML.append("<credential>");
			credentialXML.append("<namespace>").append(namespace).append("</namespace>");
			credentialXML.append("<username>").append(uid).append("</username>");
			credentialXML.append("<password>").append(pwd).append("</password>");
			credentialXML.append("</credential>");

			String encodedCredentials = credentialXML.toString();

			oCrn.logon(encodedCredentials, new String[]{}/* this parameter does nothing, but is required */);
		}
		catch(Exception e)
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
	
	//get the page that will be used as a template
	public Pagelet getTemplatePage(String templateSearchPath, String pageName)
	{
		PropEnum props[] = new PropEnum[] {PropEnum.searchPath,
					PropEnum.defaultName,PropEnum.portalPages, PropEnum.layout};
		Sort sOpt[] = new Sort[]{}; 
		QueryOptions qOpt = new QueryOptions();
		
		try
		{
			// get the template user's Account object. 
			BaseClass template[] = oCrn.query(templateSearchPath, props, sOpt, qOpt);
			if (template != null && template.length >0)
			{
				BaseClass pages[] = ((Account)template[0]).getPortalPages().getValue();
				for (int i=0; i<pages.length ; i++)
				{
					if (pages[i] instanceof Pagelet && 
							pages[i].getSearchPath().getValue().indexOf(pageName) > 0)
					{
						return (Pagelet)oCrn.query(pages[i].getSearchPath().getValue(), props, sOpt, qOpt)[0]; 
					}
				}
			}else
				System.out.println("No pagelets found for the template account");
			
		}catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static void main(String[] args) 
	{
		// Variable that contains the default URL for CRN Content Manager.			
		String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";

		// must be a user with System administrator privledges.
		String userName = "admin"; 
		String password = "password";
		String nameSpaceID = "DLDAP";
			
		// search path to the user whose pages you want to copy 
		String templateSearchPath = "CAMID(\"DLDAP:u:uid=vv,cn=group1,ou=people\")";
		String pageName = "New Page";
		CopyPersonalPages up = new CopyPersonalPages(endPoint);

		// use this logon code to logon as a system administrator
		up.quickLogon(nameSpaceID,userName,password);
		
		//set the template searchPath of the class to the one set in main
		up.setTemplateSearchPath(templateSearchPath);
		
		Pagelet templatePage = up.getTemplatePage(templateSearchPath, pageName);
		
		if (templatePage != null)
		{
			// we want to update pages by role and/or group at a time
			// we therefore need to get the roles/groups and build the 
			// appropriate searchpath.
			String[] roleIDs = up.getRoles(nameSpaceID);
			String[] groupIDs = up.getGroups(nameSpaceID);
			for (int x=0;x<roleIDs.length;x++)
			{
				up.updatePages("expandMembers("+roleIDs[x]+")", templatePage);
			}
			for (int x=0;x<groupIDs.length;x++)
			{
				up.updatePages("expandMembers("+groupIDs[x]+")", templatePage);
			}
		}
		else
			System.out.println("No pages found");
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
