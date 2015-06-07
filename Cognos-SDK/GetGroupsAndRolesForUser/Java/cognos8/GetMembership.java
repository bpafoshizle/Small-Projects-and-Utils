import java.rmi.RemoteException;

import com.cognos.developer.schemas.bibus._3.*;

/*
 * GetMembership.java
 * 
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 * 
 * Description: KB 1020914 - SDK Sample on how to use Membership function
 */
public class GetMembership {
	private   ContentManagerService_Port cmService = null;
	
	public static void main(String[] args) {
		
		String C8_URL = "http://localhost:9300/p2pd/servlet/dispatch"; 
		//account to query for membership
		String sPath = "CAMID(\"DLDAP:u:uid=admin,ou=people\")";
		GetMembership membership = new GetMembership(C8_URL);
		//logon info. Comment next line if Anonymous is enabled
		membership.quickLogon("DLDAP", "admin", "password");
		membership.getMembers(sPath);
	}
	
	public void getMembers(String sPath)
	{
		PropEnum properties[] = new PropEnum[]{PropEnum.defaultName, PropEnum.searchPath};
		//set the searchPath to use the membership function
		SearchPathMultipleObject spMulti = 
			new SearchPathMultipleObject("membership(" + sPath +")");
		
		//set the order for the results to ascending
		Sort sort[] = new Sort[1];
		sort[0] = new Sort();
		sort[0].setOrder(OrderEnum.ascending);
		sort[0].setPropName(PropEnum.defaultName);
		
		try {
			BaseClass results[] = cmService.query(spMulti, properties, sort, new QueryOptions());
			if (results != null && results.length > 0)
				for (int i=0; i<results.length ; i++)
				{
					//display the defaultName and the searchPath for results
					System.out.println(results[i].getDefaultName().getValue());
					System.out.println("\t" + results[i].getSearchPath().getValue());
				}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	GetMembership(String endPoint)
	{
		ContentManagerService_ServiceLocator cmServiceLocator = new ContentManagerService_ServiceLocator();
		  		 
	    try {
	      	cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(endPoint));
						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
//	This method loggs the user to Cognos 8
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
			cmService.logon(xmlCredentials,null );
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return ("Logon successful as " + uid);
	}
}
