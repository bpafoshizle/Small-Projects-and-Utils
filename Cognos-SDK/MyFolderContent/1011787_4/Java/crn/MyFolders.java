package crn;
/**
 * MyFolders.java
 *
 * Copyright © 2008 Cognos, an IBM Company. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos, an IBM Company
 *  
 * Description: (KB #1011787) - SDK Sample to display contents from all the existing My Folders from Cognos Connection.
 * 
 */
import java.io.FileWriter;
import java.io.Writer;

import com.cognos.developer.schemas.bibus._2.*;

public class MyFolders 
{
	
	public CognosReportNetServiceLocator service = null;
	public CognosReportNetPortType 		 oCrn    = null;
	// Default URL for CRN Content Manager. Change if not using default gateway
	public final String CM_URL = "http://localhost/crn/cgi-bin/cognos.cgi";   
	public CognosReportNetPortType getCrn() {return oCrn;}
	
	public static void main(String args[])
	{
		//initialize a connection to CRN
		MyFolders connect = new MyFolders();
		String namespaceID  = "DLDAP";
		String adminID = "admin";
		String adminPassword = "password";
		String namespaceCAMID = "CAMID(\"DLDAP\")";
		String camid = namespaceCAMID + "//account"; //this is the search path for all user accounts
		String userSearchPaths = camid + "/folder[@name='My Folders']//*";
		String fileName = "D:\\Content.csv";
		
		String toWrite ="User,Folder,Type,Name,SearchPath" +  System.getProperty("line.separator");
		try
		{
			//login as jsmith who is a member of System Administrators
			connect.quickLogon(namespaceID,adminID, adminPassword);
			//We will display My Folders and My Pages for all users in namespace
			PropEnum props[] = new PropEnum[]{PropEnum.searchPath, 
					PropEnum.objectClass,PropEnum.defaultName, PropEnum.portalPage};
			
			BaseClass bc[] = connect.getCrn().query(camid, props, new Sort[]{}, new QueryOptions());
 	 			
 			if (bc != null && bc.length > 0)
 			{
 				for (int i=0; i<bc.length; i++)
 				{
 					//Query the Content Store for all objects in My Folders for user bc[i]
					userSearchPaths = bc[i].getSearchPath().getValue() + "/folder[@name='My Folders']/*";
					BaseClass contents[] = connect.getCrn().query(userSearchPaths, props, new Sort[]{}, new QueryOptions());
					
					if (contents != null && contents.length > 0)
					{
						//Display all objects in My Folders for user bc[i]
						for (int j=0; j<contents.length; j++)
						{
							toWrite += bc[i].getDefaultName().getValue() + ",MyFolders,";
							toWrite += contents[j].getObjectClass().getValue().getValue() + ",";
							toWrite += contents[j].getDefaultName().getValue() + ",";
							toWrite += contents[j].getSearchPath().getValue() + ",";
							toWrite += System.getProperty("line.separator");
						}
					}
					else
						toWrite +="NoObject" + System.getProperty("line.separator");;
 				}
 			}
 			connect.writeDoc(toWrite, fileName);
 			System.out.println("Done");
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}

	private void writeDoc(String doc, String fileName)
	{		
		//file name and path to write the information to
		Writer fw = null;
		try
		{
			fw = new FileWriter(fileName);
			fw.write(doc);
			fw.flush() ;
			fw.close();
			System.out.println("The contents were written to " + fileName);
			
		}catch (Exception ioe)
		{
			ioe.printStackTrace() ;
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
		{		oCrn.logon(encodedCredentials, new String[]{});
		}catch (Exception e)
		{System.out.println(e);}
		return ("Logon successful as " + uid);
	}
	
	public MyFolders ()
	{		
		// Retrieve the service           
		service = new CognosReportNetServiceLocator();

		try
		{
			oCrn = service.getCognosReportNetPort(new java.net.URL(CM_URL));
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
	
}
