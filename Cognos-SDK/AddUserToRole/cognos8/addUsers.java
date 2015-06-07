package cognos8;
/**
 * addUsers.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1020975) - SDK Sample to add users to roles, reading information from text file.
 * 
 */

import java.io.*;
import org.apache.axis.AxisFault;
import com.cognos.developer.schemas.bibus._3.*;

public class addUsers
{
	public ContentManagerService_ServiceLocator cmServiceLocator = null;
	public ContentManagerService_Port cmService = null;
	
	public addUsers(String endPoint)
	{
		// The user must login to Cognos8
		// before accessing any functionality.
		try 
		{
			cmServiceLocator = new ContentManagerService_ServiceLocator();   
	   		// Connect to Cognos8
	    	cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(endPoint));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public void addToRoll(String authNameSpace, String uid, String pwd, String fileName)
	{
		final String userRoleSeparator = "~";
		try 
		{
	   		
			StringBuffer credentialXML = new StringBuffer();

			credentialXML.append("<credential>");
			credentialXML.append("<namespace>").append(authNameSpace).append("</namespace>");
			credentialXML.append("<username>").append(uid).append("</username>");
			credentialXML.append("<password>").append(pwd).append("</password>");
			credentialXML.append("</credential>");

			String encodedCredentials = credentialXML.toString();
		    XmlEncodedXML xmlCredentials = new XmlEncodedXML();
		    xmlCredentials.setValue(encodedCredentials);
		        
		   // Invoke the ContentManager service logon() method passing the credential string
		   // You will pass an empty string in the second argument.  

		    try
			{		
				cmService.logon(xmlCredentials,null );
				System.out.println("Logon successful as " + uid);
			//	return;
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
	   		
	   		//
	   		//	Reading from Text file
	   		//
	   		//	RoleName~UserName
	   		//      For Example: CAMID(":Authors")~John Smith
			//
			
	   		String userName = null;
	   		String roleName = null;
			
			boolean exists = (new File(fileName)).exists();
			if (exists)
			{
				// File or directory exists
			    try
			    {
			    	BufferedReader in = new BufferedReader(new FileReader(fileName));
			    	String str;
			    	int count = 1;
			    	while ((str = in.readLine()) != null)
			    	{  			    		
			    		if (str.charAt(0) == '#') 
			    		{
			    			continue;
			    		}
			    		else
			    		{	
			    			System.out.println();
			    			System.out.println("Read record [" + count + "] data [" + str + "]");
				    		count++;
				    		roleName = str.substring(0, str.indexOf(userRoleSeparator));
				    		userName = str.substring(str.indexOf(userRoleSeparator)+ 1);
				    		System.out.println("Role [" + roleName + "] User [" + userName + "]");
			    		}
			    		
						// Query for the current user, and the defined role.
				    	PropEnum[] props = { PropEnum.defaultName, PropEnum.searchPath, PropEnum.members };
				        Sort sortOptions[] = {new Sort()};
				        QueryOptions queryOptions = new QueryOptions();

				        // Search for the User in the Content Store
				        String sSearchPath = "CAMID(\""+ authNameSpace + "\")//account[@defaultName='" + userName + "']";
				        System.out.println("Searching for user with searchPath [" + sSearchPath + "]");				                             
				        
						BaseClass member[] = cmService.query(new SearchPathMultipleObject(sSearchPath), props, sortOptions, queryOptions );
						String currentUserPath = "";
						if (member.length > 0)
						{
							currentUserPath = member[0].getSearchPath().getValue();
							System.out.println("currentUserPath = " + currentUserPath);
						}
						else
						{
							System.out.println("User - " + sSearchPath + " not found.");
							return;
						}
						
						// get the role
						BaseClass roles[] = cmService.query(new SearchPathMultipleObject(roleName), props, sortOptions,queryOptions );
						Role role = (Role)roles[0];
						
						// if the role doesn't have any members just add the user.
						if (role.getMembers().getValue() == null)
						{
							role.setMembers(new BaseClassArrayProp());
							role.getMembers().setValue(member);
							System.out.println(role.getDefaultName().getValue());
							cmService.update(new BaseClass[] { role }, new UpdateOptions());
						}
						else
						{
							// Preserve all the existing members.
							BaseClass[] newMembers = new BaseClass[role.getMembers().getValue().length + 1];
							boolean fDup = false;
							int index = 0;
							for (int i = 0; i < role.getMembers().getValue().length; i++)
							{
								String objPath = role.getMembers().getValue()[i].getSearchPath().getValue();
								System.out.println("Processing member [" + objPath + "] currentUserPath [" + currentUserPath + "]");								
								if (currentUserPath.equals(objPath))
								{
									System.out.println("Ignoring user [" + currentUserPath + "]. Already a member.");									
									fDup = true;
									break;
								}
								
								cmService.query(new SearchPathMultipleObject(objPath),
										props,sortOptions,queryOptions);
								newMembers[index] = role.getMembers().getValue()[i];
								index++;
							}
							
							if (!fDup)
							{
								newMembers[index] = member[0];

								role.setMembers(new BaseClassArrayProp());
								role.getMembers().setValue(newMembers);

								// Update the membership.
								System.out.println("call update, to add new member");
								cmService.update(new BaseClass[] { role }, new UpdateOptions());
							}
						}
			    	}
			    	in.close();
			    	
			    	System.out.println();
					System.out.println(uid + " has logged off");			    	
					cmService.logoff();  // we need to call this in order to get a new cam_passport.			    	
				}
				catch (AxisFault e)
				{
					e.printStackTrace();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			else 
			{
				// File or directory does not exist
				System.out.println("File or directory does not exist");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) 
	{
	    
		String endPoint = "http://localhost:9300/p2pd/servlet/dispatch"; // name of the server with C8
		String authNameSpace = "SDK"; //namespace ID defined in Cognos Connection 
	    String uid = "admin";
	    String pwd = "password";
   		// defines the name of the file that contains the role and username information
		String fileName = "C:\\Perf\\support_dev\\CRN\\SDK\\SAMPLES\\KB_DOCS\\1020975\\Java\\cognos8\\users.txt";

		addUsers oAddUsers = new addUsers(endPoint);
		System.out.println("Log on and Search Content Store");
		System.out.println();
		oAddUsers.addToRoll(authNameSpace, uid, pwd, fileName);
	}
}

