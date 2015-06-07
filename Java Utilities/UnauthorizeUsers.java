import com.cognos.developer.schemas.bibus._2.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.io.*;

public class UnauthorizeUsers
{
	private CognosReportNetServiceLocator service = null;
	private CognosReportNetBindingStub oCrn = null;
	private CognosReportNetService crn  = null;
	
	private ArrayList userList;
	private ArrayList objectsToUpdate;
	private BufferedWriter stateWriter;
	
	public UnauthorizeUsers(String endPoint, String userFileName)
	{
		try
		{
			stateWriter = new BufferedWriter(new FileWriter(new File("previousState.txt")));
			// Attempt to create ReportNet objects and logon
			service = new CognosReportNetServiceLocator();
			oCrn = new CognosReportNetBindingStub(new java.net.URL(endPoint), service);
			
			// Initialize the list of users from our file
			userList = new ArrayList();
			
			// initialize the Arraylist of objects to update
			objectsToUpdate = new ArrayList();

			// Set the Axis request timeout
			oCrn.setTimeout(0);  // in milliseconds, 0 turns the timeout off
			
			//initialize the ArrayList of users
			initUserList(userFileName);
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
	
	public static void main(String[] args)
	{
		String url = "http://reprd2:9300/p2pd/servlet/dispatch";
		//String url = "http://cogdbi1:9300/p2pd/servlet/dispatch";
		String userFileName = "usersToUnauthorize.txt";
		String notFoundFileName = "namesNotFound.txt";
		String foundFileName = "namesFound.txt";
		// Enter username and password of an admin here
		String un = args[0];
		String pw = args[1];
		String ns = "ADS";
		UnauthorizeUsers ua = new UnauthorizeUsers(url, userFileName);
		ua.quickLogon(ns, un, pw);
		ua.updateSecurityObjects();
		ua.writeFoundAndNotFound(notFoundFileName, foundFileName);
	}
	
	/**
	 * Method to run through all of the group's 
	 * users and updating the groups accordingly
	 */
	public boolean updateSecurityObjects()
	{
			BaseClass securityObjects[] = new BaseClass[]{};
			securityObjects = getGroups();
			
			// Loop through all of the groups 
			for(int i = 0; i < securityObjects.length; i++)
			{
				// Call method that will compare all the members of a group
				// to our list; works on single group; removes the offending member 
				// from the group array of members, and adds that group
				// to our list of objects to update, if the membership changes at all
				compareUsers(securityObjects[i]);
			}
				
			try
			{
				// Close the stateWriter that was printing the previous state of groups
				stateWriter.close();
				printObjectsToUpdate(objectsToUpdate);
				// Uncomment the following line to actually perform the update. 
				//oCrn.update((BaseClass[])objectsToUpdate.toArray(new BaseClass[objectsToUpdate.size()]), new UpdateOptions());
			}
			catch(Exception e)
			{
				System.out.println(e);
				e.printStackTrace(System.out);
			}
			
			return true;
	}
	
	/**
	 * Method to compare the members of a single group
	 * with the userIDs in our list and remove the ones that match
	 * from the group.
	 */
	 public void compareUsers(BaseClass so)
	 {
			ArrayList members = new ArrayList();
			boolean membershipChanged = false;
			
			try
			{
				stateWriter.write(so.getDefaultName().getValue() + "\n");
			}
			catch(Exception e)
			{
				System.out.println(e);
				e.printStackTrace(System.out);
			}
			
			// Must cast to a Group object before we can access the getMembers() function
			Group temp = (Group)so;
			if(temp.getMembers().getValue() != null)
			{
				members = new ArrayList(Arrays.asList(temp.getMembers().getValue()));
			
				// Loop through all the members of the group
				for(int i = 0; i < members.size(); i++)
				{
					BaseClass member = (BaseClass)members.get(i);
					
					// Only deal with account objects. Leave the groups alone
					if(member instanceof Account)
					{
						Account acct = (Account)member;
						String userID = acct.getUserName().getValue();
						
						try
						{
							stateWriter.write("		" + userID + "\n");
						}
						catch(Exception e)
						{
							System.out.println(e);
							e.printStackTrace(System.out);
						}
						
						// For this account, check it against our list
						for(int j = 0; j < userList.size(); j++)
						{
							HashMap user = (HashMap)userList.get(j);
							
							// If the userID is found in our list, remove that user
							// from the group, and break out of the inner loop.
							// Decrement the outer loop index.
							// Also, denote that we found the user in our list
							if(userID.equalsIgnoreCase((String)user.get("userID")))
							{
								user.put("found", "true");
								membershipChanged = true;
								members.remove(i);
								i--;
								break;
							}
						}
					}
				}
			}
			
			try
			{
				stateWriter.write("\n");
			}
			catch(Exception e)
			{
				System.out.println(e);
				e.printStackTrace(System.out);
			}
			
			// If members where removed, add the group object
			// to the list of objects to update
			// with the new member list
			if(membershipChanged)
			{
				temp.setMembers(new BaseClassArrayProp());
				
				// Must cast the result of toArray() after passing it the array type
				// This is a shortcoming of the way the collection class works.'
				// 
				temp.getMembers().setValue((BaseClass[])members.toArray(new BaseClass[members.size()]));
				objectsToUpdate.add(temp);
			}
	 }
	 
	/**
	 * Method to return an array of groups with members attached
	 */
	public BaseClass[] getGroups()
	{
		    PropEnum props[] = new PropEnum[] { PropEnum.defaultName, PropEnum.searchPath, PropEnum.type, PropEnum.members };

            // Properties used to get account class information for the members
			// including the userIDs
            RefProp memberProps = new RefProp();
            memberProps.setRefPropName(PropEnum.members);
            memberProps.setProperties(new PropEnum[] { PropEnum.searchPath, PropEnum.defaultName, PropEnum.userName, 
													  PropEnum.objectClass});

            BaseClass[] securityObjects = new BaseClass[]{};
            Account[] targetAccount = null;

            QueryOptions qo = new QueryOptions();

            qo.setRefProps(new RefProp[] { memberProps });

            // Set search path to pull back all groups  in Cognos
            String searchPath = "//group";

            // Query for all groups 
            try
            {
                securityObjects = oCrn.query(searchPath, props, new Sort[] { }, qo);
            }
            catch (Exception e)
            {
                System.out.println(e);
				e.printStackTrace(System.out);
            }
			return securityObjects;
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
	 * Method to populate the list of userIDs from the file
	 */
	public void initUserList(String file)
	{
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader (new File(file)));
		
			String userID = reader.readLine();
			while(userID != null)
			{
				// Set up the userIDs as an arrya of hashmaps so we can associate
				// a 'found' property with each userID. This facilitates final 
				// separation of the found userIDs from those not found
				HashMap hm = new HashMap();
				hm.put("userID", userID);
				hm.put("found", "false");
				userList.add(hm);
				userID = reader.readLine();
			}
			reader.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	
	/**
	 *  Function to print out the list of all objects that will be updated
	 */
	public void printObjectsToUpdate(ArrayList obs)
	{
	
		try
		{
			// Set up object and member-change writer
			BufferedWriter toUpdateWriter = new BufferedWriter(new FileWriter(new File("updatedObjects.txt")));
			System.out.println(obs.size());
			// Loop through the groups and print them
			for(int i = 0; i < obs.size(); i++)
			{
				// Must cast to a Group object before we can access the getMembers() function
				Group temp = (Group)obs.get(i);
				
				// Make sure that the default name for the group is not null
				if(((BaseClass)obs.get(i)).getDefaultName() != null)
				{
					toUpdateWriter.write(((BaseClass)obs.get(i)).getDefaultName().getValue() + "\n");
				}
				
				// Make sure the array of members is not null. It should never be,
				// because it should always be set in compareUsers() for all objects
				// in the ArrayList. 
				if(temp.getMembers().getValue() != null)
				{
					// Loop through the members of the group and print them
					for(int j = 0; j < temp.getMembers().getValue().length; j++)
					{
						// Make sure that the defaultName property is not null
						if(temp.getMembers().getValue()[j].getDefaultName() != null)
						{
							toUpdateWriter.write("	" + temp.getMembers().getValue()[j].getDefaultName().getValue() + "\n");
						}
					}
					toUpdateWriter.write("\n");
				}
			}
			
			toUpdateWriter.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
			e.printStackTrace(System.out);
		}
	}
	
	/**
	 * Method to print out the usernames that were found,
	 * and those that were not found to their respective files
	 */
	public void writeFoundAndNotFound(String notFoundFileName, String foundFileName)
	{
		try
		{
			// Set up status writers
			BufferedWriter notFoundWriter = new BufferedWriter(new FileWriter(new File(notFoundFileName)));
			BufferedWriter foundWriter = new BufferedWriter(new FileWriter(new File(foundFileName)));
			
			for(int i = 0; i < userList.size(); i++)
			{
				HashMap user = (HashMap)userList.get(i);
				if(((String)user.get("found")).equalsIgnoreCase("true"))
				{
					foundWriter.write(user.get("userID") + "\n");
				}
				else
				{
					notFoundWriter.write(user.get("userID") + "\n");
				}
			}
			
			foundWriter.close();
			notFoundWriter.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
}