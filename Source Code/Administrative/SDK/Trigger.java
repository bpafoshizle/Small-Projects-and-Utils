/**
 *
 * Copyright 2006 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 */

import java.rmi.RemoteException;

import org.apache.axis.client.Stub;

import com.cognos.developer.schemas.bibus._3.BiBusHeader;
import com.cognos.developer.schemas.bibus._3.CAM;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_ServiceLocator;
import com.cognos.developer.schemas.bibus._3.EventManagementService_Port;
import com.cognos.developer.schemas.bibus._3.EventManagementService_ServiceLocator;
import com.cognos.developer.schemas.bibus._3.HdrSession;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.cognos.developer.schemas.bibus._3.XmlEncodedXML;

/**
 *
 */
public class Trigger
{
	private String endpoint;

	private ContentManagerService_Port cmService;
	private ContentManagerService_ServiceLocator cmServiceLocator;
	private EventManagementService_Port eventService;
	private EventManagementService_ServiceLocator eventServiceLocator;
	private XmlEncodedXML credentialXEX;

	public Trigger(String serverURLString)
	{
		this.endpoint = serverURLString;
		if (endpoint == null || endpoint.equals(""))
			return;
		try
		{
			//initialize the service locators
			eventServiceLocator = new EventManagementService_ServiceLocator();
			cmServiceLocator = new ContentManagerService_ServiceLocator();

			//get the service objects from the locators
			eventService = eventServiceLocator.geteventManagementService(new java.net.URL(endpoint));

			cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(endpoint));
			//Set the axis timeout to 0 (infinite)
			//There may be many, many actions due to this trigger
			((Stub)eventService).setTimeout(0);

			credentialXEX = new XmlEncodedXML();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public boolean loginAnonymous()
	{
		if (! loginAnonymousEnabled() )
		{
			return false;
		}

		CAM cam = new CAM();
		cam.setAction("logon");

		HdrSession header = new HdrSession();

		BiBusHeader bibus = new BiBusHeader();
		bibus.setCAM(cam);
		bibus.setHdrSession(header);

		((Stub)cmService).setHeader("", "biBusHeader", bibus);
		return true;
	}

	public boolean login(String namespace, String uid, String passwd)
	  {
		try
		{
			StringBuffer credentialXML = new StringBuffer();
			credentialXML.append("<credential>");
			credentialXML.append("<namespace>" + namespace + "</namespace>");
			credentialXML.append("<password>" + passwd + "</password>");
			credentialXML.append("<username>" + uid + "</username>");
			credentialXML.append("</credential>");

			credentialXEX.setValue(credentialXML.toString());
			cmService.logon(credentialXEX, null);
		}
		catch (RemoteException e)
		{
			e.printStackTrace();
			return false;
		}
		BiBusHeader bibus =
			(BiBusHeader) ((Stub)cmService).getHeaderObject("", "biBusHeader");

		if (bibus != null)
		{
			((Stub)eventService).setHeader("","biBusHeader", bibus);
			return true;
		}

		return false;
	}

	public void logoff()
	{
	  	try
		{
			cmService.logoff();
		}
	  	catch (RemoteException e)
		{
			e.printStackTrace();
		}

	}

	public boolean loginAnonymousEnabled()
	{
		SearchPathMultipleObject cmSearch = new SearchPathMultipleObject("~");
		try
		{
			cmService.query(
				cmSearch,
				new PropEnum[] {},
				new Sort[] {},
				new QueryOptions());
		}
		catch (java.rmi.RemoteException remoteEx)
		{
			return false;
		}
		BiBusHeader bibus =
			(BiBusHeader) ((Stub)cmService).getHeaderObject("", "biBusHeader");

		if (bibus != null)
		{
			((Stub)eventService).setHeader("","biBusHeader", bibus);
			return true;
		}

		return false;
	}

	public int fireTrigger(String triggerName)
	{
		try
		{
			// sn_dg_sdk_method_eventManagementService_trigger_start_1
			return eventService.trigger(triggerName);
			// sn_dg_sdk_method_eventManagementService_trigger_end_1
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return 0;
		}
	}

	public static void usage()
	{
		System.out.println("");
		System.out.println("");
		System.out.println("Command Line Parameters:");
		System.out.println("");
		System.out.println("<URL> [ <userName> <password> <nameSpace> ] triggerList");
		System.out.println("");
		System.out.println("  Required arguments:");
		System.out.println("");
		System.out.println("          URL - Cognos 8 Server URL");
		System.out.println("			eg. \"http://localhost:9300/p2pd/servlet/dispatch\"");
		System.out.println("  triggerList - comma separated list of trigger names");
		System.out.println("			eg. \"triggerName1,triggerName2,triggerName3\"");
		System.out.println("");
		System.out.println("  Optional arguments: for use with secured namespace (Anonymous disabled)");
		System.out.println("");
		System.out.println("     userName - username, valid within the namespace, to run the utility");
		System.out.println("     password - password for the given user");
		System.out.println("    nameSpace - namespace for the desired user");
	}

	public static void main(String args[])
	{
		if ((args.length != 5) && (args.length != 2) )
		{
			usage();
			System.exit(-1);
		}
		String nameSpace = "";
		String passwd = "";
		String userName = "";
		String triggers = "";
		String url;
		url = args[0];

		boolean cmIsReady = false;
		Trigger myTrigger = new Trigger(url);

                if (args.length == 5) {
			userName = args[1];
			passwd = args[2];
			nameSpace = args[3];
			triggers = args[4];
			cmIsReady = myTrigger.login(nameSpace,userName,passwd);
		}
		else
		{
			triggers = args[1];
			cmIsReady = myTrigger.loginAnonymous();
		}

		java.util.StringTokenizer triggerTokens = new java.util.StringTokenizer(triggers,",");

		if (cmIsReady)
		{
			int totalTriggersFired = 0;
			int triggerFired = 0;

			while(triggerTokens.hasMoreTokens())
			{
				String triggerName = triggerTokens.nextToken();
				triggerFired = myTrigger.fireTrigger(triggerName);
				if ( triggerFired > 0 )
				{
					System.out.println("Trigger: " + triggerName + " fired successfully");
					totalTriggersFired += 1;
				}
				else
				{
					System.out.println("Failed to fire trigger " + triggerName);
				}
			}
		       	System.exit(totalTriggersFired);
		}
		else
		{
			System.out.println("Error: Login Failure - please try again.");
			usage();
			System.exit(-2);
		}
	}

}
