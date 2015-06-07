package cognos8;
/**
 * GetModel.java
 *
 * Copyright © 2004 Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1008101) - Cognos SDK Java Sample to Extract the Model of a Published Package
 *
 */
import com.cognos.developer.schemas.bibus._3.*;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;

public class GetModel
{
	public ContentManagerService_Port cmService = null;

	public void connectToReportServer (String endPoint) throws Exception
	{
		ContentManagerService_ServiceLocator cmServiceLocator = new ContentManagerService_ServiceLocator();
		try
		{
			cmService = cmServiceLocator.getcontentManagerService(new java.net.URL(endPoint));
			
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
		
	}
	
	public void logon(String namespace, String uid, String pwd) throws Exception
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
			cmService.logon(new XmlEncodedXML(encodedCredentials), null);
		}
		catch (Exception ex)
		{
			System.out.println("exception thrown " + ex);
			return;
		}
		System.out.println("Logon successful as " + uid);
	}

	public void getModel(String packageSearchPath) throws Exception
	{
		PropEnum props[] = new PropEnum[]{PropEnum.searchPath, PropEnum.model};
										  
		BaseClass[] model = cmService.query(new SearchPathMultipleObject(packageSearchPath+"/model"), props, new Sort[]{}, new QueryOptions());

		for(int i = 0; i < model.length; i++)
		{
			System.out.println("\n**********************************************************\n");
			System.out.println("Model: " + model[i].getSearchPath().getValue() + "\n");
			System.out.println(((Model)model[i]).getModel().getValue());
			System.out.println("\n**********************************************************\n");
			
			//If you want to write the model data to a file that can be opened from Framework Manager,
			//Uncomment the following lines
			/*
			String modelFile="d:\\temp\\model"+i+".xml";
			File oFile = new File(modelFile);
			FileOutputStream fos = new FileOutputStream(oFile);
			String temp=((Model)model[i]).getModel().getValue();
			ByteArrayInputStream bais = new ByteArrayInputStream(temp.getBytes("UTF-8"));
			System.out.println("Writing model data to file"+modelFile);
			while (bais.available() > 0) {
					fos.write(bais.read());
				};
			fos.flush();
			fos.close();
			*/
		}
	}
	
	public static void main(String args[])
	{
		// connection to the Cognos 8 service
		String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";
		// log in as a System Administrator to ensure you have the necessary permissions to access the model
		String namespaceID = "namespaceID";
		String userID = "userID";
		String password = "password";
		// search path of the package from which the model will be extracted 
		String packageSearchPath = "/content/package[@name='GO Sales and Retailers']";
		GetModel test = new GetModel();

		try
		{
			test.connectToReportServer(endPoint);
			//test.logon(namespaceID, userID, password);
			test.getModel(packageSearchPath);
			System.out.println("\nDone.");
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
}
