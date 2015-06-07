/**
 * DBSignOn.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1006118) - How do you change the Data Source Connection based on an application variable using the SDK
 * 
 */
import com.cognos.developer.schemas.bibus._3.*;

public class DBSignOn {

	public CognosReportNetServiceLocator service = null;
	public CognosReportNetPortType  oCrn    = null;
	
	public static void main(String[] args) 
	{
		String param = null;
		final String dsConnection = "CAMID(\":\")/dataSource[@name='gosales']/dataSourceConnection[@name='GOSL']";
		String sDbConnection = null;
		final String sDbConnectionDev = "dev";
		String sDbConnectionProd = "prod";
		final String sDbCatalogDev = "^User ID:^?Password:;LOCAL;OL;DBInfo_Type=MS;Provider=SQLOLEDB;User ID=%s;Password=%s;Data Source=localhost;Provider_String=Initial Catalog=GOSL;@COLSEQ=";
		final String sDbCatalogProd = "^User ID:^?Password:;LOCAL;OL;DBInfo_Type=MS;Provider=SQLOLEDB;User ID=%s;Password=%s;Data Source=localhost;Provider_String=Initial Catalog=gosales;@COLSEQ=";
		String sDbCatalog = null;
		
		PropEnum props[] = {PropEnum.defaultName, PropEnum.user,
				PropEnum.credentials,PropEnum.searchPath, PropEnum.version,
				PropEnum.consumers, PropEnum.connectionString, PropEnum.credentialNamespaces ,
				PropEnum.permissions, PropEnum.policies };
		
		if (args.length > 0)
		{	
			param= args[0];
			if (param.compareToIgnoreCase(sDbConnectionDev) == 0)
				sDbCatalog = sDbCatalogDev;
			else 
				if (param.compareToIgnoreCase(sDbConnectionProd) == 0)
					sDbCatalog = sDbCatalogProd;
				else
				{
					System.out.println("Invalid parameter. Pick \"dev\" or \"prod\".");
					System.exit(1);
				}
		}
		else
		{
			System.out.println("Invalid parameter. Pick \"dev\" or \"prod\".");
			System.exit(1);
		}
		
		DBSignOn connect = new DBSignOn();
		connect.connectToReportServer();
		 
		try
		{
		  //Connect to ReportNet as System Administrator	
		  connect.quickLogon("DLDAP", "admin", "password");
		  //Query CM for the an existing signon
		  BaseClass bc[] = connect.oCrn.query(dsConnection,props, new Sort[]{}, new QueryOptions());
		  DataSourceConnection ds = (DataSourceConnection)(bc[0]);

		  StringProp sp = new StringProp();
		  sp.setValue(sDbCatalog);
		  ds.setConnectionString(sp);
		  
		  connect.oCrn.update(new BaseClass [] {ds});
		  System.out.println("Done");
		}
		catch (Exception e){
		System.out.println(e);
		}
	 }
	

	public String quickLogon(String namespace, String uid, String pwd) throws Exception
	{
		StringBuffer credentialXML = new StringBuffer();

		credentialXML.append("<credential>");
		credentialXML.append("<namespace>").append(namespace).append("</namespace>");
		credentialXML.append("<username>").append(uid).append("</username>");
		credentialXML.append("<password>").append(pwd).append("</password>");
		credentialXML.append("</credential>");

		String encodedCredentials = credentialXML.toString();

		oCrn.logon(encodedCredentials, new String[]{}/* this parameter does nothing, but is required */);

		return ("Logon successful as " + uid);
	}


	public void connectToReportServer ()
		{		
			// Default URL for CRN Content Manager
			String endPoint = "http://localhost:9300/p2pd/servlet/dispatch";      
			
			// Retrieve the service           
			service = new CognosReportNetServiceLocator();

			try
			{
				oCrn = service.getCognosReportNetPort(new java.net.URL(endPoint));
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
}
