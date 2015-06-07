//  Copyright © 2003 Cognos Incorporated. All Rights Reserved.
//  Cognos and the Cognos logo are trademarks of Cognos Incorporated.

// Sample ReportNet "query" usage.

using System;
using System.Windows.Forms;
using System.Web.Services.Protocols;
using cognosdotnet;
using SamplesCommon;

namespace cmQuery
{
	/// <summary>
	/// cmQuery is a simple command-line application that lets you explore
	/// the ReportNet Content Manager/Content Store database.
	/// </summary>
	public class cmQuery
	{
		/// <summary>
		/// No constructor; this class only holds the Main() method used
		/// to demonstrate the ReportNet query.
		/// </summary>
		public cmQuery()
		{
		}

		public string doQuery(CognosReportNetBinding reportNet, 
			string searchPath, 
			string userName, 
			string userPassword, 
			string userNamespace,
			bool isGUImode)
		{
			string output = "";
			try
			{
				// Search properties: we need the defaultName and the searchPath.
				propEnum[] properties =
				{ propEnum.defaultName, propEnum.searchPath };

				// Sort options: ascending sort on the defaultName property.
				sort[] sortBy = { new sort()};
				sortBy[0].order = orderEnum.ascending;
				sortBy[0].propName = propEnum.defaultName;

				// Query options; use the defaults.
				queryOptions options = new queryOptions();

				// Add the authentication information, if any.
				//
				// Another option would be to use the logon() and logonAs() methods...
				CAM cam = new CAM();
				cam.action = "logonAs";

				hdrSession header = new hdrSession();
				if ((userName != null) && (0 != userName.CompareTo("")) )
				{
					formFieldVar[] vars = new formFieldVar[3];

					vars[0] = new formFieldVar();
					vars[0].name = "CAMNamespace";
					vars[0].value = userNamespace;
					vars[0].format = formatEnum.not_encrypted;

					vars[1] = new formFieldVar();
					vars[1].name = "CAMUsername";
					vars[1].value = userName;
					vars[1].format = formatEnum.not_encrypted;

					vars[2] = new formFieldVar();
					vars[2].name = "CAMPassword";
					vars[2].value = userPassword;
					vars[2].format = formatEnum.not_encrypted;

					header.formFieldVars = vars;
				}
				else
				{
					cam.action = "logon";
				}

				biBusHeader bibus = new biBusHeader();
				bibus.CAM = cam;
				bibus.hdrSession = header;

				reportNet.biBusHeaderValue = bibus;

				// Make the query.
				baseClass[] results =
					reportNet.query(searchPath, properties, sortBy, options);

				// Display the results.
				output += "Results:\n\n";
				for (int i = 0; i < results.GetLength(0); i++)
				{
					tokenProp theDefaultName = results[i].defaultName;
					stringProp theSearchPath = results[i].searchPath;

					output += "\t" + theDefaultName.value + "\t\t" + theSearchPath.value + "\n";
				}
			}
			catch(SoapException ex)
			{
				SamplesException.ShowExceptionMessage("\n" + ex, isGUImode, "Content Manager Query Sample - doQuery()" );
				return "The error occurred in doQuery()";
			}
			catch(System.Exception ex)
			{
				if (0 != ex.Message.CompareTo("INPUT_CANCELLED_BY_USER"))
				{
					SamplesException.ShowExceptionMessage("\n" + ex.Message, isGUImode, "Content Manager Query Sample - doQuery()" );					
				}
				return "The error occurred in doQuery()";
			}
			return output;
		}
		/// <summary>
		/// Application entry point.
		/// </summary>
		/// <param name="args">Standard command-line arguments.</param>
		public static void Main( string[] args )
		{
			// Attempt a simple CM query.

			string searchPath = "//report";
			string userName = "admin";
			string userPassword = "password";
			string userNamespace = "DLDAP";

			// TODO: should change the command-line processing to handle:
			//
			// -host hostName
			// -port portName
			// -search searchPath
			// -user userName
			// -password userPassword
			// -namespace userNamespace
			//
			// ie, it should handle similar arguments to reportrunner

			// Process command-line arguments.
			//
			// cmQuery accepts these arguments:
			//
			// --search=searchPath
			// --uid=userName
			// --pwd=userPassword
			// --namespace=userNamespace
			char[] arg_separator = { '=' };
			foreach( string arg in args )
			{
				string[] command = arg.Split( arg_separator, 2 );

				switch( command[0] )
				{
				case "--search":
					searchPath = command[1];
					break;
				case "--uid":
					userName = command[1];
					break;
				case "--pwd":
					userPassword = command[1];
					break;
				case "--namespace":
					userNamespace = command[1];
					break;
				default:
					throw new ApplicationException( "Unknown argument: " + arg );
				}
			}

			// Concatenate the read filter to the searchPath this way we
			// ask CM to only return the objects we have read acces on.
			searchPath = searchPath + "[permission('read')]";

			// Create the ReportNet connection object.
			Console.WriteLine( "Creating CognosReportNetService..." );
			CognosReportNetBinding crn = new CognosReportNetBinding();
			crn.Url = "http://localhost:9300/p2pd/servlet/dispatch";
			

			// Add the authentication information, if any.
			//
			// Another option would be to use the logon() methods...
			CAM cam = new CAM();
			cam.action = "logonAs";

			hdrSession header = new hdrSession();
			if( userName != null )
			{
				formFieldVar[] vars = new formFieldVar[3];

				vars[0] = new formFieldVar();
				vars[0].name = "CAMNamespace";
				vars[0].value = userNamespace;
				vars[0].format = formatEnum.not_encrypted;

				vars[1] = new formFieldVar();
				vars[1].name = "CAMUsername";
				vars[1].value = userName;
				vars[1].format = formatEnum.not_encrypted;

				vars[2] = new formFieldVar();
				vars[2].name = "CAMPassword";
				vars[2].value = userPassword;
				vars[2].format = formatEnum.not_encrypted;

				header.formFieldVars = vars;
			}
			else
			{
				cam.action = "logon";
			}

			biBusHeader bibus = new biBusHeader();
			bibus.CAM = cam;
			bibus.hdrSession = header;

			crn.biBusHeaderValue = bibus;


			try 

			{

				propEnum[] props = new propEnum[] { propEnum.searchPath, 
													  propEnum.defaultName, propEnum.policies, 
													  propEnum.permissions, propEnum.members };

				sort[] s = new sort[]{ new sort() };

				s[0].order = orderEnum.ascending;

				s[0].propName = propEnum.defaultName;

				queryOptions qo = new queryOptions(); 
				// Look for all of the reports.

				Console.WriteLine( "\nReports:\n" );

				baseClass[] bc = crn.query( "/content//report", props, s, qo );

				if( bc.Length > 0 ) 
				{
					foreach( baseClass report_item in bc ) 

					{ 
						Console.WriteLine( "  {0}", report_item.searchPath.value);
						policy[] p = report_item.policies.value;
						if (p.Length > 0)
						{
							foreach( policy pol in p )
							{
								Console.WriteLine( "  {0}", pol.securityObject.searchPath.value);
								permission[] perm = pol.permissions;
								foreach( permission prm in perm )
									Console.WriteLine( "  {0}   {1}",prm.name, prm.access.ToString());
							}
						}
					}
				}
			}

			catch( System.Web.Services.Protocols.SoapHeaderException ex )
			{
				Console.WriteLine( "SOAP Header Exception:" );
				Console.WriteLine( "Actor  : " + ex.Actor );
				Console.WriteLine( "Code   : " + ex.Code );
				Console.WriteLine( "Detail : " + ex.Detail );
				Console.WriteLine( "Message: " + ex.Message );

				// We can access the SOAP fault information through
				// the ex.Detail property.
				System.Xml.XmlNode node = ex.Detail;
				Console.WriteLine( node.OuterXml );
			}
			catch( System.Web.Services.Protocols.SoapException ex )
			{
				Console.WriteLine( "SOAP Exception:" );
				Console.WriteLine( "Actor  : " + ex.Actor );
				Console.WriteLine( "Code   : " + ex.Code );
				Console.WriteLine( "Detail : " + ex.Detail );
				Console.WriteLine( "Message: " + ex.Message );

				// We can access the SOAP fault information through
				// the ex.Detail property.
				System.Xml.XmlNode node = ex.Detail;
				Console.WriteLine( node.OuterXml );
			}
		}
	}
}
