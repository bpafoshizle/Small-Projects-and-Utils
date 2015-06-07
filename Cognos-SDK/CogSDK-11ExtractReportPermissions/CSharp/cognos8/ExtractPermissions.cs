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
		public cmQuery() {}

		
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
			contentManagerService1 cmService = new contentManagerService1();
			cmService.Url = "http://localhost:9300/p2pd/servlet/dispatch";
			
			//logon to Cognos 8
			logon(cmService, userName, userPassword, userNamespace);

			propEnum[] props = new propEnum[] { propEnum.searchPath, 
												  propEnum.defaultName, propEnum.policies, 
												  propEnum.permissions, propEnum.members };

			sort[] s = new sort[]{ new sort() };

			s[0].order = orderEnum.ascending;

			s[0].propName = propEnum.defaultName;

			queryOptions qo = new queryOptions(); 
			
			Console.WriteLine( "\nReports:\n" );
			// Look for all of the reports.
			searchPathMultipleObject spMulti = new searchPathMultipleObject();
			spMulti.Value = "/content//report";
			baseClass[] bc = null;

			try 
			{
				bc = cmService.query(spMulti, props, s, qo );

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
				displayException(ex);
			}
			catch( System.Web.Services.Protocols.SoapException ex )
			{
				displayException(ex);
			}
		}
		
		public static void logon(contentManagerService1 cmService,
			string userName, string userPassword, string userNamespace)
		{
			try 
			{
				System.Text.StringBuilder credentialXML = new System.Text.StringBuilder("<credential>" );
				credentialXML.AppendFormat( "<namespace>{0}</namespace>", userNamespace );
				credentialXML.AppendFormat( "<username>{0}</username>", userName );
				credentialXML.AppendFormat( "<password>{0}</password>", userPassword );
				credentialXML.Append( "</credential>" );

				//TODO - encode this, don't just toString it.
				string encodedCredentials = credentialXML.ToString ();
				xmlEncodedXML xmlEncodedCredentials = new xmlEncodedXML();
				xmlEncodedCredentials.Value = encodedCredentials;
				
				cmService.logon(xmlEncodedCredentials, null);
			
			}
			catch( SoapException ex ) 
			{
				displayException(ex);
			}
			catch (System.Exception ex)
			{
				displayException(ex);
			}
		}

		private static void displayException(Exception ex)
		{
			Console.WriteLine( "SOAP Exception:" );
			Console.WriteLine(ex.StackTrace);
		}
	}
}
