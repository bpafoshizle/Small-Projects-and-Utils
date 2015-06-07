/*
 * Description: KB 1020887 - SDK sample to remove a security object from Permissions tab 
 */
 
using System;
using System.Drawing;
using System.Collections;
using System.ComponentModel;
using System.Windows.Forms;
using System.Data;
using System.Web.Services;
using System.Web;
using cognosdotnet;

namespace Permissions
{
	public class SetPermissions : System.Windows.Forms.Form
	{
		private System.ComponentModel.Container components = null;

		public SetPermissions()
		{
			InitializeComponent();
		}

		protected override void Dispose( bool disposing )
		{
			if( disposing )
			{
				if (components != null) 
				{
					components.Dispose();
				}
			}
			base.Dispose( disposing );
		}

		#region Windows Form Designer generated code
		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			// 
			// Form1
			// 
			this.AutoScaleBaseSize = new System.Drawing.Size(5, 13);
			this.ClientSize = new System.Drawing.Size(292, 266);
			this.Name = "Form1";
			this.Text = "Form1";
			this.Load += new System.EventHandler(this.Form1_Load);

		}
		#endregion

		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		static void Main(string[] args)
		{
			contentManagerService1 cmService = null;
			string userSearchPath = "CAMID(\"DLDAP:u:uid=dd,ou=people\")";
			string sPath = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']";
			string C8URL = "http://localhost:9330/p2pd/servlet/dispatch";
			
			cmService = new contentManagerService1 ();
			cmService.Url = C8URL;

			SetPermissions permObj = new SetPermissions();

			//if Anonymous is disabled, login with valid credentials
			string userID = "admin";	//user ID
			string pass = "password";	//password
			string nsID = "DLDAP";		//namespaceID as defined in Cognos Configuration
			permObj.UserLogon(userID, pass, nsID, cmService);
			
			//to add an object to permissions uncomment the following line
			//permObj.setPermissionsOnFolders(userSearchPath, cmService, sPath) ;

			//delete an object from permissions 
			permObj.deleteUserFromPermissions(userSearchPath,  cmService, sPath) ;
							
				
			}
		

		public void setPermissionsOnFolders(string userSearchPath, contentManagerService1 cmService, string sPath) 
		{
			searchPathMultipleObject spMulti = new searchPathMultipleObject();
			spMulti.Value = sPath;
			folder pFolder = (folder)cmService.query(spMulti, new propEnum[]{propEnum.searchPath,propEnum.policies},new sort[]{},new queryOptions())[0];

			bool found = false;
			permission newPermission = new permission();
			newPermission.name ="execute";
			newPermission.access = accessEnum.deny;

			for (int i = 0; i < pFolder.policies.value.Length && !found; i ++)
			{
				policy policy = pFolder.policies.value[i];
				//If the security object already exists, update its permissions
				if(policy.securityObject.searchPath.value.Equals(userSearchPath))
				{
					found = true;
					permission[] newPerms = new permission[policy.permissions.Length + 1];
					for(int j = 0; j < policy.permissions.Length; j ++)
					{
						newPerms[j] = policy.permissions[j];
					}
					newPerms[newPerms.Length - 1] = newPermission;
					policy.permissions = newPerms;
				}
			}
			//If the security object does not exist, create a new one
			if(!found)
			{
				baseClass entry = null;
				spMulti.Value = userSearchPath;
				entry = cmService.query(spMulti, new propEnum[]{},new sort[]{},new queryOptions())[0];

				policy newPolicy = new policy();
				newPolicy.securityObject = entry;
				permission[] permissions = new permission[1];
				permissions[0] = newPermission;
				newPolicy.permissions = permissions;

				policyArrayProp existingPols = pFolder.policies ;
				policy[] newPols = new policy[existingPols.value.Length + 1];
				for(int j = 0; j < existingPols.value.Length; j ++)
				{
					newPols[j] = existingPols.value[j];
				}
				newPols[newPols.Length - 1] = newPolicy;
				existingPols.value = newPols;
			}

			cmService.update(new baseClass[]{pFolder},new updateOptions() );

		}

		public void deleteUserFromPermissions(string userSearchPath, contentManagerService1 cmService, string sPath) 
		{
			//set searchPath of the required folder
			searchPathMultipleObject spMulti = new searchPathMultipleObject();
			spMulti.Value = sPath;
			folder pFolder = (folder)cmService.query(spMulti, new propEnum[]{propEnum.searchPath,propEnum.policies},new sort[]{},new queryOptions())[0];

			//keep trak if the aecurity object was found in the permissions
			bool found = false;
						
			for (int i = 0; i < pFolder.policies.value.Length && !found; i ++)
			{
				policy policy = pFolder.policies.value[i];
				//If the security object already exists, update its permissions
				if(policy.securityObject.searchPath.value.Equals(userSearchPath))
				{
					found = true;
				}
			}
			//if the security object exists, remove it from the array of permissions
			if (found)
			{
				policy[] newPolicies = new policy[pFolder.policies.value.Length-1];
				policyArrayProp pArray = new policyArrayProp();
				for (int i = 0; i < pFolder.policies.value.Length ; i ++)
				{
					policy policy = pFolder.policies.value[i];
					if(!policy.securityObject.searchPath.value.Equals(userSearchPath))
					{
						newPolicies[i] = new policy ();
						newPolicies[i].securityObject = pFolder.policies.value[i].securityObject;
						newPolicies[i].permissions  = pFolder.policies.value[i].permissions;
					}
				
				}
				//If the security object does not exist, nothing to delete
				pArray.value = newPolicies;
				pFolder.policies = pArray;
				cmService.update(new baseClass[]{pFolder},new updateOptions() );
			}
		}
	
		public void UserLogon(string userName, string userPassword, string userNamespace, contentManagerService1 cmService)
		{
			try 
			{
				System.Text.StringBuilder credentialXML = new System.Text.StringBuilder("<credential>" );
				credentialXML.AppendFormat( "<namespace>{0}</namespace>", userNamespace );
				credentialXML.AppendFormat( "<username>{0}</username>", userName );
				credentialXML.AppendFormat( "<password>{0}</password>", userPassword );
				credentialXML.Append( "</credential>" );

				//The csharp toolkit encodes the credentials
				string encodedCredentials = credentialXML.ToString ();
				xmlEncodedXML xmlEncodedCredentials = new xmlEncodedXML();
				xmlEncodedCredentials.Value = encodedCredentials;
				searchPathSingleObject[] emptyRoleSearchPathList = new searchPathSingleObject[0];	
				cmService.logon(xmlEncodedCredentials, null);
				
			}
			catch (System.Exception ex)
			{
				Console.Write(ex.Message);				
			}
		}

		private void Form1_Load(object sender, System.EventArgs e)
		{
		
		}
	}
}
