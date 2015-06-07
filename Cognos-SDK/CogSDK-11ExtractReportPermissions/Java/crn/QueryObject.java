/**
 * QueryObject.java
 * 
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 * 
 * Description: (KB 1011329) - SDK sample to extract all the permissions out for each report 
 */


import org.apache.axis.encoding.Base64;
import com.cognos.developer.schemas.bibus._2.*;


public class QueryObject {

	public CognosReportNetServiceLocator service = null;
	public CognosReportNetPortType 		 oCrn    = null;
	// Default URL for CRN Content Manager. Change if not using default gateway
	public final String CM_URL = "http://localhost/crn/cgi-bin/cognos.cgi"; 
		  
	public CognosReportNetPortType getCrn() {return oCrn;}
	
	public static void main(String[] args) {
		
		QueryObject cp = new QueryObject();
		//logon info. Comment next line if Anonymous is enabled
		cp.quickLogon("DLDAP", "admin", "password");
		
		//SearchPath to the report
		String	searchPath = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']";

		PropEnum props[] = cp.getAllPropEnum();
		
		try
		{
			
			BaseClass bc[] = cp.oCrn.query(searchPath, props,
					new Sort[]{}, new QueryOptions());
			if (bc != null && bc.length > 0)
				for (int i=0; i<bc.length; i++)
				{
					System.out.println("Report " + bc[i].getSearchPath().getValue());
					Policy p[] = bc[i].getPolicies().getValue();
					if (p != null && p.length > 0)
					{
						for (int j=0; j<p.length; j++)
						{
							System.out.println(p[j].getSecurityObject().getSearchPath().getValue());
							Permission perm[] = p[j].getPermissions();
							for (int k=0; k<perm.length; k++)
								System.out.println("  Permission "+perm[k].getName() +" is "+ perm[k].getAccess().getValue());
						}
					}
			}
			
			System.out.println("Done");
	
		}catch (Exception e)
		{
			e.printStackTrace() ;
		}
		
				
	}
	
	public PropEnum[] getAllPropEnum ()
	  {
	    PropEnum properties[] = new PropEnum[]{
	      PropEnum.active,
	      PropEnum.actualCompletionTime,
	      PropEnum.actualExecutionTime,
	      PropEnum.advancedSettings,
	      PropEnum.ancestors,
	      PropEnum.asOfTime,
	      PropEnum.base,
	      PropEnum.brsAffineConnections,
	      PropEnum.brsMaximumProcesses,
	      PropEnum.brsNonAffineConnections,
	      PropEnum.burstKey,
	      PropEnum.businessPhone,
	      PropEnum.canBurst,
	      PropEnum.capabilities,
	      PropEnum.capacity,
	      PropEnum.configuration,
	      PropEnum.connections,
	      PropEnum.connectionString,
	      PropEnum.consumers,
	      PropEnum.contact,
	      PropEnum.contactEMail,
	      PropEnum.contentLocale,
	      PropEnum.creationTime,
	      PropEnum.credential,
	      PropEnum.credentialNamespaces,
	      PropEnum.credentials,
	      PropEnum.dailyPeriod,
	      PropEnum.data,
	      PropEnum.dataSize,
	      PropEnum.dataType,
	      PropEnum.defaultDescription,
	      PropEnum.defaultName,
	      PropEnum.defaultOutputFormat,
	      PropEnum.defaultScreenTip,
	      PropEnum.defaultTriggerDescription,
	      PropEnum.deployedObject,
	      PropEnum.deployedObjectAncestorDefaultNames,
	      PropEnum.deployedObjectClass,
	      PropEnum.deployedObjectDefaultName,
	      PropEnum.deployedObjectStatus,
	      PropEnum.deployedObjectUsage,
	      PropEnum.deploymentOptions,
	      PropEnum.description,
	      PropEnum.disabled,
	      PropEnum.dispatcherID,
	      PropEnum.dispatcherPath,
	      PropEnum.displaySequence,
	      PropEnum.email,
	      PropEnum.endDate,
	      PropEnum.endType,
	      PropEnum.eventID,
	      PropEnum.everyNPeriods,
	      PropEnum.executionDetails,
	      PropEnum.executionFormat,
	      PropEnum.executionLocale,
	      PropEnum.executionPageDefinition,
	      PropEnum.executionPageOrientation,
	      PropEnum.executionPrompt,
	      PropEnum.faxPhone,
	      PropEnum.format,
	      PropEnum.givenName,
	      PropEnum.governors,
	      PropEnum.hasChildren,
	      PropEnum.hasMessage,
	      PropEnum.height,
	      PropEnum.homePhone,
	      PropEnum.horizontalElementsRenderingLimit,
	      PropEnum.identity,
	      PropEnum.isolationLevel,
	      PropEnum.jsmNonPeakDemandBeginHour,
	      PropEnum.jsmNonPeakDemandMaximumJobs,
	      PropEnum.jsmPeakDemandBeginHour,
	      PropEnum.jsmPeakDemandMaximumJobs,
	      PropEnum.lastConfigurationModificationTime,
	      PropEnum.lastPage,
	      PropEnum.loadBalancingMode,
	      PropEnum.locale,
	      PropEnum.location,
	      PropEnum.lsAuditAdminLevel,
	      PropEnum.lsAuditLevel,
	      PropEnum.lsAuditOtherLevel,
	      PropEnum.lsAuditUsageLevel,
	      PropEnum.members,
	      PropEnum.metadataModel,
	      PropEnum.mobilePhone,
	      PropEnum.model,
	      PropEnum.modelName,
	      PropEnum.modificationTime,
	      PropEnum.monthlyAbsoluteDay,
	      PropEnum.monthlyRelativeDay,
	      PropEnum.monthlyRelativeWeek,
	      PropEnum.name,
	      PropEnum.namespaceFormat,
	      PropEnum.objectClass,
	      PropEnum.output,
	      PropEnum.owner,
	      PropEnum.packageBase,
	      PropEnum.page,
	      PropEnum.pageOrientation,
	      PropEnum.pagerPhone,
	      PropEnum.parameters,
	      PropEnum.parent,
	      PropEnum.paths,
	      PropEnum.permissions,
	      PropEnum.policies,
	      PropEnum.portalPage,
	      PropEnum.portalPreferences,
	      PropEnum.position,
	      PropEnum.postalAddress,
	      PropEnum.printerAddress,
	      PropEnum.productLocale,
	      PropEnum.qualifier,
	      PropEnum.related,
	      PropEnum.recipientsEMail,
	      PropEnum.recipients,
	      PropEnum.related,
	      PropEnum.replacement,
	      PropEnum.requestedExecutionTime,
	      PropEnum.retentions,
	      PropEnum.rsAffineConnections,
	      PropEnum.rsMaximumProcesses,
	      PropEnum.rsNonAffineConnections,
	      PropEnum.rsQueueLimit,
	      PropEnum.runAsOwner,
	      PropEnum.runningState,
	      PropEnum.runOptions,
	      PropEnum.screenTip,
	      PropEnum.searchPath,
	      PropEnum.searchPathForURL,
	      PropEnum.sequencing,
	      PropEnum.serverGroup,
	      PropEnum.source,
	      PropEnum.specification,
	      PropEnum.startAsActive,
	      PropEnum.startDate,
	      PropEnum.state,
	      PropEnum.status,
	      PropEnum.stepObject,
	      PropEnum.surname,
	      PropEnum.target,
	      PropEnum.taskID,
	      PropEnum.timeZoneID,
	      PropEnum.triggerDescription,
	      PropEnum.triggerName,
	      PropEnum.type,
	      PropEnum.unit,
	      PropEnum.uri,
	      PropEnum.usage,
	      PropEnum.user,
	      PropEnum.userCapabilities,
	      PropEnum.userCapability,
	      PropEnum.userName,
	      PropEnum.version,
	      PropEnum.verticalElementsRenderingLimit,
	      PropEnum.viewed,
	      PropEnum.weeklyFriday,
	      PropEnum.weeklyMonday,
	      PropEnum.weeklySaturday,
	      PropEnum.weeklySunday,
	      PropEnum.weeklyThursday,
	      PropEnum.weeklyTuesday,
	      PropEnum.weeklyWednesday,
	      PropEnum.yearlyAbsoluteDay,
	      PropEnum.yearlyAbsoluteMonth,
	      PropEnum.yearlyRelativeDay,
	      PropEnum.yearlyRelativeMonth,
	      PropEnum.yearlyRelativeWeek,
	    };
	    return properties;
	  }
		
		public QueryObject ()
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
		
//This method loggs the user to ReportNet
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
}
