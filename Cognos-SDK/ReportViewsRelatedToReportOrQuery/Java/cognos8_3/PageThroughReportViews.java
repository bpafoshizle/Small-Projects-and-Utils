/**
 * PageThroughReportViews.java
 *
 * Copyright © Cognos Incorporated. All Rights Reserved.
 * Cognos and the Cognos logo are trademarks of Cognos Incorporated.
 *
 * Description: (KB 1037706) - SDK How to page through report views to find ones related to a specific report or query
 * 
 * Tested in 8.3
 */

import java.math.BigInteger;

import com.cognos.developer.schemas.bibus._3.BaseClass;
import com.cognos.developer.schemas.bibus._3.BaseClassArrayProp;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_Port;
import com.cognos.developer.schemas.bibus._3.ContentManagerService_ServiceLocator;
import com.cognos.developer.schemas.bibus._3.PropEnum;
import com.cognos.developer.schemas.bibus._3.Query;
import com.cognos.developer.schemas.bibus._3.QueryOptions;
import com.cognos.developer.schemas.bibus._3.Report;
import com.cognos.developer.schemas.bibus._3.ReportView;
import com.cognos.developer.schemas.bibus._3.SearchPathMultipleObject;
import com.cognos.developer.schemas.bibus._3.Sort;
import com.cognos.developer.schemas.bibus._3.StringProp;
import com.cognos.developer.schemas.bibus._3.XmlEncodedXML;
import org.apache.axis.AxisFault;

public class PageThroughReportViews {
	private ContentManagerService_ServiceLocator cmServiceLocator = null;

	private ContentManagerService_Port cmService = null;

	public final static String CM_URL = "http://localhost:9300/p2pd/servlet/dispatch";

	private String objSearchPath = "//reportView";

	private String baseReportSearchPath = "/content/package[@name='GO Data Warehouse (query)']/folder[@name='SDK Report Samples']/query[@name='Product Cost List']";

	public static void main(String[] args) {
		PageThroughReportViews dObj = new PageThroughReportViews(CM_URL);
		dObj.quickLogon("namespaceID", "username", "password");
		dObj.getPages(dObj.getobjSearchPath(), dObj.getStoreID(dObj.getBaseReportSearchPath()));

	}

	public String getStoreID(String p_objSearchPath) {
		PropEnum props[] = new PropEnum[] { PropEnum.searchPath,
				PropEnum.storeID };
		String stringStoreID = "";
		if (p_objSearchPath != null && p_objSearchPath != "") {
			try {
				SearchPathMultipleObject spMulti = new SearchPathMultipleObject();
				spMulti.setValue(p_objSearchPath);

				QueryOptions options = new QueryOptions();
				BaseClass bc[] = cmService.query(spMulti, props, new Sort[] {},
						options);

				for (int i = 0; i < bc.length; i++) {
					StringProp theSearchPath = bc[i].getSearchPath();
					stringStoreID = bc[i].getStoreID().getValue().getValue();

					System.out.println("SearchPath of base object\t = "
							+ theSearchPath.getValue());
					System.out.println("storeID of base object\t = " + stringStoreID);
				}

			} catch (AxisFault ex) {
				useAxisInterface_dumpToString(ex);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return stringStoreID;
	}

	public void getPages(String p_objSearchPath, String stringStoreID) {
		PropEnum props[] = new PropEnum[] { PropEnum.searchPath, PropEnum.base };

		if (p_objSearchPath != null && p_objSearchPath != "") {
			try {
				SearchPathMultipleObject spMulti = new SearchPathMultipleObject();
				spMulti.setValue(p_objSearchPath);

				QueryOptions options = new QueryOptions();

				BigInteger maxObjs = new BigInteger("15");
				options.setMaxObjects(maxObjs);

				BigInteger skipObjs = new BigInteger("0");
				options.setSkipObjects(skipObjs);

				BaseClass bc[] = cmService.query(spMulti, props, new Sort[] {},
						options);
				BigInteger n = new BigInteger("1");

				while (bc.length > 0) {
					System.out.println("~~~~~~~~~~~~~~~~~~~Results from query "
							+ n + " ~~~~~~~~~~~~~~~~~~~");
					for (int i = 0; i < bc.length; i++) {
						StringProp theSearchPath = bc[i].getSearchPath();
						if (bc[i] instanceof ReportView) {
							ReportView rv = (ReportView) bc[i];
							BaseClassArrayProp bcap = rv.getBase();
							BaseClass[] rbc = bcap.getValue();
							for (int j = 0; j < rbc.length; j++) {
								if (rbc[j] instanceof Report || rbc[j] instanceof Query) {
									String rvBRstoreID = rbc[j].getStoreID().getValue()
									.getValue();
									if (rvBRstoreID.equals(stringStoreID)){
										System.out.println("Found report view based on report|query");
										System.out.println("SearchPath of report view\t = "
												+ theSearchPath.getValue());
										System.out
												.println("SearchPath of base report|query\t = "
														+ rbc[j].getSearchPath()
																.getValue());
										System.out.println("storeID of base report|query\t = "
												+ rvBRstoreID);
										
									}
								}
							}
						}
					}
					n = n.add(new BigInteger("1"));
					skipObjs = (n.subtract(new BigInteger("1")))
							.multiply(maxObjs);
					options.setSkipObjects(skipObjs);
					bc = cmService
							.query(spMulti, props, new Sort[] {}, options);
				}

			} catch (AxisFault ex) {
				useAxisInterface_dumpToString(ex);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String getBaseReportSearchPath() {
		return baseReportSearchPath;
	}

	public String getobjSearchPath() {
		return objSearchPath;
	}

	// This method logs the user to Cognos 8
	public String quickLogon(String namespace, String uid, String pwd) {
		StringBuffer credentialXML = new StringBuffer();

		credentialXML.append("<credential>");
		credentialXML.append("<namespace>").append(namespace).append(
				"</namespace>");
		credentialXML.append("<username>").append(uid).append("</username>");
		credentialXML.append("<password>").append(pwd).append("</password>");
		credentialXML.append("</credential>");

		String encodedCredentials = credentialXML.toString();
		XmlEncodedXML xmlCredentials = new XmlEncodedXML();
		xmlCredentials.setValue(encodedCredentials);
		try {
			cmService.logon(xmlCredentials, null);
		} catch (AxisFault ex) {
			useAxisInterface_dumpToString(ex);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ("Logon successful as " + uid);
	}

	// This method connects to Cognos 8
	public PageThroughReportViews(String gateway) {
		// Retrieve the service
		cmServiceLocator = new ContentManagerService_ServiceLocator();
		try {
			cmService = cmServiceLocator
					.getcontentManagerService(new java.net.URL(CM_URL));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void useAxisInterface_dumpToString(AxisFault ex) {
		String details = ex.dumpToString();
		System.out.println("\n\n1) CALL dumpToString:");
		System.out.println("-------------------------\n");
		String message = ex.getFaultString();
		System.out.println("message: " + message + "\n");
		System.out.println(details);
	}

}
