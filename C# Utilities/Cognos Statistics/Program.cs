using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;
using cognosdotnet_2_0;
using System.Web.Services.Protocols;
using Microsoft.Office.Interop.Excel;
using System.Reflection;
using Microsoft.Practices.EnterpriseLibrary.Common;
using Microsoft.Practices.EnterpriseLibrary.Data;
using System.Data.Common;
using System.Data;


namespace Tyson.BI.IS.Cognos.SDK.CognosStatistics
{
    class CogStats
    {
        contentManagerService1 cCMS;
        contentManagerService1 c11CMS;

        string cogVersion8;
        string cogVersion11;
        string cog8URL;
        string cog11URL;
        string cog8BaseReportPath;
        string cog11BaseReportPath;

        // User, Pass, and Namespace are common to both environments
        string user;
        string pass;
        string ns;

        public CogStats()
        {
            cCMS = new contentManagerService1();

            cogVersion8 = "Cognos8";
            cogVersion11 = "Cognos11";
            cog8URL = "http://cogpbi1.tyson.com:9300/p2pd/servlet/dispatch";
            //cog11URL = "http://reprd2.tyson.com:9300/crn11mr2/cgi-bin/cognos.cgi";
            cog8BaseReportPath = "http://perform.tyson.com/cognos8/cgi-bin/cognosisapi.dll?b_action=xts.run&m=portal/report-viewer.xts&ui.action=run&ui.object=";
            cog11BaseReportPath = "http://reports.tyson.com/crn/cgi-bin/cognosisapi.dll?b_action=xts.run&m=portal/report-viewer.xts&method=execute&m_obj=";

            cCMS.Url = cog8URL;
            cCMS.Timeout = -1;

            user = "CognosReportNet";
            pass = "En(Rp(CGNS10";
            ns = "ADS";
            specificUserLogon(false, cCMS, user, pass, ns);
        }
        static void Main(string[] args)
        {
            CogStats stats = new CogStats();
            /********************************************************************************/
            /* The following lines of code will extrace the database tables and fields from
             * all of the models in the content store
            stats.extractTablesAndFieldsFromModels();
             */
            /********************************************************************************/
            
            /********************************************************************************/
            /* The following lines of code will get all groups and roles with 
               members for the Cognos 8 environment and print them out */
            List<CogObject>cogObjects = stats.getGroupsAndRoles();
            printSecurityCogObjects(ref cogObjects);
            /********************************************************************************/

            /********************************************************************************/
            /* The Following line will disable Cognos objects given by a list of
               paths for those objects in a text file -- C8 only */
            //stats.disableCogObjects();
            /********************************************************************************/

            /********************************************************************************/
            /* Uncomment the following lines to get full C8 report */
            //List<CogObject> cogObjects = new List<CogObject>();
            //cogObjects = stats.getCog8ReportsWithSubElements();
            //cogObjects.AddRange(stats.getCog8QueriesWithSubElements());
            //cogObjects.AddRange(stats.getCog8ReportViewsWithSubElements());
            //cogObjects.AddRange(stats.getUsersMyFolderContentsWithSubElements());
            //cogObjects = stats.getUsersMyFolderContentsWithSubElements();
            //stats.printCogObjects(ref cogObjects);
            /********************************************************************************/

            /********************************************************************************/
            /* Run the following code to turn off the duration property for all objects
               and to print the objects with the property to an excel sheet for analysis.*/
            //int[] cellCount = { 1, 1 };
            //stats.turnOffReportDuration(cellCount);
            //stats.turnOffQueryDuration(cellCount);
            //stats.turnOffReportViewDuration(cellCount);
            /********************************************************************************/
        }

        /**
         * Method to print column headers to a spreadsheet
         */
        private static void printColumnHeaders(ref _Worksheet oSheet)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));

            oSheet.Cells[1, 1] = "ReportName";
            oSheet.Cells[1, 2] = "Disabled";
            oSheet.Cells[1, 3] = "PackageName";
            oSheet.Cells[1, 4] = "PackageDisabled";
            oSheet.Cells[1, 5] = "Version";
            oSheet.Cells[1, 6] = "ActiveSchedule";
            oSheet.Cells[1, 7] = "Type";
            oSheet.Cells[1, 8] = "BaseID";
            oSheet.Cells[1, 9] = "BasePath";
            oSheet.Cells[1, 10] = "MyFolder";
            oSheet.Cells[1, 11] = "Owner";
            oSheet.Cells[1, 12] = "Path";
            oSheet.Cells[1, 13] = "URL";
            oSheet.Cells[1, 14] = "CreationTime";
            //oSheet.Cells[1, 13] = "LastRunDate";
            //oSheet.Cells[1, 14] = "LastRunUser";
            //oSheet.Cells[1, 15] = "3MonthRuns";
            //oSheet.Cells[1, 16] = "3MonthUsers";
            //oSheet.Cells[1, 17] = "6MonthRuns";
            //oSheet.Cells[1, 18] = "6MonthUsers";
            //oSheet.Cells[1, 19] = "12MonthRuns";
            //oSheet.Cells[1, 20] = "12MonthUsers";
            //oSheet.Cells[1, 21] = "PrimaryDataSource";
            //oSheet.Cells[1, 22] = "Fields";
        }

        /**
         * Method to print column headers specific to SecurityOverview 
         * to a spreadsheet
         */
        private static void printSecurityColumnHeaders(ref _Worksheet oSheet)
        {
            oSheet.Cells[1, 1] = "User";
            oSheet.Cells[1, 2] = "Category Type";
            oSheet.Cells[1, 3] = "Name";
        }

        /**
         * Method to print the attributes of the cog objects out to a spreadsheet
         */
        private static void printCogObjects(ref List<CogObject> cogObjects)
        {

            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            // Print out the column headers first
            printColumnHeaders(ref oSheet);

            /*
            * Loop through all the reports and print values to spreadsheet
            */
            for (int i = 0; i < cogObjects.Count; i++)
            {
                CogObject cog = cogObjects[i];
                oSheet.Cells[i + 2, 1] = cog.getAttributeValue("Name");
                oSheet.Cells[i + 2, 2] = cog.getAttributeValue("Disabled");
                oSheet.Cells[i + 2, 3] = cog.getAttributeValue("Package_Name");
                oSheet.Cells[i + 2, 4] = cog.getAttributeValue("Package_Disabled");
                oSheet.Cells[i + 2, 5] = cog.getAttributeValue("Version");
                oSheet.Cells[i + 2, 6] = cog.getAttributeValue("ActiveSchedule");
                oSheet.Cells[i + 2, 7] = cog.getAttributeValue("Type");
                oSheet.Cells[i + 2, 8] = cog.getAttributeValue("BaseID");
                oSheet.Cells[i + 2, 9] = cog.getAttributeValue("BasePath");
                oSheet.Cells[i + 2, 10] = cog.getAttributeValue("MyFolder");
                oSheet.Cells[i + 2, 11] = cog.getAttributeValue("Author_Name");
                oSheet.Cells[i + 2, 12] = cog.getAttributeValue("Path");
                oSheet.Cells[i + 2, 13] = cog.getAttributeValue("URL");
                oSheet.Cells[i + 2, 14] = cog.getAttributeValue("CreationTime");
                //oSheet.Cells[i + 2, 13] = cog.getAttributeValue("lastTime");
                //oSheet.Cells[i + 2, 14] = cog.getAttributeValue("lastUser");
                //oSheet.Cells[i + 2, 15] = cog.getAttributeValue("timesRun3Months");
                //oSheet.Cells[i + 2, 16] = cog.getAttributeValue("numUsers3Months");
                //oSheet.Cells[i + 2, 17] = cog.getAttributeValue("timesRun6Months");
                //oSheet.Cells[i + 2, 18] = cog.getAttributeValue("numUsers6Months");
                //oSheet.Cells[i + 2, 19] = cog.getAttributeValue("timesRun1Year");
                //oSheet.Cells[i + 2, 20] = cog.getAttributeValue("numUsers1Year");
            }
        }

        /**
         * Method to print the attributes of the cog objects specific to 
         * SecurityOverview out to a spreadsheet
         */
        private static void printSecurityCogObjects(ref List<CogObject> cogObjects)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            printSecurityColumnHeaders(ref oSheet);

            /*
            * Loop through all the reports and print values to spreadsheet
            */
            for (int i = 0; i < cogObjects.Count; i++)
            {
                CogObject cog = cogObjects[i];
                oSheet.Cells[i + 2, 1] = cog.getAttributeValue("user");
                oSheet.Cells[i + 2, 2] = cog.getAttributeValue("categoryType");
                oSheet.Cells[i + 2, 3] = cog.getAttributeValue("categoryName");
            }
        }

        private int recursiveExcelCogObjects(List<CogObject> cogObjects, 
                                             Microsoft.Office.Interop.Excel._Worksheet oSheet,
                                             ref int rowCount, int columnsToSkip)
        {
            if (cogObjects == null || cogObjects.Count <= 0)
            {
                return 0;
            }
            else
            {
                oSheet.Cells[rowCount, 1 + columnsToSkip] = "There are " + cogObjects.Count + " elements on this level";
                rowCount++;
                foreach (CogObject cog in cogObjects)
                {
                    foreach (KeyValuePair<string, string> pair in cog.AttributeList)
                    {
                        oSheet.Cells[rowCount, 1 + columnsToSkip] = pair.Key.ToString() + ": " + pair.Value.ToString();
                        rowCount++;
                    }
                    recursiveExcelCogObjects(cog.SubElements, oSheet, ref rowCount, columnsToSkip + 1);
                    rowCount++;
                }
                return 0;
            }
        }

        private int recursivePrintCogObjects(List<CogObject> cogObjects, System.IO.StreamWriter writer, string tabString)
        {
            if (cogObjects == null || cogObjects.Count <= 0)
            {
                return 0;
            }
            else
            {
                writer.Write(tabString + "There are " + cogObjects.Count + " elements on this level\r\n");
                foreach (CogObject cog in cogObjects)
                {
                    foreach (KeyValuePair<string, string> pair in cog.AttributeList)
                    {
                        writer.Write(tabString + "{0}: {1}\r\n", pair.Key.ToString(), pair.Value.ToString());
                    }
                    recursivePrintCogObjects(cog.SubElements, writer, tabString + "\t");
                    writer.Write(tabString + "**********************************************************************************\r\n");
                }
                return 0;
            }
        }


        private bool extractTablesAndFieldsFromModels()
        {
            bool success = true;
            System.IO.StreamWriter writer = new StreamWriter("ModelTablesAndFields.csv");
            writer.WriteLine("Model Name|Table Name|Field Name");

            // Same query options used for all calls
            queryOptions qo = new queryOptions();

            propEnum[] packageProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName };
            //model sorting
            sort[] pkgSort = new sort[] { new sort() };
            pkgSort[0].order = orderEnum.ascending;
            pkgSort[0].propName = propEnum.defaultName;
            searchPathMultipleObject packagePath = new searchPathMultipleObject();
            packagePath.Value = "//package";
             // Make call to get all queries. Get each author for each query during this process
            // by making a separate call, based on CAMID; the same as above when pulling the reports.
            baseClass[] bcPackages = cCMS.query(packagePath, packageProps, pkgSort, qo);

            foreach (baseClass pkg in bcPackages)
            {
                //Model properties
                propEnum[] modelProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName, propEnum.model };

                //model sorting
                sort[] mdlSort = new sort[] { new sort() };
                mdlSort[0].order = orderEnum.ascending;
                mdlSort[0].propName = propEnum.defaultName;

                searchPathMultipleObject modelsPath = new searchPathMultipleObject();
                modelsPath.Value = pkg.searchPath.value + "/model";

                // Make call to get all models. Get each author for each query during this process
                // by making a separate call, based on CAMID; the same as above when pulling the reports.
                baseClass[] bcmodels = cCMS.query(modelsPath, modelProps, mdlSort, qo);

                // Latest model should always be at the end of the array
                model latestModel = (model)bcmodels[bcmodels.Length - 1];


                /* Uncomment the following code to write a model XML out to file */
                if (pkg.defaultName.value == "Commitments")
                {
                    System.IO.StreamWriter xmlWriter = new StreamWriter("model.xml");
                    xmlWriter.Write(latestModel.model1.value);
                    xmlWriter.Close();

                    // Write out the database tables and fields used in the current model
                    //extractAndWriteTablesAndFields(ref writer, pkg.defaultName.value, latestModel.model1.value);
                }
            }
            writer.Close();
            return success;
        }

        static void extractAndWriteTablesAndFields(ref System.IO.StreamWriter writer, string modelName, string model)
        {
            XmlDocument modelDoc = new XmlDocument();
            modelDoc.LoadXml(model);
            XmlNodeList querySubjects = modelDoc.GetElementsByTagName("querySubject");
            
            foreach (XmlElement querySubject in querySubjects)
            {
                XmlDocument querySubjectDoc = new XmlDocument();
                querySubjectDoc.LoadXml("<?xml version=\"1.0\" encoding=\"UTF-8\" ?><root>" + querySubject.InnerXml + "</root>");
                XmlNodeList queryItems = querySubjectDoc.GetElementsByTagName("queryItem");
                
                // Grab the definition type (either dbQuery or modelQuery)
                XmlNodeList definitions = querySubjectDoc.GetElementsByTagName("definition");
                string definitionType = definitions[0].ChildNodes[0].Name;

                if (definitionType == "modelQuery")
                {
                    Console.WriteLine("modelQuery");
                }
                else if (definitionType == "dbQuery")
                {
                    string tableName = "";
                    if (querySubject.ChildNodes[0].Name == "name")
                    {
                        tableName = querySubject.ChildNodes[0].InnerText;
                    }

                    foreach (XmlElement queryItem in queryItems)
                    {
                        foreach (XmlElement childNode in queryItem.ChildNodes)
                        {
                            if (childNode.Name == "externalName")
                            {
                                writer.WriteLine(modelName + "|" + tableName + "|" + childNode.InnerText);
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        private bool turnOffMyFolderContentDuration(int[] cellCount)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            string camid = "//account"; //this is the search path for all user accounts
            //string camid = "CAMID(\"ADS:u:cn=miller\\, barret,ou=new,ou=groups,ou=general,ou=tyson team members\")";
            string userSearchPaths = "";
            // We will display My Folders and My Pages for all users in namespace
            propEnum[] props = new propEnum[]{propEnum.searchPath, 
					propEnum.objectClass,propEnum.defaultName,propEnum.portalPages, 
                    propEnum.ancestors, propEnum.owner};

            // Same query options used for all calls
            queryOptions qo = new queryOptions();
            // Create sort object
            sort[] accountSort = new sort[] { new sort() };
            accountSort[0].order = orderEnum.ascending;
            accountSort[0].propName = propEnum.defaultName;

            //query for all accounts
            searchPathMultipleObject spMulti = new searchPathMultipleObject();
            spMulti.Value = camid;
            baseClass[] bc = cCMS.query(spMulti, props, accountSort, qo);

            if (bc != null && bc.Length > 0)
            {
                // Set different properties to grab for the reports, queries, or report views
                props = new propEnum[]{propEnum.searchPath, propEnum.defaultName,
                                       propEnum.owner, propEnum.storeID, propEnum.connectionString, propEnum.creationTime,
                                       propEnum.metadataModelPackage, propEnum.ancestors, propEnum.disabled, propEnum.user,
                                       propEnum.retentions };
                // Declare properties to retrieve for package object internal to report object
                refProp packageProps = new refProp();
                packageProps.refPropName = propEnum.metadataModelPackage;
                packageProps.properties = new propEnum[] { propEnum.searchPath, propEnum.storeID,
                                                       propEnum.defaultName, propEnum.disabled, 
                                                       propEnum.ancestors };
                // Properties used to get base class information if the object is a report view
                refProp reportProps = new refProp();
                reportProps.refPropName = propEnum.@base;
                reportProps.properties = new propEnum[] { propEnum.metadataModelPackage, propEnum.storeID, 
                                                      propEnum.searchPath, propEnum.disabled };
                
                qo.refProps = new refProp[] { packageProps, reportProps };

                retentionRuleArrayProp retentionRules = new retentionRuleArrayProp();
                retentionRule[] outputRule = new retentionRule[1];
                outputRule[0] = new retentionRule();
                outputRule[0].objectClass = classEnum.reportVersion;
                outputRule[0].prop = propEnum.creationTime;
                outputRule[0].maxDuration = null;
                //outputRule[0].maxObjects = "1";
                List<baseClass> turnOffDurationList = new List<baseClass>();

                for (int i = 0; i < bc.Length; i++)
                {
                    //Query the Content Store for all objects in My Folders for user bc[i]
                    userSearchPaths = bc[i].searchPath.value + "/folder[@name='My Folders']//report"; /*+
                                      bc[i].searchPath.value + "/folder[@name='My Folders']//query |" +
                                      bc[i].searchPath.value + "/folder[@name='My Folders']//reportView";*/

                    spMulti.Value = userSearchPaths;
                    baseClass[] contents = cCMS.query(spMulti, props, new sort[] { }, qo);
                    if (contents != null && contents.Length > 0)
                    {
                        //Disable Duration all objects in My Folders for user bc[i]
                        for (int j = 0; j < contents.Length; j++)
                        {
                            /*
                             * Check whether object is a more specific type (e.g. query or report)
                             */
                            if (contents[j] is report)
                            {
                                cognosdotnet_2_0.report report = (cognosdotnet_2_0.report)contents[j];
                                foreach (retentionRule rule in report.retentions.value)
                                {
                                    // If the rule is for the type 'reportVersion' and has the duration set
                                    if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                                    {
                                        // Output the name
                                        oSheet.Cells[cellCount[0], cellCount[1]] = report.defaultName.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        //Output the path
                                        oSheet.Cells[cellCount[0], cellCount[1]] = report.searchPath.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        oSheet.Cells[cellCount[0], cellCount[1]] = bc[i].defaultName.value;
                                        //Reset the column count, and increment the row count
                                        cellCount[1] = 1;
                                        cellCount[0]++;
                                        report.retentions = retentionRules;
                                        turnOffDurationList.Add(contents[j]);
                                    }
                                }
                                
                            }
                            else if (contents[j] is query)
                            {
                                cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)contents[j];
                                foreach (retentionRule rule in query.retentions.value)
                                {
                                    // If the rule is for the type 'reportVersion' and has the duration set
                                    if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                                    {
                                        // Output the name
                                        oSheet.Cells[cellCount[0], cellCount[1]] = query.defaultName.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        //Output the path
                                        oSheet.Cells[cellCount[0], cellCount[1]] = query.searchPath.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        oSheet.Cells[cellCount[0], cellCount[1]] = bc[i].defaultName.value;
                                        //Reset the column count, and increment the row count
                                        cellCount[1] = 1;
                                        cellCount[0]++;
                                        query.retentions = retentionRules;
                                        turnOffDurationList.Add(contents[j]);
                                    }
                                }
                            }
                            else if (contents[j] is reportView)
                            {
                                cognosdotnet_2_0.reportView reportView = (cognosdotnet_2_0.reportView)contents[j];
                                foreach (retentionRule rule in reportView.retentions.value)
                                {
                                    // If the rule is for the type 'reportVersion' and has the duration set
                                    if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                                    {
                                        // Output the name
                                        oSheet.Cells[cellCount[0], cellCount[1]] = reportView.defaultName.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        //Output the path
                                        oSheet.Cells[cellCount[0], cellCount[1]] = reportView.searchPath.value;
                                        //Increment the column count
                                        cellCount[1]++;
                                        oSheet.Cells[cellCount[0], cellCount[1]] = bc[i].defaultName.value;
                                        //Reset the column count, and increment the row count
                                        cellCount[1] = 1;
                                        cellCount[0]++;
                                        reportView.retentions = retentionRules;
                                        turnOffDurationList.Add(contents[j]);
                                    }
                                }
                            }
                        }
                        if (turnOffDurationList.Count > 0)
                        {
                            cCMS.update(turnOffDurationList.ToArray(), new updateOptions());
                        }
                    }
                }
            }
            return true;
        }

        private bool turnOffReportDuration(int[] cellCount)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            if (cCMS == null)
            {
                return false;
            }

            // Declare query properties array for report
            propEnum[] reportProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName, 
                                                      propEnum.retentions, propEnum.owner };

            // Properties used to get account class information for the owner
            refProp ownerProps = new refProp();
            ownerProps.refPropName = propEnum.owner;
            ownerProps.properties = new propEnum[] { propEnum.searchPath, propEnum.defaultName };

            // Declare sort properties for reports and users
            // reports
            sort[] reportSort = new sort[] { new sort() };
            reportSort[0].order = orderEnum.ascending;
            reportSort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps = new refProp[] { ownerProps };

            // Declare search path for reports and for a single user, based on CAMID
            searchPathMultipleObject objectsPath = new searchPathMultipleObject();
            searchPathMultipleObject userPath = new searchPathMultipleObject();
            //Set search paths to get reports. Userpath must be set 
            //separately for each individual based on CAMID
            objectsPath.Value = "//report";
            //objectsPath.Value = "/content/package[@name='National Accounts']/report[@name='(0226) NA Ebit by Customer/Minor Line - Combined']";
            // Run query to get all reports. Users will be queried as part of this
            // process, one for each report. 
            baseClass[] bc = cCMS.query(objectsPath, reportProps, reportSort, qo);

            retentionRuleArrayProp retentionRules = new retentionRuleArrayProp();
            retentionRule[] outputRule = new retentionRule[1];
            outputRule[0] = new retentionRule();
            outputRule[0].objectClass = classEnum.reportVersion;
            outputRule[0].prop = propEnum.creationTime;
            outputRule[0].maxDuration = null;
            //outputRule[0].maxObjects = "1";
            List<baseClass> turnOffDurationList = new List<baseClass>();

            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
                    retentionRules.value = outputRule;
                    // Cast base class object to more specific report object for access to more
                    // properties
                    if (report_item is report)
                    {
                        cognosdotnet_2_0.report report = (cognosdotnet_2_0.report)report_item;
                        foreach (retentionRule rule in report.retentions.value)
                        {
                            // If the rule is for the type 'reportVersion' and has the duration set
                            if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                            {
                                // Output the name
                                oSheet.Cells[cellCount[0], cellCount[1]] = report.defaultName.value;
                                //Increment the column count
                                cellCount[1]++;
                                //Output the path
                                oSheet.Cells[cellCount[0], cellCount[1]] = report.searchPath.value;
                                //Increment the column count
                                cellCount[1]++;
                                if (report.owner.value == null)
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = "unknown";
                                }
                                else
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = report.owner.value[0].defaultName.value;
                                }
                                //Reset the column count, and increment the row count
                                cellCount[1] = 1;
                                cellCount[0]++;
                                report.retentions = retentionRules;
                                turnOffDurationList.Add(report_item);
                            }
                        }
                    }
                }
                if (turnOffDurationList.Count > 0)
                {
                    cCMS.update(turnOffDurationList.ToArray(), new updateOptions());
                }
            }
            return true;
        }

        private bool turnOffQueryDuration(int[] cellCount)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            if (cCMS == null)
            {
                return false;
            }

            // Declare query properties array for report
            propEnum[] reportProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName, 
                                                      propEnum.retentions, propEnum.owner };

            // Properties used to get account class information for the owner
            refProp ownerProps = new refProp();
            ownerProps.refPropName = propEnum.owner;
            ownerProps.properties = new propEnum[] { propEnum.searchPath, propEnum.defaultName };

            // Declare sort properties for reports and users
            //reports
            sort[] reportSort = new sort[] { new sort() };
            reportSort[0].order = orderEnum.ascending;
            reportSort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps = new refProp[] { ownerProps };

            // Declare search path for reports and for a single user, based on CAMID
            searchPathMultipleObject objectsPath = new searchPathMultipleObject();
            searchPathMultipleObject userPath = new searchPathMultipleObject();
            //Set search paths to get reports. Userpath must be set 
            //separately for each individual based on CAMID
            objectsPath.Value = "//query";
            //objectsPath.Value = "/content/package[@name='National Accounts']/report[@name='(0226) NA Ebit by Customer/Minor Line - Combined']";
            // Run query to get all reports. Users will be queried as part of this
            // process, one for each report. 
            baseClass[] bc = cCMS.query(objectsPath, reportProps, reportSort, qo);

            retentionRuleArrayProp retentionRules = new retentionRuleArrayProp();
            retentionRule[] outputRule = new retentionRule[1];
            outputRule[0] = new retentionRule();
            outputRule[0].objectClass = classEnum.reportVersion;
            outputRule[0].prop = propEnum.creationTime;
            outputRule[0].maxDuration = null;
            //outputRule[0].maxObjects = "1";
            List<baseClass> turnOffDurationList = new List<baseClass>();

            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
                    retentionRules.value = outputRule;
                    // Cast base class object to more specific report object for access to more
                    // properties
                    if (report_item is query)
                    {
                        cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)report_item;
                        foreach (retentionRule rule in query.retentions.value)
                        {
                            // If the rule is for the type 'reportVersion' and has the duration set
                            if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                            {
                                // Output the name
                                oSheet.Cells[cellCount[0], cellCount[1]] = query.defaultName.value;
                                //Increment the column count
                                cellCount[1]++;
                                //Output the path
                                oSheet.Cells[cellCount[0], cellCount[1]] = query.searchPath.value;
                                //Increment the column count
                                cellCount[1]++;
                                if (query.owner.value == null)
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = "unknown";
                                }
                                else
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = query.owner.value[0].defaultName.value;
                                }
                                //Reset the column count, and increment the row count
                                cellCount[1] = 1;
                                cellCount[0]++;
                                query.retentions = retentionRules;
                                turnOffDurationList.Add(report_item);
                            }
                        }
                    }
                }
                if (turnOffDurationList.Count > 0)
                {
                    cCMS.update(turnOffDurationList.ToArray(), new updateOptions());
                }
            }
            return true;
        }

        private bool turnOffReportViewDuration(int[] cellCount)
        {
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;

            // Create Excel Application
            oXL.Visible = true;

            //Get a new workbook.
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;

            if (cCMS == null)
            {
                return false;
            }

            // Declare query properties array for report
            propEnum[] reportProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName, 
                                                      propEnum.retentions, propEnum.owner };

            // Properties used to get account class information for the owner
            refProp ownerProps = new refProp();
            ownerProps.refPropName = propEnum.owner;
            ownerProps.properties = new propEnum[] { propEnum.searchPath, propEnum.defaultName };

            // Declare sort properties for reports and users
            //reports
            sort[] reportSort = new sort[] { new sort() };
            reportSort[0].order = orderEnum.ascending;
            reportSort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps = new refProp[] { ownerProps };

            // Declare search path for reports and for a single user, based on CAMID
            searchPathMultipleObject objectsPath = new searchPathMultipleObject();
            searchPathMultipleObject userPath = new searchPathMultipleObject();
            //Set search paths to get reports. Userpath must be set 
            //separately for each individual based on CAMID
            objectsPath.Value = "//reportView";
            //objectsPath.Value = "/content/package[@name='National Accounts']/report[@name='(0226) NA Ebit by Customer/Minor Line - Combined']";
            //objectsPath.Value = "CAMID(\"ADS:u:cn=miller\\, barret,ou=new,ou=groups,ou=general,ou=tyson team members\")/folder[@name='My Folders']/reportView[@name='Report View of (0224) Combined Sales']";
            // Run query to get all reports. Users will be queried as part of this
            // process, one for each report. 
            baseClass[] bc = cCMS.query(objectsPath, reportProps, reportSort, qo);

            retentionRuleArrayProp retentionRules = new retentionRuleArrayProp();
            retentionRule[] outputRule = new retentionRule[1];
            outputRule[0] = new retentionRule();
            outputRule[0].objectClass = classEnum.reportVersion;
            outputRule[0].prop = propEnum.creationTime;
            outputRule[0].maxDuration = null;
            //outputRule[0].maxObjects = "1";
            retentionRules.value = outputRule;
            List<baseClass> turnOffDurationList = new List<baseClass>();

            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    if (report_item is reportView)
                    {
                        cognosdotnet_2_0.reportView reportv = (cognosdotnet_2_0.reportView)report_item;
                        foreach (retentionRule rule in reportv.retentions.value)
                        {
                            // If the rule is for the type 'reportVersion' and has the duration set
                            if ((rule.objectClass == classEnum.reportVersion) && (rule.maxDuration != null))
                            {
                                // Output the name
                                oSheet.Cells[cellCount[0], cellCount[1]] = reportv.defaultName.value;
                                //Increment the column count
                                cellCount[1]++;
                                //Output the path
                                oSheet.Cells[cellCount[0], cellCount[1]] = reportv.searchPath.value;
                                //Increment the column count
                                cellCount[1]++;
                                if (reportv.owner.value == null)
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = "unknown";
                                }
                                else
                                {
                                    oSheet.Cells[cellCount[0], cellCount[1]] = reportv.owner.value[0].defaultName.value;
                                }
                                //Reset the column count, and increment the row count
                                cellCount[1] = 1;
                                cellCount[0]++;
                                reportv.retentions = retentionRules;
                                turnOffDurationList.Add(report_item);
                            }
                        }    
                    }
                }
                if (turnOffDurationList.Count > 0)
                {
                    cCMS.update(turnOffDurationList.ToArray(), new updateOptions());
                }
            }
            return true;
        }

        public List<CogObject> getGroupsAndRoles()
        {
            propEnum[] props = new propEnum[] { propEnum.defaultName, propEnum.searchPath, propEnum.type, propEnum.members, 
                                                propEnum.objectClass };

            // Properties used to get account class information for the members
            refProp memberProps = new refProp();
            memberProps.refPropName = propEnum.members;
            memberProps.properties = new propEnum[] { propEnum.searchPath, propEnum.defaultName, propEnum.userName, 
                                                      propEnum.objectClass };

            baseClass[] securityObjects = new baseClass[]{};
            searchPathMultipleObject spMulti = new searchPathMultipleObject();
            account[] targetAccount = null;
            List<CogObject> cogObjects = new List<CogObject>();

            queryOptions qo = new queryOptions();

            qo.refProps = new refProp[] { memberProps };

            // Set search path to pull back all groups and roles in Cognos
            spMulti.Value = "CAMID(\"ADS:f:ou=enterprise reporting,ou=applications,ou=tyson groups\")//group | //group | //role"; 
            Console.WriteLine(spMulti.Value);

            // Query for all groups and roles
            try
            {
                securityObjects = cCMS.query(spMulti, props, new sort[] { }, qo);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            if (securityObjects.Length <= 0)
            {
                Console.WriteLine("No roles or groups were found.");
            }

            // Loop through groups and roles and get members for each
            foreach (baseClass securityObject in securityObjects)
            {
                baseClass[] members = new baseClass[] { };
                searchPathMultipleObject membersPath = new searchPathMultipleObject();
                membersPath.Value = "expandMembers(" + securityObject.searchPath.value + ")";

                // Attempt to get members for the group or role
                try
                {
                    members = cCMS.query(membersPath, props, new sort[] { }, new queryOptions());
                    
                    // Loop through members of the group
                    foreach (account account in members)
                    {
                        CogObject user = new CogObject();
                        user.AddAttribute("user", account.defaultName.value);
                        
                        if (securityObject is role)
                        {
                            user.AddAttribute("categoryType", "role");
                        }
                        else if (securityObject is group)
                        {
                            user.AddAttribute("categoryType", "group");
                        }
                        else
                        {
                            user.AddAttribute("categoryType", "unknown");
                        }

                        user.AddAttribute("categoryName", securityObject.defaultName.value);
                        cogObjects.Add(user);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            return cogObjects;
        }
        
        /**
         * Method to take in a list of paths to cognos objects and disable them.
         * pwnsauce...
         */
        public void disableCogObjects()
        {
            System.IO.TextReader reader = new StreamReader("objectsToDisable.txt");

            StreamWriter errorWriter = new StreamWriter("objectsNotFoundC8.txt");
            StreamWriter successWriter = new StreamWriter("objectsFoundC8.txt");
            // Declare query properties array for report
            propEnum[] props = new propEnum[] { propEnum.defaultName, propEnum.disabled,
                                                propEnum.searchPath};

            // Declare sort properties for reports and users
            //reports
            sort[] sorts = new sort[] {  };

            // Object representing path to single report
            searchPathMultipleObject reportSearchPath = new searchPathMultipleObject();

            // Query options
            queryOptions qo = new queryOptions();

            // Read in first path
            string path = reader.ReadLine();
            List<baseClass> objectsToDisable = new List<baseClass>();
            
            // Loop through all the paths and generate a list of objects
            // to disable
            while (path != null)
            {
                reportSearchPath.Value = path;
                // Run query to get all reports. Users will be queried as part of this
                // process, one for each report. 

                try
                {
                    baseClass[] bc = cCMS.query(reportSearchPath, props, sorts, qo);
                    if (bc.Length > 0)
                    {
                        booleanProp bp = new booleanProp();
                        bp.value = true;
                        bc[0].disabled = bp;
                        // Add the object to our List of things to disable
                        objectsToDisable.Add(bc[0]);
                        successWriter.WriteLine(path);
                    }
                    else
                    {
                        errorWriter.WriteLine(path);
                    }
                }
                // The exception will simply say "The client did something wrong"
                // if the path line from the file is nothing resembling a path in the current 
                // cognos environment
                catch (Exception e)
                {
                    errorWriter.WriteLine(path);
                }
                path = reader.ReadLine();
            }
            reader.Close();
            errorWriter.Close();
            successWriter.Close();

            updateOptions updateOptions = new updateOptions();

            cCMS.update(objectsToDisable.ToArray(), updateOptions);
        }

        /**
         * Method to get all users' MyFolder contents with the same information that is collected in
         * the other methods, including the auditing info.
         */
        public List<CogObject> getUsersMyFolderContentsWithSubElements()
        {
            bool success8 = false;

            List<CogObject> cogs = new List<CogObject>();

            try
            {
                success8 = doViewUsersMyFolderContentsWithSubElements(cCMS, ref cogs, cogVersion8, cog8BaseReportPath);
            }
            catch (Exception ex)
            {
                //ExceptionPolicy.HandleException(ex, "Unhandled Exception Policy");
            }

            if (!success8)
            {
                return cogs;
            }
            else
            {
                //Could do something more useful here.
                return cogs;
            }
        }

        /* Does the bulk of the work to extract information on Users' MyFolder objects
         */
        private bool doViewUsersMyFolderContentsWithSubElements(contentManagerService1 cCMS, ref List<CogObject> cogs, string cogVersion, string baseReportPath)
        {
            string camid = "//account"; //this is the search path for all user accounts
            string userSearchPaths = "";
            // We will display My Folders and My Pages for all users in namespace
            propEnum[] props = new propEnum[]{propEnum.searchPath, 
					propEnum.objectClass,propEnum.defaultName,propEnum.portalPages, 
                    propEnum.ancestors, propEnum.owner};

            // Same query options used for all calls
            queryOptions qo = new queryOptions();
            // Create sort object
            sort[] accountSort = new sort[] { new sort() };
            accountSort[0].order = orderEnum.ascending;
            accountSort[0].propName = propEnum.defaultName;

            //query for all accounts
            searchPathMultipleObject spMulti = new searchPathMultipleObject();
            spMulti.Value = camid;
            baseClass[] bc = cCMS.query(spMulti, props, accountSort, qo);

            if (bc != null && bc.Length > 0)
            {
                // Set different properties to grab for the reports, queries, or report views
                props = new propEnum[]{propEnum.searchPath, propEnum.defaultName,
                                       propEnum.owner, propEnum.storeID, propEnum.connectionString, propEnum.creationTime,
                                       propEnum.metadataModelPackage, propEnum.ancestors, propEnum.disabled};
                // Declare properties to retrieve for package object internal to report object
                refProp packageProps = new refProp();
                packageProps.refPropName = propEnum.metadataModelPackage;
                packageProps.properties = new propEnum[] { propEnum.searchPath, propEnum.storeID,
                                                       propEnum.defaultName, propEnum.disabled, 
                                                       propEnum.ancestors };
                // Properties used to get base class information if the object is a report view
                refProp reportProps = new refProp();
                reportProps.refPropName = propEnum.@base;
                reportProps.properties = new propEnum[] { propEnum.metadataModelPackage, propEnum.storeID, 
                                                      propEnum.searchPath, propEnum.disabled };
                qo.refProps = new refProp[] { packageProps, reportProps };

                for (int i = 0; i < bc.Length; i++)
                {
                    //Query the Content Store for all objects in My Folders for user bc[i]
                    userSearchPaths = bc[i].searchPath.value + "/folder[@name='My Folders']//report |" +
                                      bc[i].searchPath.value + "/folder[@name='My Folders']//query |" +
                                      bc[i].searchPath.value + "/folder[@name='My Folders']//reportView";

                    spMulti.Value = userSearchPaths;
                    baseClass[] contents = cCMS.query(spMulti, props, new sort[] { }, qo);
                    if (contents != null && contents.Length > 0)
                    { 
                        //Display all objects in My Folders for user bc[i]
                        for (int j = 0; j < contents.Length; j++)
                        {
                            CogObject cog = new CogObject();
                            /*
                             * Check whether object is a more specific type (e.g. query or report)
                             */
                            if (contents[j] is report)
                            {
                                // Cast base class object to more specific report object for access to more
                                // properties
                                cognosdotnet_2_0.report report = (cognosdotnet_2_0.report)contents[j];
                                cog.AddAttribute("Type", "Report");
                                cog.AddAttribute("CreationTime", contents[j].creationTime.value.ToString());
                                // Make sure package or Package is not null
                                if (report.metadataModelPackage.value != null)
                                {
                                    cognosdotnet_2_0.package package = (cognosdotnet_2_0.package)report.metadataModelPackage.value[0];
                                    cog.AddAttribute("Package_Path", report.metadataModelPackage.value[0].searchPath.value);
                                    cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                                    cog.AddAttribute("Package_Disabled", package.disabled.value.ToString());
                                }
                                else
                                {
                                    cog.AddAttribute("Package_Path", "null");
                                    cog.AddAttribute("Package_Name", "null");
                                    cog.AddAttribute("Package_Disabled", "null");
                                }
                                cog.AddAttribute("Disabled", report.disabled.value.ToString());
                            }
                            else if (contents[j] is query)
                            {
                                // Cast base class object to more specific report object for access to more
                                // properties
                                cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)contents[j];
                                cog.AddAttribute("Type", "Query");
                                // Make sure package or Package is not null
                                if (query.metadataModelPackage.value != null)
                                {
                                    cognosdotnet_2_0.package package = (cognosdotnet_2_0.package)query.metadataModelPackage.value[0];
                                    cog.AddAttribute("Package_Path", query.metadataModelPackage.value[0].searchPath.value);
                                    cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                                    cog.AddAttribute("Package_Disabled", package.disabled.value.ToString());
                                }
                                else
                                {
                                    cog.AddAttribute("Package_Path", "null");
                                    cog.AddAttribute("Package_Name", "null");
                                    cog.AddAttribute("Package_Disabled", "null");
                                }
                                cog.AddAttribute("Disabled", query.disabled.value.ToString());
                            }
                            else if (contents[j] is reportView)
                            {
                                // Cast base class object to more specific report object for access to more
                                // properties
                                cognosdotnet_2_0.reportView reportView = (cognosdotnet_2_0.reportView)contents[j];
                                cog.AddAttribute("Type", "ReportView");
                                // Make sure package or Package is not null
                                if (reportView.packageBase != null)
                                {
                                    cog.AddAttribute("Package_Path", reportView.packageBase.value);
                                    cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                                }
                                else
                                {
                                    cog.AddAttribute("Package_Path", "null");
                                    cog.AddAttribute("Package_Name", "null");
                                }

                                if (reportView.@base.value != null)
                                {
                                    cog.AddAttribute("BaseID", reportView.@base.value[0].storeID.value.Value);
                                    cog.AddAttribute("BasePath", reportView.@base.value[0].searchPath.value);

                                    if (reportView.@base.value[0] is report)
                                    {
                                        report tempReport = (report)reportView.@base.value[0];
                                        if (tempReport.metadataModelPackage.value[0] != null)
                                        {
                                            cog.AddAttribute("BasePackage_Path", tempReport.metadataModelPackage.value[0].searchPath.value);
                                            cog.AddAttribute("BasePackage_Name", getPackageName(cog.getAttributeValue("BasePackage_Path")));
                                        }
                                    }
                                    else if (reportView.@base.value[0] is query)
                                    {
                                        query tempQuery = (query)reportView.@base.value[0];
                                        if (tempQuery.metadataModelPackage.value[0] != null)
                                        {
                                            cog.AddAttribute("BasePackage_Path", tempQuery.metadataModelPackage.value[0].searchPath.value);
                                            cog.AddAttribute("BasePackage_Name", getPackageName(cog.getAttributeValue("BasePackage_Path")));
                                        }
                                    }
                                    cog.AddAttribute("Disabled", reportView.disabled.value.ToString());

                                }
                            }


                            // Make sure owner is not null
                            if (contents[j].owner.value != null)
                            {
                                /* We will get the author based on who's folder the report is in. This may
                                * not be strictly true. Uncomment the following line to get the old method. 
                                * Also comment out or remove the line farther below that sets the Author_Name 
                                * attribute*/
                                //getUserAccount(cCMS, contents[j].owner.value[0].searchPath.value, ref cog);
                                cog.AddAttribute("Author_CAMID", contents[j].owner.value[0].searchPath.value);
                            }
                            else
                            {
                                cog.AddAttribute("Author_CAMID", "Unknown");
                            }

                            cog.AddAttribute("Author_Name", bc[i].defaultName.value);
                            cog.AddAttribute("Version", cogVersion);
                            cog.AddAttribute("Name", contents[j].defaultName.value);
                            cog.AddAttribute("Path", contents[j].searchPath.value);
                            cog.AddAttribute("URL", baseReportPath + contents[j].searchPath.value);
                            cog.AddAttribute("MyFolder", "TRUE");
                            getSubElements(cCMS, contents[j].searchPath.value, ref cog);
                            
                            /* This gets some limited auditing information. Most of this can be found 
                             * more easily in the Raw Usage report of the Business Intelligence Dashboard
                             * package on perform-dev.tyson.com. Uncommenting the following line will
                             * cause a dramatic increase in runtime for this program
                             */
                            //getAuditInfo(ref cog);
                            cogs.Add(cog);
                        }
                    }
                }
            }
            else
            {
                cogs = null;
            }
            return true;
        }

        /*
         * Methods to return the list of reports with sub-elements from the Cognos 8
         * environment. Uses the API provided by the cognosdotnet dll
         * which in turn calls a web service which in turn queries the 
         * content store. Getting what you want using this method requires
         * understanding the search_paths for the Cognos SDK. Check the proven
         * practices and the SDK developer guide for more info.
         */
        public List<CogObject> getCog8ReportsWithSubElements()
        {
            bool success8 = false;

            List<CogObject> cogReports = new List<CogObject>();

            try
            {
                success8 = doViewReportsWithSubElements(cCMS, ref cogReports, cogVersion8, cog8BaseReportPath);
            }
            catch (Exception ex)
            {
                //ExceptionPolicy.HandleException(ex, "Unhandled Exception Policy");
            }

            if (!success8)
            {
                return cogReports;
            }
            else
            {
                //Could do something more useful here.
                return cogReports;
            }
        }

        private bool doViewReportsWithSubElements(contentManagerService1 cCMS, ref List<CogObject> reports, string cogVersion, string baseReportPath)
        {
            if (cCMS == null)
            {
                reports = null;
                return false;
            }

            // Declare query properties array for report
            propEnum[] reportProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString, propEnum.creationTime,
                                                propEnum.metadataModelPackage, propEnum.ancestors, propEnum.disabled};
            
            // Declare properties to retrieve for package object internal to report object
            refProp packageProps = new refProp();
            packageProps.refPropName = propEnum.metadataModelPackage;
            packageProps.properties = new propEnum[] { propEnum.searchPath, propEnum.storeID,
                                                       propEnum.defaultName, propEnum.disabled, 
                                                       propEnum.ancestors };

            // Declare sort properties for reports and users
            //reports
            sort[] reportSort = new sort[] { new sort() };
            reportSort[0].order = orderEnum.ascending;
            reportSort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps= new refProp[] { packageProps };

            // Declare search path for reports and for a single user, based on CAMID
            searchPathMultipleObject reportsPath = new searchPathMultipleObject();
            searchPathMultipleObject userPath = new searchPathMultipleObject();
            //Set search paths to get reports. Userpath must be set 
            //separately for each individual based on CAMID
            reportsPath.Value = "/content//report";


            // Run query to get all reports. Users will be queried as part of this
            // process, one for each report. 
            baseClass[] bc = cCMS.query(reportsPath, reportProps, reportSort, qo);
            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    
                    cognosdotnet_2_0.report report = (cognosdotnet_2_0.report)report_item;
                    CogObject rpt = new CogObject();
                    rpt.ID = report_item.storeID.value.Value;
                    rpt.AddAttribute("ID", rpt.ID);
                    rpt.AddAttribute("Name", report_item.defaultName.value);
                    rpt.AddAttribute("CreationTime", report_item.creationTime.value.ToString());
                    rpt.AddAttribute("Type", "Report");
                    rpt.AddAttribute("Disabled", report.disabled.value.ToString());

                    // Make sure package or model is not null
                    if (report.metadataModelPackage.value!= null)
                    {
                        cognosdotnet_2_0.package package = (cognosdotnet_2_0.package)report.metadataModelPackage.value[0];
                        rpt.AddAttribute("Package_Path", report.metadataModelPackage.value[0].searchPath.value);
                        rpt.AddAttribute("Package_Name", getPackageName(rpt.getAttributeValue("Package_Path")));
                        rpt.AddAttribute("Package_Disabled", package.disabled.value.ToString());
                    }
                    else
                    {
                        rpt.AddAttribute("Package_Path", "null");
                        rpt.AddAttribute("Package_Name", "null");
                        rpt.AddAttribute("Package_Disabled", "null");
                    }
                    // Make sure owner is not null
                    if (report_item.owner.value != null)
                    {
                        getUserAccount(cCMS, report_item.owner.value[0].searchPath.value, ref rpt);
                        rpt.AddAttribute("Author_CAMID", report_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        rpt.AddAttribute("Author_CAMID", "Unknown");
                    }
                    String version = cogVersion;
                    rpt.AddAttribute("Version", cogVersion);
                    rpt.AddAttribute("Path", report_item.searchPath.value);
                    rpt.AddAttribute("URL", baseReportPath + report_item.searchPath.value);
                    getSubElements(cCMS, report_item.searchPath.value, ref rpt);
                    /* This gets some limited auditing information. Most of this can be found 
                     * more easily in the Raw Usage report of the Business Intelligence Dashboard
                     * package on perform-dev.tyson.com. Uncommenting the following line will
                     * cause a dramatic increase in runtime for this program
                     */
                    //getAuditInfo(ref cog);
                    reports.Add(rpt);
                }
            }
            else
            {
                reports = null;
            }
            return true;
        }

        /**
         * Methods to return a list of all queries with sub-elements from the Cognos 8 env
         */
        public List<CogObject> getCog8QueriesWithSubElements()
        {
            bool success8 = false;

            List<CogObject> cogQueries = new List<CogObject>();

            try
            {
                success8 = doViewQueriesWithSubElements(cCMS, ref cogQueries, cogVersion8, cog8BaseReportPath);
            }
            catch (Exception ex)
            {
                //ExceptionPolicy.HandleException(ex, "Unhandled Exception Policy");
            }

            if (!success8)
            {
                return cogQueries;
            }
            else
            {
                //Could do something more useful here.
                return cogQueries;
            }
        }

        private bool doViewQueriesWithSubElements(contentManagerService1 cCMS, ref List<CogObject> queries, string cogVersion, string baseQueryPath)
        {
            if (cCMS == null)
            {
                queries = null;
                return false;
            }

            //queries
            propEnum[] queryProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString, propEnum.creationTime,
                                                propEnum.metadataModelPackage, propEnum.ancestors, propEnum.disabled};

            // Declare properties to retrieve for package object internal to report object
            refProp packageProps = new refProp();
            packageProps.refPropName = propEnum.metadataModelPackage;
            packageProps.properties = new propEnum[] { propEnum.searchPath, propEnum.storeID,
                                                       propEnum.defaultName, propEnum.disabled, 
                                                       propEnum.ancestors };

            //queries
            sort[] querySort = new sort[] { new sort() };
            querySort[0].order = orderEnum.ascending;
            querySort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps = new refProp[] { packageProps };

            searchPathMultipleObject userPath = new searchPathMultipleObject();
            searchPathMultipleObject queriesPath = new searchPathMultipleObject();
            queriesPath.Value = "/content//query";

            // Make call to get all queries. Get each author for each query during this process
            // by making a separate call, based on CAMID; the same as above when pulling the reports.
            baseClass[] bcQueries = cCMS.query(queriesPath, queryProps, querySort, qo);
            if (bcQueries.Length > 0)
            {
                foreach (baseClass query_item in bcQueries)
                {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)query_item;
                    CogObject qry = new CogObject();
                    qry.ID = query_item.storeID.value.Value;
                    qry.AddAttribute("ID", qry.ID);
                    qry.AddAttribute("Name", query_item.defaultName.value);
                    qry.AddAttribute("CreationTime", query_item.creationTime.value.ToString());
                    qry.AddAttribute("Type", "Query");
                    qry.AddAttribute("Disabled", query.disabled.value.ToString());

                    // Make sure package or package is not null
                    if (query.metadataModelPackage.value != null)
                    {
                        cognosdotnet_2_0.package package = (cognosdotnet_2_0.package)query.metadataModelPackage.value[0];
                        qry.AddAttribute("Package_Path", query.metadataModelPackage.value[0].searchPath.value);
                        qry.AddAttribute("Package_Name", getPackageName(qry.getAttributeValue("Package_Path")));
                        qry.AddAttribute("Package_Disabled", package.disabled.value.ToString());
                    }
                    else
                    {
                        qry.AddAttribute("Package_Path", "null");
                        qry.AddAttribute("Package_Name", "null");
                        qry.AddAttribute("Package_Disabled", "null");
                    }
                    // Make sure owner is not null
                    if (query_item.owner.value != null)
                    {
                        getUserAccount(cCMS, query_item.owner.value[0].searchPath.value, ref qry);
                        qry.AddAttribute("Author_CAMID", query_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        qry.AddAttribute("Author_CAMID", "Unknown");
                    }
                    qry.AddAttribute("Version", cogVersion);
                    qry.AddAttribute("Path", query_item.searchPath.value);
                    qry.AddAttribute("URL", baseQueryPath + query_item.searchPath.value);
                    getSubElements(cCMS, query_item.searchPath.value, ref qry);
                    /* This gets some limited auditing information. Most of this can be found 
                     * more easily in the Raw Usage report of the Business Intelligence Dashboard
                     * package on perform-dev.tyson.com. Uncommenting the following line will
                     * cause a dramatic increase in runtime for this program
                     */
                    //getAuditInfo(ref cog);
                    queries.Add(qry);
                }
            }
            else
            {
                queries = null;
            }
            return true;
        }

        /**
         *
         *  Methods to return ReportViews with sub-elements from the Cognos 8 env 
         */
        public List<CogObject> getCog8ReportViewsWithSubElements()
        {
            bool success8 = false;

            List<CogObject> cogReportViews = new List<CogObject>();

            try
            {
                success8 = doViewReportViewsWithSubElements(cCMS, ref cogReportViews, cogVersion8, cog8BaseReportPath);
            }
            catch (Exception ex)
            {
                //ExceptionPolicy.HandleException(ex, "Unhandled Exception Policy");
            }

            if (!success8)
            {
                return cogReportViews;
            }
            else
            {
                //Could do something more useful here.
                return cogReportViews;
            }
        }

        private bool doViewReportViewsWithSubElements(contentManagerService1 cCMS, ref List<CogObject> reportViews, string cogVersion, string baseRVPath)
        {
            if (cCMS == null)
            {
                reportViews = null;
                return false;
            }

            //queries
            propEnum[] rvProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString, propEnum.creationTime,
                                                propEnum.metadataModelPackage, propEnum.packageBase, propEnum.@base,
                                                propEnum.disabled};

            // Declare properties to retrieve for package object internal to report object
            refProp packageProps = new refProp();
            refProp reportProps = new refProp();
            reportProps.refPropName = propEnum.@base;
            reportProps.properties = new propEnum[] { propEnum.metadataModelPackage, propEnum.storeID, 
                                                      propEnum.searchPath, propEnum.disabled };

            //queries
            sort[] rvSort = new sort[] { new sort() };
            rvSort[0].order = orderEnum.ascending;
            rvSort[0].propName = propEnum.defaultName;

            // Set up query options for the call. Adding the packageProps
            // will cause all requested subproperties to be retrieved for
            // the properties listed that refer to other objects.
            queryOptions qo = new queryOptions();
            qo.refProps = new refProp[] { reportProps };

            searchPathMultipleObject userPath = new searchPathMultipleObject();
            searchPathMultipleObject rvPath = new searchPathMultipleObject();
            rvPath.Value = "/content//reportView";

            // Make call to get all queries. Get each author for each query during this process
            // by making a separate call, based on CAMID; the same as above when pulling the reports.
            baseClass[] bcReportViews = cCMS.query(rvPath, rvProps, rvSort, qo);
            if (bcReportViews.Length > 0)
            {
                foreach (baseClass rv_item in bcReportViews)
                {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    cognosdotnet_2_0.reportView reportView = (cognosdotnet_2_0.reportView)rv_item;
                    CogObject rv = new CogObject();
                    rv.ID = rv_item.storeID.value.Value;
                    rv.AddAttribute("ID", rv.ID);
                    rv.AddAttribute("CreationTime", rv_item.creationTime.value.ToString());

                    if (reportView.@base.value != null)
                    {
                        rv.AddAttribute("BaseID", reportView.@base.value[0].storeID.value.Value);
                        rv.AddAttribute("BasePath", reportView.@base.value[0].searchPath.value);

                        if (reportView.@base.value[0] is report)
                        {
                            report tempReport = (report)reportView.@base.value[0];
                            if (tempReport.metadataModelPackage.value[0] != null)
                            {
                                rv.AddAttribute("BasePackage_Path", tempReport.metadataModelPackage.value[0].searchPath.value);
                                rv.AddAttribute("BasePackage_Name", getPackageName(rv.getAttributeValue("BasePackage_Path")));
                            }
                        }
                        else if (reportView.@base.value[0] is query)
                        {
                            query tempQuery = (query)reportView.@base.value[0];
                            if (tempQuery.metadataModelPackage.value[0] != null)
                            {
                                rv.AddAttribute("BasePackage_Path", tempQuery.metadataModelPackage.value[0].searchPath.value);
                                rv.AddAttribute("BasePackage_Name", getPackageName(rv.getAttributeValue("BasePackage_Path")));
                            }
                        }

                    }
                    rv.AddAttribute("Name", rv_item.defaultName.value);
                    rv.AddAttribute("Type", "ReportView");
                    rv.AddAttribute("Disabled", reportView.disabled.value.ToString());
                   
                    // Make sure package or Package is not null
                    if (reportView.packageBase != null)
                    {
                        rv.AddAttribute("Package_Path", reportView.packageBase.value);
                        rv.AddAttribute("Package_Name", getPackageName(rv.getAttributeValue("Package_Path")));
                    }
                    else
                    {
                        rv.AddAttribute("Package_Path", "null");
                        rv.AddAttribute("Package_Name", "null");
                        rv.AddAttribute("Package_Disabled", "null");
                    }
                    // Make sure owner is not null
                    if (rv_item.owner.value != null)
                    {
                        getUserAccount(cCMS, rv_item.owner.value[0].searchPath.value, ref rv);
                        rv.AddAttribute("Author_CAMID", rv_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        rv.AddAttribute("Author_CAMID", "Unknown");
                    }
                    rv.AddAttribute("Version", cogVersion);
                    rv.AddAttribute("Path", rv_item.searchPath.value);
                    rv.AddAttribute("URL", baseRVPath + rv_item.searchPath.value);
                    getSubElements(cCMS, rv_item.searchPath.value, ref rv);
                    /* This gets some limited auditing information. Most of this can be found 
                     * more easily in the Raw Usage report of the Business Intelligence Dashboard
                     * package on perform-dev.tyson.com. Uncommenting the following line will
                     * cause a dramatic increase in runtime for this program
                     */
                    //getAuditInfo(ref cog);
                    reportViews.Add(rv);
                }
            }
            else
            {
                reportViews = null;
            }
            return true;
        }

        
        public List<CogObject> getCog8ObjectsByPackage()
        {
            bool success8 = false;

            List<CogObject> cogPackages = new List<CogObject>();

            try
            {
                success8 = doViewObjectsByPackage(cCMS, ref cogPackages, cogVersion8, cog8BaseReportPath);
            }
            catch (Exception ex)
            {
                //ExceptionPolicy.HandleException(ex, "Unhandled Exception Policy");
            }

            if (!success8)
            {
                return cogPackages;
            }
            else
            {
                //Could do something more useful here.
                return cogPackages;
            }
        }
        
        private bool doViewObjectsByPackage(contentManagerService1 cCMS, ref List<CogObject> packages, string cogVersion, string basePackagePath)
        {
            if (cCMS == null)
            {
                packages = null;
                return false;
            }

            // Same query options used for all calls
            queryOptions qo = new queryOptions();

            //queries
            propEnum[] packageProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString,
                                                propEnum.metadataModelPackage, propEnum.policies, propEnum.ancestors};
            //queries
            sort[] pkgSort = new sort[] { new sort() };
            pkgSort[0].order = orderEnum.ascending;
            pkgSort[0].propName = propEnum.defaultName;

            searchPathMultipleObject packagesPath = new searchPathMultipleObject();
            packagesPath.Value = "/content//package";

            // Make call to get all queries. Get each author for each query during this process
            // by making a separate call, based on CAMID; the same as above when pulling the reports.
            baseClass[] bcPackages = cCMS.query(packagesPath, packageProps, pkgSort, qo);
            if (bcPackages.Length > 0)
            {
                 foreach (baseClass package_item in bcPackages)
                 {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    cognosdotnet_2_0.package package = (cognosdotnet_2_0.package)package_item;
                    CogObject pkg = new CogObject();
                    pkg.ID = package_item.storeID.value.Value;
                    pkg.AddAttribute("ID", pkg.ID);
                    pkg.AddAttribute("Name", package_item.defaultName.value);
                    pkg.AddAttribute("Type", "Package");
                    addSecurityPolicies(ref pkg, package_item);

                    if (package_item.owner.value != null)
                    {
                        getUserAccount(cCMS, package_item.owner.value[0].searchPath.value, ref pkg);
                        pkg.AddAttribute("Author_CAMID", package_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        pkg.AddAttribute("Author_CAMID", "Unknown");
                    }
                    pkg.AddAttribute("Version", cogVersion);
                    pkg.AddAttribute("URL", basePackagePath + package_item.searchPath.value);

                    /* Run another query to retrieve all reports for given package based on package path
                    */
                    List<CogObject> children = new List<CogObject>();
                    
                    getObjectsForPackage(cCMS, ref children, cogVersion, basePackagePath, package_item.searchPath.value +
                                         "//report | " + package_item.searchPath.value + "//query | " + 
                                         package_item.searchPath.value + "//reportView");
                    pkg.addSubElements(children);
                    packages.Add(pkg);
                }
            }
            else
            {
                packages = null;
            }
            return true;
        }


        private bool getObjectsForPackage(contentManagerService1 cCMS, ref List<CogObject> objects, string cogVersion, 
                                          string basePath, string pathToObjectsInPackage)
        {
            if (cCMS == null)
            {
                objects = null;
                return false;
            }
            // Declare query properties array for reports, queries and for users
            propEnum[] cogProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString, 
                                                propEnum.metadataModelPackage, propEnum.user,propEnum.actualExecutionTime,
                                                propEnum.policies};

            // Declare sort properties for reports
            sort[] cogSort = new sort[] { new sort() };
            cogSort[0].order = orderEnum.ascending;
            cogSort[0].propName = propEnum.defaultName;

            // Same query options used for all calls
            queryOptions qo = new queryOptions();
   
            searchPathMultipleObject objectsPath = new searchPathMultipleObject();

            // Declare search path for reports and for a single user, based on CAMID
            objectsPath.Value = pathToObjectsInPackage; 

            // Run query to get all objects in package. Users will be queried as part of this
            // process, one for each report. 
            baseClass[] bc = cCMS.query(objectsPath, cogProps, cogSort, qo);
            if (bc.Length > 0)
            {
                foreach (baseClass cog_item in bc)
                {
                    
                    CogObject cog = new CogObject();
                    cog.ID = cog_item.storeID.value.Value;
                    cog.Name = cog_item.defaultName.value;
                    cog.AddAttribute("ID", cog.ID);
                    cog.AddAttribute("Name", cog_item.defaultName.value);
                    cog.AddAttribute("Version", cogVersion);
                    cog.AddAttribute("URL", basePath + cog_item.searchPath.value);
                    addSecurityPolicies(ref cog, cog_item);

                    /* Get owner/author information for this object
                     */
                    if (cog_item.owner.value != null)
                    {
                        getUserAccount(cCMS, cog_item.owner.value[0].searchPath.value, ref cog);
                        cog.AddAttribute("Author_CAMID", cog_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        cog.AddAttribute("Author_CAMID", "Unknown");
                    }

                    /*
                     * Check whether object is a more specific type (e.g. query or report)
                     */
                    if (cog_item is report)
                    {
                        // Cast base class object to more specific report object for access to more
                        // properties
                        cognosdotnet_2_0.report report = (cognosdotnet_2_0.report)cog_item;
                        cog.AddAttribute("Type", "Report");
                        // Make sure package or Package is not null
                        if (report.metadataModelPackage.value != null)
                        {
                            cog.AddAttribute("Package_Path", report.metadataModelPackage.value[0].searchPath.value);
                            cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                        }
                        else
                        {
                            cog.AddAttribute("Package_Path", "null");
                            cog.AddAttribute("Package_Name", "null");
                        }
                    }
                    else if (cog_item is query)
                    {
                        // Cast base class object to more specific report object for access to more
                        // properties
                        cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)cog_item;
                        cog.AddAttribute("Type", "Query");
                        // Make sure package or Package is not null
                        if (query.metadataModelPackage.value != null)
                        {
                            cog.AddAttribute("Package_Path", query.metadataModelPackage.value[0].searchPath.value);
                            cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                        }
                        else
                        {
                            cog.AddAttribute("Package_Path", "null");
                            cog.AddAttribute("Package_Name", "null");
                        }
                    }
                    else if (cog_item is reportView)
                    {
                        // Cast base class object to more specific report object for access to more
                        // properties
                        cognosdotnet_2_0.reportView reportView = (cognosdotnet_2_0.reportView)cog_item;
                        cog.AddAttribute("Type", "ReportView");
                        // Make sure package or Package is not null
                        if (reportView.packageBase != null)
                        {
                            cog.AddAttribute("Package_Path", reportView.packageBase.value);
                            cog.AddAttribute("Package_Name", getPackageName(cog.getAttributeValue("Package_Path")));
                        }
                        else
                        {
                            cog.AddAttribute("Package_Path", "null");
                            cog.AddAttribute("Package_Name", "null");
                        }
                    }

                    getSubElements(cCMS, cog_item.searchPath.value, ref cog);

                    // Add the object to the list
                    objects.Add(cog);
                }
            }
            return true;
        }


        // Method to take a reference to a Cog object and make queries to the Cognos
        // content audit database to retrieve auditing information for the report, query,
        // or report_view based on the unique searchpath for the object.
        private void getAuditInfo(ref CogObject cog)
        {
            string timesRun3Months = "";
            string timesRun6Months = "";
            string timesRun1Year = "";
            string numUsers3Months = "";
            string numUsers6Months = "";
            string numUsers1Year = "";
            string lastUser = "";
            string lastTime = "";

            try
            {
                
                // Set up times-run query
                string timesRunQry = @"SELECT COUNT(DISTINCT(r.cogipf_localtimestamp)) AS TIMESRUN
                                       FROM content_audit.cogipf_runreport r
                                       WHERE 
                                       r.COGIPF_REPORTPATH = :reportPath
                                       AND
                                       r.COGIPF_LOCALTIMESTAMP >= SYSDATE - :timespan";

                Database db = DatabaseFactory.CreateDatabase("DBPTSN4");
                DbCommand dbCommand = db.GetSqlStringCommand(timesRunQry);
                db.AddInParameter(dbCommand, ":reportPath", DbType.String, cog.getAttributeValue("Path"));
                
                // Run for 3 months first.
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 90);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                       System.Decimal temp = (System.Decimal)dr["TIMESRUN"];
                       timesRun3Months = temp.ToString();
                    }
                }
                
                // Now run for 6 months
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 180);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.Decimal temp = (System.Decimal)dr["TIMESRUN"];
                        timesRun6Months = temp.ToString();
                    }
                }

                // Finally, run for a year
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 365);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.Decimal temp = (System.Decimal)dr["TIMESRUN"];
                        timesRun1Year = temp.ToString();
                    }
                }
                

                // Set up query for users run over timespan
                string usersRunQuery = @"SELECT COUNT(DISTINCT(u.cogipf_username)) AS DistinctUsers
                                        FROM content_audit.cogipf_runreport r, content_audit.cogipf_userlogon u
                                        WHERE 
                                        r.COGIPF_REPORTPATH = :reportPath
                                        AND
                                        r.COGIPF_LOCALTIMESTAMP >= SYSDATE - :timespan
                                        AND
                                        u.cogipf_sessionid = r.cogipf_sessionid";
                
                dbCommand = db.GetSqlStringCommand(usersRunQuery);
                db.AddInParameter(dbCommand, ":reportPath", DbType.String, cog.getAttributeValue("Path"));

                // Run query for 3 months
                
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 90);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.Decimal temp = (System.Decimal)dr["DistinctUsers"];
                        numUsers3Months = temp.ToString();
                    }
                }

                // Run query for 6 months
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 180);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.Decimal temp = (System.Decimal)dr["DistinctUsers"];
                        numUsers6Months = temp.ToString();
                    }
                }
                
                // Run query for 1 year
                db.AddInParameter(dbCommand, ":timespan", DbType.Int32, 365);
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.Decimal temp = (System.Decimal)dr["DistinctUsers"];
                        numUsers1Year = temp.ToString();
                    }
                }
        
                // Set up query for last user and time run
                string lastUserAndTimeQry = @"SELECT distinct(r.COGIPF_LOCALTIMESTAMP) AS LastTimeRun,
                                                 u.cogipf_username AS LastUserRun
                                            FROM content_audit.cogipf_runreport r, content_audit.cogipf_userlogon u,
                                                 (SELECT max(ir.COGIPF_LOCALTIMESTAMP) as maxdate
                                                  FROM content_audit.cogipf_runreport ir
                                                  WHERE ir.COGIPF_REPORTPATH = :reportPath) maxresults
                                            WHERE r.COGIPF_LOCALTIMESTAMP = maxresults.maxdate
                                            AND r.COGIPF_SESSIONID = u.COGIPF_SESSIONID
                                            AND u.COGIPF_USERNAME like '%'";

                dbCommand = db.GetSqlStringCommand(lastUserAndTimeQry);
                db.AddInParameter(dbCommand, ":reportPath", DbType.String, cog.getAttributeValue("Path"));
                
                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        System.DateTime temp = (System.DateTime)dr["LastTimeRun"];
                        lastTime = temp.ToString();
                        lastUser = (string)dr["LastUserRun"];
                    }
                }

                cog.AddAttribute("timesRun3Months", timesRun3Months);
                cog.AddAttribute("timesRun6Months", timesRun6Months);
                cog.AddAttribute("timesRun1Year", timesRun1Year);
                cog.AddAttribute("numUsers3Months", numUsers3Months);
                cog.AddAttribute("numUsers6Months", numUsers6Months);
                cog.AddAttribute("lastTime", lastTime);
                cog.AddAttribute("lastUser", lastUser);
                
                cog.AddAttribute("numUsers1Year", numUsers1Year);
            }
            catch (Exception ex)
            {
                
            }
        }

        private void getSubElements(contentManagerService1 cCMS, string objectSearchPath, ref CogObject cog)
        {
            searchPathMultipleObject subElementPath = new searchPathMultipleObject();
            subElementPath.Value = objectSearchPath + "/*";

            // Same query options used for all calls
            queryOptions qo = new queryOptions();

            //users
            propEnum[] subElementProps = new propEnum[] { propEnum.storeID, propEnum.searchPath, propEnum.defaultName,
                                                          propEnum.user, propEnum.status, propEnum.requestedExecutionTime,
                                                          propEnum.actualExecutionTime, propEnum.actualCompletionTime,
                                                          propEnum.eventID, propEnum.ownerEventID, propEnum.asOfTime,
                                                          propEnum.canBurst, propEnum.contact, propEnum.contactEMail,
                                                          propEnum.metadataModel, propEnum.metadataModelPackage, 
                                                          propEnum.options, propEnum.parameters, propEnum.runOptions,
                                                          propEnum.serverGroup, propEnum.specification, propEnum.active,
                                                          propEnum.credential, propEnum.dailyPeriod, propEnum.endDate,
                                                          propEnum.endType, propEnum.everyNPeriods, propEnum.startDate,
                                                          propEnum.taskID, propEnum.timeZoneID, propEnum.triggerName, propEnum.type,
                                                          propEnum.weeklyFriday, propEnum.weeklyMonday, propEnum.weeklySaturday,
                                                          propEnum.weeklySunday, propEnum.weeklyThursday, propEnum.weeklyTuesday,
                                                          propEnum.weeklyWednesday, propEnum.yearlyAbsoluteDay, propEnum.yearlyAbsoluteMonth,
                                                          propEnum.yearlyRelativeDay, propEnum.yearlyRelativeWeek, propEnum.ancestors, 
                                                          propEnum.creationTime, propEnum.modificationTime, propEnum.name, propEnum.usage,
                                                          propEnum.version};
            sort[] subElementSort = new sort[] { new sort() };
            subElementSort[0].order = orderEnum.ascending;
            subElementSort[0].propName = propEnum.defaultName;

            baseClass[] bc = cCMS.query(subElementPath, subElementProps, subElementSort, qo);
            if (bc.Length > 0)
            {
                foreach (baseClass item in bc)
                {
                    CogObject cogItem = new CogObject();

                    if (item is history)
                    {
                        cognosdotnet_2_0.history hist = (history)item;
                        addHistoryAttributes(cogItem, hist);
                        
                    }
                    else if (item is schedule)
                    {
                        cognosdotnet_2_0.schedule sched = (schedule)item;
                        cog.AddAttribute("ActiveSchedule", sched.active.value.ToString());
                        addScheduleAttributes(ref cogItem, sched);
                        
                    }
                    else if (item is reportVersion)
                    
                    {
                        cognosdotnet_2_0.reportVersion rver = (reportVersion)item;
                        addReportVersionAttributes(ref cogItem, rver);
                    }

                    cog.addSubElement(cogItem);
                }
            }
            
        }

        /* Method to query the content store for the object at the given path and populate the
         * referenced object with user name information of the owner or author
         */
        private void getUserAccount(contentManagerService1 cCMS, string searchPath, ref CogObject cog)
        {
            searchPathMultipleObject userPath = new searchPathMultipleObject();
            userPath.Value = searchPath;

            // Same query options used for all calls
            queryOptions qo = new queryOptions();
            
            //users
            propEnum[] userProps = new propEnum[] { propEnum.userName, propEnum.name, propEnum.user };
            sort[] userSort = new sort[] { new sort() };
            userSort[0].order = orderEnum.ascending;
            userSort[0].propName = propEnum.userName;

            /* Run another query to retrieve the account object based on the CAMID.
            UserID is pulled from the account object. CAMID's for users who no
            longer work here will return no account objects. Author set to ""
            in this case. 
            */
            baseClass[] bcUserAccount = cCMS.query(userPath, userProps, userSort, qo);
            if (bcUserAccount.Length > 0)
            {
                cognosdotnet_2_0.account account = (cognosdotnet_2_0.account)bcUserAccount[0];
                cog.AddAttribute("Author", account.userName.value);
                cog.AddAttribute("Author_Name", account.name.value[0].value);
            }
            else
            {
                string wholeName = getAuthorName(userPath.Value);
                cog.AddAttribute("Author_Name", wholeName);
            }
        }


        private bool getQueriesForPackage(contentManagerService1 cCMS, ref List<CogObject> queries, string cogVersion,
                                          string baseQueryPath, string pathToQueriesInPackage)
        {
            if (cCMS == null)
            {
                queries = null;
                return false;
            }

            // Same query options used for all calls
            queryOptions qo = new queryOptions();

            //queries
            propEnum[] queryProps = new propEnum[] { propEnum.searchPath, propEnum.defaultName,
                                                propEnum.owner, propEnum.storeID, propEnum.connectionString,
                                                propEnum.metadataModelPackage};
            //users
            propEnum[] userProps = new propEnum[] { propEnum.userName, propEnum.name, propEnum.user };

            //queries
            sort[] querySort = new sort[] { new sort() };
            querySort[0].order = orderEnum.ascending;
            querySort[0].propName = propEnum.defaultName;
            //users
            sort[] userSort = new sort[] { new sort() };
            userSort[0].order = orderEnum.ascending;
            userSort[0].propName = propEnum.userName;

            searchPathMultipleObject userPath = new searchPathMultipleObject();
            searchPathMultipleObject queriesPath = new searchPathMultipleObject();
            queriesPath.Value = pathToQueriesInPackage;

            // Make call to get all queries. Get each author for each query during this process
            // by making a separate call, based on CAMID; the same as above when pulling the reports.
            baseClass[] bcQueries = cCMS.query(queriesPath, queryProps, querySort, qo);
            if (bcQueries.Length > 0)
            {
                foreach (baseClass query_item in bcQueries)
                {
                    // Cast base class object to more specific report object for access to more
                    // properties
                    cognosdotnet_2_0.query query = (cognosdotnet_2_0.query)query_item;
                    CogObject qry = new CogObject();
                    qry.ID = query_item.storeID.value.Value;
                    qry.AddAttribute("ID", qry.ID);
                    qry.AddAttribute("Name", query_item.defaultName.value);
                    qry.AddAttribute("Type", "AdHoc");
                    qry.AddAttribute("Package_Path", query.metadataModelPackage.value[0].searchPath.value);
                    qry.AddAttribute("Package_Name", getPackageName(qry.getAttributeValue("Package_Path")));

                    if (query_item.owner.value != null)
                    {
                        getUserAccount(cCMS, query_item.owner.value[0].searchPath.value, ref qry);
                        qry.AddAttribute("Author_CAMID", query_item.owner.value[0].searchPath.value);
                    }
                    else
                    {
                        qry.AddAttribute("Author_CAMID", "Unknown");
                    }
                    qry.AddAttribute("Version", cogVersion);
                    qry.AddAttribute("URL", baseQueryPath + query_item.searchPath.value);
                    queries.Add(qry);
                }
            }
            else
            {
                queries = null;
            }
            return true;
        }

        private void specificUserLogon(bool guiMode, contentManagerService1 cCMS, string userName, string userPassword, string userNamespace)
        {

            // sn_dg_prm_sdk_method_contentManagerService_logon_start_0
            System.Text.StringBuilder credentialXML = new System.Text.StringBuilder("<credential>");
            credentialXML.AppendFormat("<namespace>{0}</namespace>", userNamespace);
            credentialXML.AppendFormat("<username>{0}</username>", userName);
            credentialXML.AppendFormat("<password>{0}</password>", userPassword);
            credentialXML.Append("</credential>");

            //The csharp toolkit encodes the credentials
            string encodedCredentials = credentialXML.ToString();
            xmlEncodedXML xmlEncodedCredentials = new xmlEncodedXML();
            xmlEncodedCredentials.Value = encodedCredentials;
            searchPathSingleObject[] emptyRoleSearchPathList = new searchPathSingleObject[0];
            cCMS.logon(xmlEncodedCredentials, null);
            // sn_dg_prm_sdk_method_contentManagerService_logon_end_0

        }

        private string getReportName(string rn)
        {
            string pattern = @".{14}(.*)'|""?\]";
            return System.Text.RegularExpressions.Regex.Replace(rn, pattern, "$1");
            //return rn;
        }

        private string getAuthorName(string camid)
        {
            return System.Text.RegularExpressions.Regex.Replace(camid, ".*cn=(.*)\\\\, (.*?),.*", "$1, $2");
        }

        private string getLastName(string name)
        {
            return System.Text.RegularExpressions.Regex.Replace(name, "(.*), .*", "$1");
        }

        private string getFirstName(string name)
        {
            return System.Text.RegularExpressions.Regex.Replace(name, ".*, (.*)", "$1");
        }

        private string getPackageName(string PackagePath)
        {
            return System.Text.RegularExpressions.Regex.Replace(PackagePath, ".*@name='(.*)']", "$1");
        }


        /**
         * Method to add attribute specific to a History object to our CogObject
         */
        private void addHistoryAttributes(CogObject cog, cognosdotnet_2_0.history hist)
        {
            cog.ID = hist.storeID.value.Value;
            cog.AddAttribute("ID", cog.ID);
            cog.AddAttribute("Type", "History");
            cog.AddAttribute("CreationTime", hist.creationTime.value.ToString());
            cog.AddAttribute("Name", hist.name.value[0].value);
            cog.AddAttribute("Path", hist.searchPath.value);
            cog.AddAttribute("actualCompletionTime", hist.actualCompletionTime.value.ToString());
            cog.AddAttribute("actualExecutionTime", hist.actualExecutionTime.value.ToString());
            cog.AddAttribute("requestedExecutionTime", hist.requestedExecutionTime.value.ToString());
            cog.AddAttribute("Status", hist.status.value);

            if (hist.user.value != null)
            {
                cog.AddAttribute("User", hist.user.value[0].searchPath.value);
            }
            else
            {
                cog.AddAttribute("User", "Null");
            }
        }

        /**
         * Method to add attributes specific to a Schedule object to our CogObject
         */
        private void addScheduleAttributes(ref CogObject cog, cognosdotnet_2_0.schedule sched)
        {
            cog.ID = sched.storeID.value.Value;
            cog.AddAttribute("ID", cog.ID);
            cog.AddAttribute("Type", "Schedule");
            cog.AddAttribute("CreationTime", sched.creationTime.value.ToString());
            cog.AddAttribute("Name", sched.name.value[0].value);
            cog.AddAttribute("Path", sched.searchPath.value);
            cog.AddAttribute("Active", sched.active.value.ToString());
            cog.AddAttribute("Credential", sched.credential.value[0].searchPath.value);
            
            if(sched.dailyPeriod.value != null)
            {
                cog.AddAttribute("DailyPeriod", sched.dailyPeriod.value);
            }
            cog.AddAttribute("EndDate", sched.endDate.value.ToString());
            cog.AddAttribute("EndType", sched.endType.value);

            if (sched.everyNPeriods.value != null)
            {
                cog.AddAttribute("EveryNPeriods", sched.everyNPeriods.value);
            }
            else
            {
                cog.AddAttribute("EveryNPeriods", "null");
            }
            addOptionsAttributes(ref cog, sched.options.value);
        }

        private void addReportVersionAttributes(ref CogObject cog, reportVersion rver)
        {
            cog.ID = rver.storeID.value.Value;
            cog.AddAttribute("ID", cog.ID);
            cog.AddAttribute("Type", "ReportVersion");
            cog.AddAttribute("CreationTime", rver.creationTime.value.ToString());
            cog.AddAttribute("Name", rver.name.value[0].value);
            cog.AddAttribute("Path", rver.searchPath.value);
            cog.AddAttribute("asOfTime", rver.asOfTime.value.ToString());
            addOptionsAttributes(ref cog, rver.options.value);
        }

        /*
         * Method to extract the options, which can vary a great deal, from
         * the array of options
         */
        private void addOptionsAttributes(ref CogObject cog, cognosdotnet_2_0.option[] options)
        {
            if (options != null)
            {
                for (int i = 0; i < options.Length; i++)
                {
                    cognosdotnet_2_0.option option = options[i];
                    System.Type optionType = option.GetType();

                    if (optionType.Name == "runOptionBoolean")
                    {
                        cognosdotnet_2_0.runOptionBoolean rob = (cognosdotnet_2_0.runOptionBoolean)option;
                        cog.AddAttribute(rob.name.ToString(), rob.value.ToString());
                    }
                    else if (optionType.Name == "runOptionData")
                    {
                        cognosdotnet_2_0.runOptionData rod = (cognosdotnet_2_0.runOptionData)option;
                        cog.AddAttribute(rod.name.ToString(), rod.value.ToString());
                    }
                    else if (optionType.Name == "runOptionStringArray")
                    {
                        cognosdotnet_2_0.runOptionStringArray rosa = (cognosdotnet_2_0.runOptionStringArray)option;
                        StringBuilder runOptions = new StringBuilder();
                        foreach (string s in rosa.value)
                        {
                            runOptions.AppendFormat("{0}:", s);
                        }
                        // Remove the last colon
                        runOptions.Remove(runOptions.Length - 1, 1);
                        cog.AddAttribute(rosa.name.ToString(), runOptions.ToString());
                    }
                    else if (optionType.Name == "runOptionLanguageArray")
                    {
                        cognosdotnet_2_0.runOptionLanguageArray rola = (cognosdotnet_2_0.runOptionLanguageArray)option;
                        StringBuilder languageOptions = new StringBuilder();
                        foreach (string s in rola.value)
                        {
                            languageOptions.AppendFormat("{0}:", s);
                        }
                        // Remove the last colon
                        languageOptions.Remove(languageOptions.Length - 1, 1);
                        cog.AddAttribute(rola.name.ToString(), languageOptions.ToString());
                    }
                    else if (optionType.Name == "deliveryOptionString")
                    {
                        cognosdotnet_2_0.deliveryOptionString dos = (cognosdotnet_2_0.deliveryOptionString)option;
                        cog.AddAttribute(dos.name.ToString(), dos.value);
                    }
                    else if (optionType.Name == "deliveryOptionSearchPathMultipleObjectArray")
                    {
                        cognosdotnet_2_0.deliveryOptionSearchPathMultipleObjectArray dospmoa = (deliveryOptionSearchPathMultipleObjectArray)option;
                        StringBuilder toOptions = new StringBuilder();
                        foreach (searchPathMultipleObject s in dospmoa.value)
                        {
                            toOptions.AppendFormat("{0}|", s.Value);
                        }
                        // Remove the last colon
                        toOptions.Remove(toOptions.Length - 1, 1);
                        cog.AddAttribute(dospmoa.name.ToString(), toOptions.ToString());
                    }
                    else if (optionType.Name == "deliveryOptionAddressSMTPArray")
                    {
                        cognosdotnet_2_0.deliveryOptionAddressSMTPArray doasa = (deliveryOptionAddressSMTPArray)option;
                        StringBuilder ccOptions = new StringBuilder();
                        foreach (addressSMTP a in doasa.value)
                        {
                            ccOptions.AppendFormat("{0}:", a.Value);
                        }
                        // Remove the last colon
                        ccOptions.Remove(ccOptions.Length - 1, 1);
                        cog.AddAttribute(doasa.name.ToString(), ccOptions.ToString());
                    }
                }
            }
        }

        private void addSecurityPolicies(ref CogObject cog, cognosdotnet_2_0.baseClass bc)
        {
            if (bc is cognosdotnet_2_0.package)
            {
                cognosdotnet_2_0.package pkg = (cognosdotnet_2_0.package)bc;
                int count = 0;
                foreach (cognosdotnet_2_0.policy plcy in pkg.policies.value)
                {
                    cog.AddAttribute("SecurityPath" + count, plcy.securityObject.searchPath.value);
                    count++;
                }
            }
            else if (bc is cognosdotnet_2_0.report)
            {
                cognosdotnet_2_0.report rpt = (cognosdotnet_2_0.report)bc;
                int count = 0;
                foreach (cognosdotnet_2_0.policy plcy in rpt.policies.value)
                {
                    cog.AddAttribute("SecurityPath" + count, plcy.securityObject.searchPath.value);
                    count++;
                }
            }
            else if (bc is cognosdotnet_2_0.reportView)
            {
                cognosdotnet_2_0.reportView rv = (cognosdotnet_2_0.reportView)bc;
                int count = 0;
                foreach (cognosdotnet_2_0.policy plcy in rv.policies.value)
                {
                    cog.AddAttribute("SecurityPath" + count, plcy.securityObject.searchPath.value);
                    count++;
                }
            }
            else if (bc is cognosdotnet_2_0.query)
            {
                cognosdotnet_2_0.query qry = (cognosdotnet_2_0.query)bc;
                int count = 0;
                foreach (cognosdotnet_2_0.policy plcy in qry.policies.value)
                {
                    cog.AddAttribute("SecurityPath" + count, plcy.securityObject.searchPath.value);
                    count++;
                }
            }
        }
    }
}
