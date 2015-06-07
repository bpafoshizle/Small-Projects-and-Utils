using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using cognosdotnet_2_0;
using Microsoft.Office.Interop.Excel;
using System.Web.Services.Protocols;
using System.Reflection;
using System.Data.Common;
using System.Data;

namespace Tyson.BI.IS.Cognos.SDK.TurnOffAndReportDuration
{
    class Duration
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

        public Duration()
        {
            cCMS = new contentManagerService1();

            cogVersion8 = "Cognos8";
            cogVersion11 = "Cognos11";
            cog8URL = "http://cogpbi1.tyson.com:9300/p2pd/servlet/dispatch";
            //cog11URL = "http://reprd2.tyson.com:9300/crn11mr2/cgi-bin/cognos.cgi";
            cog8BaseReportPath = "http://perform.tyson.com/cognos8/cgi-bin/cognosisapi.dll?b_action=xts.run&m=portal/report-viewer.xts&ui.action=run&ui.object=";
            cog11BaseReportPath = "http://reports.tyson.com/crn/cgi-bin/cognosisapi.dll?b_action=xts.run&m=portal/report-viewer.xts&method=execute&m_obj=";

            cCMS.Url = cog8URL;

            user = "CognosReportNet";
            pass = "En(Rp(CGNS10";
            ns = "ADS";
            specificUserLogon(false, cCMS, user, pass, ns);
        }

        static void Main(string[] args)
        {
            Duration handler = new Duration();

            // Run the following code to turn off the duration property for all objects
            // and to print the objects with the property on to an excel sheet for analysis.
            Microsoft.Office.Interop.Excel.Application oXL = new Microsoft.Office.Interop.Excel.Application();
            Microsoft.Office.Interop.Excel._Workbook oWB;
            Microsoft.Office.Interop.Excel._Worksheet oSheet;
            Microsoft.Office.Interop.Excel.Range oRange;
            oWB = (Microsoft.Office.Interop.Excel._Workbook)(oXL.Workbooks.Add(Missing.Value));
            oSheet = (Microsoft.Office.Interop.Excel._Worksheet)oWB.ActiveSheet;
            int[] cellCount = { 1, 1 };

            Console.WriteLine(args[0]);
            
            switch (args[0])
            {
                case "-report":
                    handler.report_ReportDuration(ref oSheet, cellCount);
                    handler.report_QueryDuration(ref oSheet, cellCount);
                    handler.report_ReportViewDuration(ref oSheet, cellCount);
                    break;
                case "-turnOff":
                    handler.turnOffAndReport_ReportDuration(ref oSheet, cellCount);
                    handler.turnOffAndReport_QueryDuration(ref oSheet, cellCount);
                    handler.turnOffAndReport_ReportViewDuration(ref oSheet, cellCount);
                        break;
            }

            //stats.turnOffMyFolderContentDuration(ref oSheet, cellCount);
            oXL.Visible = true;
        }

        private bool turnOffAndReport_MyFolderContentDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

        private bool report_MyFolderContentDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }

        private bool turnOffAndReport_ReportDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

        private bool report_ReportDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
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
                            }
                        }
                    }
                }
            }
            return true;
        }

        private bool turnOffAndReport_QueryDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

        private bool report_QueryDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

            if (bc.Length > 0)
            {
                foreach (baseClass report_item in bc)
                {
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
                            }
                        }
                    }
                }
            }
            return true;
        }

        private bool turnOffAndReport_ReportViewDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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

        private bool report_ReportViewDuration(ref _Worksheet oSheet, int[] cellCount)
        {
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
                            }
                        }
                    }
                }
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
    }
}
