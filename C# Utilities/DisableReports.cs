using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using cognosdotnet_2_0;
using System.Web.Services.Protocols;
using Microsoft.Practices.EnterpriseLibrary.Data;
using System.Data.Common;
using System.Data;

namespace Tyson.BI.IS.Cognos.SDK.DisableReports
{
    class DisableReports
    {
        contentManagerService1 cCMS;
        string cog8URL;

        // User, Pass, and Namespace are common to both environments
        string user;
        string pass;
        string ns;

        public DisableReports()
        {
            cCMS = new contentManagerService1();
            cog8URL = "http://cogpbi1.tyson.com:9300/p2pd/servlet/dispatch";
            cCMS.Url = cog8URL;
            cCMS.Timeout = -1;

            user = "CognosReportNet";
            pass = "En(Rp(CGNS10";
            ns = "ADS";
            specificUserLogon(false, cCMS, user, pass, ns);
        }

        static void Main(string[] args)
        {
            DisableReports disabler = new DisableReports();

            try
            {
                /********************************************************************************/
                /* The Following line will disable Cognos objects given by a list of
                   paths for those objects in a text file -- C8 only */
                disabler.disableCogObjects(args[0], Convert.ToInt32(args[1]));
                /********************************************************************************/
            }
            catch (Exception ex)
            {
                if (ex is System.IndexOutOfRangeException)
                {
                    Console.WriteLine("Usage: DisableReports.exe startDate(dd-mmm-yyyy), daysBack"); 
                }
                Console.WriteLine(ex);
            }
        }

        public void disableCogObjects(string startDate, int daysBack)
        {
            List<string> reportPaths = getPathToDisableList(startDate, daysBack);
            
            /* Debugging 
            List<string> reportPaths = new List<string>();
            reportPaths.Add("/content/package[@name='Supply Chain']/folder[@name='Demand']/report[@name='(0014) Product Group Summary'])&ui.name=(0014) Product Group Summary&ui.format=XLS&ui.backURL=/cognos8/cgi-bin/cognosisapi.dll?b_action=xts.run&m=portal/cc.xts&m_folder=iAB52A69DC50C4C77AC3A623A085EF52A");
             */
            
            List<string> exceptionPaths = getExceptionPathList();
            writePathToDisableList(reportPaths);

            Console.WriteLine("Please check the file: objectsToDisable.txt in the program executable folder " +
                              "to make sure everything looks good. Do you want to proceed? Enter 'yes' or 'no' (case insensitive).");
            
            string userChoice = Console.ReadLine();

            if(userChoice.ToLower() == "yes")
            {
                StreamWriter errorWriter = new StreamWriter("objectsNotFoundC8.txt");
                StreamWriter successWriter = new StreamWriter("objectsFoundC8.txt");
                // Declare query properties array for report
                propEnum[] props = new propEnum[] { propEnum.defaultName, propEnum.disabled,
                                                    propEnum.searchPath};

                // Declare sort properties for reports and users
                //reports
                sort[] sorts = new sort[] { };

                // Object representing path to single report
                searchPathMultipleObject reportSearchPath = new searchPathMultipleObject();

                // Query options
                queryOptions qo = new queryOptions();

                List<baseClass> objectsToDisable = new List<baseClass>();

                // Loop through all the paths and generate a list of objects
                // to disable
                for (int i = 0; i < reportPaths.Count; i++)
                {
                    string path = reportPaths[i];

                    // If the path is not in the exception list, then add it to
                    // the list of paths to be disabled
                    if (!checkAgainstExceptions(path, exceptionPaths))
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
                    }
                }
                errorWriter.Close();
                successWriter.Close();

                updateOptions updateOptions = new updateOptions();

                cCMS.update(objectsToDisable.ToArray(), updateOptions);
            }
            else
            {
                Console.WriteLine("User entered something other than 'yes'. Aborting...");
            }
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

        // Method to query for a list of paths of objects to disable
        // Startdate string should be in the format "dd-mmm-yyyy"
        private List<string> getPathToDisableList(string startDate, int daysBack)
        {
            List<string> reportPaths = new List<string>();
            try
            {

                // Reports to disable query
                string rptsToDisable = @"SELECT 'C8' AS ENV, rr.COGIPF_PACKAGE, rr.COGIPF_REPORTNAME, rr.COGIPF_REPORTPATH,
                                        MAX(rr.COGIPF_LOCALTIMESTAMP) AS LAST_RUN_DATE FROM COGIPF_RUNREPORT rr
                                        GROUP BY rr.COGIPF_PACKAGE, rr.COGIPF_REPORTNAME, rr.COGIPF_REPORTPATH
                                        HAVING MAX(rr.COGIPF_LOCALTIMESTAMP) BETWEEN :startDate AND (sysdate - :daysBack)
                                        ORDER BY LAST_RUN_DATE";

                Database db = DatabaseFactory.CreateDatabase("dbptsn4_ptsn4");
                DbCommand dbCommand = db.GetSqlStringCommand(rptsToDisable);
                db.AddInParameter(dbCommand, ":startDate", DbType.String, startDate);
                db.AddInParameter(dbCommand, ":daysBack", DbType.Int32, daysBack);

                using (IDataReader dr = db.ExecuteReader(dbCommand))
                {
                    while (dr.Read())
                    {
                        reportPaths.Add((System.String)dr["COGIPF_REPORTPATH"]);
                    }
                }

                return reportPaths;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                reportPaths = null;
                return reportPaths;
            }
        }

        /** 
         * Function to check a string against a list of strings and return whether the first string was found was found
         */
        private bool checkAgainstExceptions(string path, List<string> exceptions)
        {
            return exceptions.Contains(path);
        }

        /** 
         * Read in all the exception paths from a file and return them as a comma delimited string
         */
        private List<string> getExceptionPathList()
        {
            System.IO.TextReader reader = new StreamReader("exceptions.txt");
            
            
            string path = reader.ReadLine();
            List<string> exceptionPaths = new List<string>();

            while (path != null)
            {
                exceptionPaths.Add(path);
                path = reader.ReadLine();
            }

            return exceptionPaths;
        }

        /**
         * Method to write out the list of reportPaths that will be disabled
         */
        private void writePathToDisableList(List<string> reportsToDisable)
        {
            Console.WriteLine("Writing report paths to be disabled");
            System.IO.TextWriter writer = new StreamWriter("objectsToDisable.txt");

            foreach (string path in reportsToDisable)
            {
                writer.WriteLine(path);
            }
            writer.Close();
        }
    }
}