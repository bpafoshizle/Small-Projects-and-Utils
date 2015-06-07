using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace RefreshExcelReports
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 7)
            {
                Console.WriteLine("Usage: RefreshExcelReport.exe AsOfRefreshDate ReportId ReportPath Server executeOrPrint loadBalance dataSourceType DatabaseServer [controlId]\r\nPress any key...");
                Console.ReadLine();
            }
            else
            {
                string asOfRefreshDate = args[0];
                string reportId = args[1];
                string reportPath = args[2];
                string server = args[3];
                string executeOrPrint = args[4];
                string loadBalance = args[5];
                string dataSourceType = args[6];
                string dbServer = args[7];
                int controlId = -1;

                // Set optional controlId
                if (args.Length == 9)
                {
                    controlId = Convert.ToInt32(args[8]);
                }

                string myDesktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
                
                if(!(executeOrPrint.ToLower().Equals("execute") || executeOrPrint.ToLower().Equals("print")))
                {
                    Console.WriteLine("Did not understand execution option: {0}. Use 'Execute' or 'Print'.", executeOrPrint);
                }
                else
                {                 
                        if (executeOrPrint.ToLower() == "execute")
                        {
                            // If the process was not started from an outer scope and passed to this program, then do the initial insert
                            if (controlId == -1)
                            {
                                controlId = insertStartRefreshRow(reportId, asOfRefreshDate, server, dataSourceType, dbServer);
                            }
                            else
                            {
                                updateReportToRunningInTable(controlId, dataSourceType, dbServer);
                            }

                            Stopwatch timer = Stopwatch.StartNew();
                            try
                            {
                                FileInfo oldInfo = new FileInfo(reportPath);
                                DateTime originalTime = oldInfo.LastWriteTime;
                                if (!IsFileLocked(new FileInfo(reportPath)))
                                {
                                    ExcelRefresh(reportPath, server, loadBalance);
                                    timer.Stop();

                                    TimeSpan timeElapsed = timer.Elapsed;
                                    double minutesPassed = timeElapsed.TotalMinutes;

                                    FileInfo refreshedInfo = new FileInfo(reportPath);
                                    DateTime refreshedTime = refreshedInfo.LastWriteTime;

                                    // Make sure that the file write time metadata is different than before the refresh.
                                    if (refreshedTime != originalTime)
                                    {
                                        updateReportToDoneInTable(controlId, dbServer);
                                    }
                                    else
                                    {
                                        updateReportToTimesMatchInTable(controlId, dbServer);
                                    }

                                    Console.WriteLine(String.Format("Refreshed {0} in {1} minutes", reportPath, Math.Round(minutesPassed, 2).ToString()));
                                }
                                else
                                {
                                    Console.WriteLine(String.Format("!!!Error!!! Skipping report that was open or locked: {0}", reportPath));
                                    updateReportToSkippedForLockInTable(controlId, dbServer);
                                }
                            }
                            catch (Exception ex)
                            {
                                timer.Stop();
                                TimeSpan timeElapsed = timer.Elapsed;
                                double minutesPassed = timeElapsed.TotalMinutes;
                                if (controlId != -1)
                                {
                                    updateReportToFailedInTable(controlId, dbServer);
                                }
                                Console.WriteLine(String.Format("Failed to refresh {0} in {1} minutes", reportPath, Math.Round(minutesPassed, 2).ToString()));
                            }
                        }                   
                        else
                        {
                            Console.WriteLine(reportPath);
                        }
                }
            }

           //Console.ReadLine();
        }



        private static void updateReportToSkippedForLockInTable(int controlId, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = 'Skipped - Locked', Endtime = '{0}' where ControlId = {1}", DateTime.Now, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void updateReportToErrorChangingServer(int controlId, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = 'Skipped - Error changing server', Endtime = '{0}' where ControlId = {1}", DateTime.Now, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void updateReportToTimesMatchInTable(int controlId, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = 'Failed - Times Match', Endtime = '{0}' where ControlId = {1}", DateTime.Now, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void updateReportToFailedInTable(int controlId, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = 'Failed', Endtime = '{0}' where ControlId = {1}", DateTime.Now, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }


        static DataTable getReportsToSubmitFromTable(string asOfRefreshDate, string access, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";

            var adapter = new SqlDataAdapter(String.Format(@"with reportIDsFinishedToday as
                                                (
	                                                select distinct ReportId
	                                                from tra_ops_mss.dbo.EXCEL_REPORT_REFRESH_CONTROL
	                                                where [Status] = 'Completed'
	                                                and AsOfRefreshDate = '{0}'
                                                )
                                                select report_id, report_path
                                                from tra_ops_mss.dbo.EXCEL_REPORT_REFRESH_LIST list
                                                left join reportIDsFinishedToday finished
	                                                on list.REPORT_ID = finished.ReportID
                                                where finished.ReportID is null
                                                and list.access = '{1}'
                                                and list.ENABLED_IND = 'Enabled'
                                                order by list.GLOBAL_RANK", asOfRefreshDate, access), connectionString);
            var ds = new DataSet();

            adapter.Fill(ds, "anyNameHere");

            DataTable data = ds.Tables["anyNameHere"];        

            return data;
        }

        private static void updateReportToDoneInTable(int controlId, string dbServer)
        {
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = 'Completed', Endtime = '{0}' where ControlId = {1}", DateTime.Now, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static int insertStartRefreshRow(string reportId, string asOfRefreshDate, string tabularServer, string dataSourceType, string dbServer)
        {
            string dataSourceSpecificStatus = (dataSourceType == "sql" ? "Running Sql" : "Running");
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";

            var adapter = new SqlDataAdapter(String.Format(@"insert into EXCEL_REPORT_REFRESH_CONTROL(
                                                            AsOfRefreshDate, ReportId, TabularServer, [Status])
                                                            OUTPUT Inserted.[ControlID]
                                                            VALUES('{0}', {1}, '{2}', '{3}')", asOfRefreshDate, reportId, tabularServer, dataSourceSpecificStatus), connectionString);
            var ds = new DataSet();

            adapter.Fill(ds, "anyNameHere");
            
            DataTable data = ds.Tables["anyNameHere"];

            return Convert.ToInt32(data.Rows[0]["ControlId"]);
        }

        private static void updateReportToRunningInTable(int controlId, string dataSourceType, string dbServer)
        {
            string dataSourceSpecificStatus = (dataSourceType == "sql" ? "Running Sql" : "Running");
            var connectionString = "Server=" + dbServer + "; Database=TRA_OPS_MSS; Trusted_Connection=yes;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(String.Format("UPDATE DBO.EXCEL_REPORT_REFRESH_CONTROL SET Status = '{0}' where ControlId = {1}", dataSourceSpecificStatus, controlId), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static void ExcelRefresh(string Filename, string targetServer, string loadBalance)
        {
            
            object NullValue = System.Reflection.Missing.Value;
            Microsoft.Office.Interop.Excel.Application excelApp = new Microsoft.Office.Interop.Excel.ApplicationClass();
      
            try
            {                
                excelApp.DisplayAlerts = false;
                excelApp.AskToUpdateLinks = false;
                excelApp.Visible = true;
                Microsoft.Office.Interop.Excel.Workbook Workbook = excelApp.Workbooks.Open(
                   Filename, NullValue, false, NullValue, NullValue,
                   NullValue, true, NullValue, NullValue, NullValue,
                   NullValue, NullValue, NullValue, NullValue, NullValue);

                if (loadBalance.Equals("Load Balance"))
                {
                    for (int i = 1; i <= Workbook.Connections.Count; i++)
                    {
                        var con = Workbook.Connections.Item(i);
                        var oleDBCon = con.OLEDBConnection;
                        var conStr = (string)oleDBCon.Connection;
                        var newConStr = Regex.Replace(conStr, @"Data Source=.*?;", String.Format(@"Data Source={0};", targetServer));
                        con.OLEDBConnection.Connection = newConStr;
                    }
                }

                Workbook.RefreshAll();

                /*
                if (loadBalance.Equals("Load Balance"))
                {
                    for (int i = 1; i <= Workbook.Connections.Count; i++)
                    {
                        var con = Workbook.Connections.Item(i);
                        var oleDBCon = con.OLEDBConnection;
                        var conStr = (string)oleDBCon.Connection;
                        var newConStr = Regex.Replace(conStr, @"Data Source=.*?;", String.Format(@"Data Source=tratab.tyson.com;"));
                        con.OLEDBConnection.Connection = newConStr;
                    }
                }
                 */

                Workbook.Save();
                Workbook.Close(false, Filename, null);
                excelApp.Quit();

                System.Runtime.InteropServices.Marshal.ReleaseComObject(Workbook);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(excelApp);

                System.Runtime.InteropServices.Marshal.FinalReleaseComObject(Workbook);
                System.Runtime.InteropServices.Marshal.FinalReleaseComObject(excelApp);

                //Workbook = null;
            }
            catch (Exception ex)
            {

                System.Diagnostics.EventLog.WriteEntry("RefreshExcelReport", ex.Message,
                                       System.Diagnostics.EventLogEntryType.Warning);

                System.Diagnostics.EventLog.WriteEntry("RefreshExcelReport", ex.StackTrace,
                                       System.Diagnostics.EventLogEntryType.Warning);

                throw ex;
            }
            finally
            {
                excelApp = null;
            }

            //excelApp.Quit();
        }

        static bool IsFileLocked(FileInfo file)
        {
            FileStream stream = null;

            try
            {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                //the file is unavailable because it is:
                //still being written to
                //or being processed by another thread
                //or does not exist (has already been processed)
                return true;
            }
            finally
            {
                if (stream != null)
                    stream.Close();
            }

            //file is not locked
            return false;
        }

    }

}
