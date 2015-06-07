using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ExecuteSSRSReport.RSService;
using System.Web.Services.Protocols;
using System.Threading;

namespace ExecuteSSRSReport
{
    class Program
    {
        static void Main2(string[] args)
        {
            RSService.ReportExecutionService rs = new RSService.ReportExecutionService();
            rs.Credentials = System.Net.CredentialCache.DefaultCredentials;
            rs.Url = "http://reportweb-test.tyson.com/reportserver/ReportExecution2005.asmx";
            rs.Timeout = Timeout.Infinite;

            // Render arguments
            byte[] result = null;
            string reportPath = "/Finance/Margins/12_09_Group1(0043) Margin byBusiness Unit- Material Detail";
            string format = "PDF";
            string historyID = null;
            string devInfo = @"<DeviceInfo><Toolbar>False</Toolbar></DeviceInfo>";

            // Get report parameters
            
            // Prepare report parameter
            ParameterValue[] parameters = new ParameterValue[1];
            parameters[0] = new ParameterValue();
            parameters[0].Name = "p_User_ID";
            parameters[0].Value = "Group01";

            // Do I need to set this? wtf MS
            DataSourceCredentials credentials = null;
            string showHideToggle = null;
            string encoding;
            string mimeType;
            string extension;
            Warning[] warnings = null;
            ParameterValue[] reportHistoryParameters = null;
            string[] streamIDs = null;

            ExecutionInfo execInfo = new ExecutionInfo();
            ExecutionHeader execHeader = new ExecutionHeader();

            rs.ExecutionHeaderValue = execHeader;

            execInfo = rs.LoadReport(reportPath, historyID);

            rs.SetExecutionParameters(parameters, "en-us");
            String SessionId = rs.ExecutionHeaderValue.ExecutionID;

            try
            {
                result = rs.Render(format, devInfo, out extension, out encoding, out mimeType, out warnings, out streamIDs);
                execInfo = rs.GetExecutionInfo();
                Console.WriteLine("Execution date and time: {0}", execInfo.ExecutionDateTime);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.InnerException);
            }
        }
    }
}
