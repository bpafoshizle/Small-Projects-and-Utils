using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Services.Protocols;
using ExecuteSSRSReport.ReportingService2005;

namespace ExecuteSSRSReport
{
    class DisplayReportParameters
    {
           public static void Main()
           {
              ReportingService2005.ReportingService2005 rs = new ReportingService2005.ReportingService2005();
              rs.Credentials = System.Net.CredentialCache.DefaultCredentials;

              string report = "/Finance/Margins/12_09_Group1(0043) Margin byBusiness Unit- Material Detail";
              bool forRendering = false;
              string historyID = null;
              ParameterValue[] values = null;
              DataSourceCredentials[] credentials = null;
              ReportParameter[] parameters = null;

              try
              {
                 parameters = rs.GetReportParameters(report, historyID, forRendering, values, credentials);

                 if (parameters != null)
                 {
                    foreach (ReportParameter rp in parameters)
                    {
                       Console.WriteLine("Name: {0}", rp.Name);
                    }
                 }
                 Console.ReadLine();
              }

              catch (SoapException e)
              {
                 Console.WriteLine(e.Detail.InnerXml.ToString()); 
              }
           }
    }
}
