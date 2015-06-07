using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DisableSubscriptionsBySubID.ReportService2005;
using System.Net;
using System.Web.Services.Protocols;

namespace DisableSubscriptionsBySubID
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                printUsage();
                Console.ReadLine();
            }
            else
            {
                var rs = new ReportingService2005();
                rs.Credentials = CredentialCache.DefaultCredentials;
                rs.Url = args[0];
                string subID = args[1];
                disableSubscription(rs, subID);
                Console.WriteLine("\nFinished... Press any key");
                Console.ReadLine();
            }
        }

        private static void printUsage()
        {
            Console.WriteLine("\n\nUsage:\nDisableSSRSSubscriptions.exe [SSRS Web Service URL] [SubscriptionGUID]\n\n" + 
                              
                              "Web servce URL is something like: 'http://reportweb-test.tyson.com/reportserver/reportservice2005.asmx'\n\n" + 

                              "Run a query similar to the following to determine the GUID of the subscription you want:\n" +
                              "select u.UserName,  c.Name, s.SubscriptionID from Subscriptions s\n" +
                              "inner join Users u on s.OwnerID = u.UserID\n" +
                              "inner join Catalog c on c.ItemID = s.Report_OID\n" +
                              "where u.UserName = 'TYSONET\\HOLTE'");
        }


        private static int disableSubscription(ReportingService2005 rs, string subID)
        {
            /* Set up schedule info for any time in the past */
            string scheduleXML =
                    @"<ScheduleDefinition>" +
                     "   <StartDateTime>2010-12-31T08:00:00-08:00" +
                     "   </StartDateTime>" +
                     "</ScheduleDefinition>";

            ExtensionSettings es;
            string owner;
            string description;
            ActiveState activeState;
            string status; 
            string eventType;
            string matchData = scheduleXML; //Based on if schedule is shared schedule, make it stop now
            string oldMatchData;
            ParameterValue[] parameters = null;


            
            try
            {
                owner = rs.GetSubscriptionProperties(subID, out es, out description, out activeState, out status, out eventType, out oldMatchData, out parameters);
                rs.SetSubscriptionProperties(subID, es, description, eventType, matchData, parameters);
            }
            catch (SoapException ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return 0;
        }

        private static ParameterValueOrFieldReference[] setPassword(ParameterValueOrFieldReference[] parameterValueOrFieldReference)
        {
            ParameterValue[] paramVals = new ParameterValue[parameterValueOrFieldReference.Length + 1];
            for (int i = 0; i < parameterValueOrFieldReference.Length; i++)
            {
                paramVals[i] = (ParameterValue)parameterValueOrFieldReference[i];
            }

            paramVals[parameterValueOrFieldReference.Length] = new ParameterValue();
            paramVals[parameterValueOrFieldReference.Length].Name = "PASSWORD";
            paramVals[parameterValueOrFieldReference.Length].Value = "pass";
            return paramVals;
        }

    }
}
