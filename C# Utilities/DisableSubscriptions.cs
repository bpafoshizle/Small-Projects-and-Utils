using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DisableSSRSSubscriptions.ReportService2005;
using System.Net;
using System.Web.Services.Protocols;

namespace DisableSSRSSubscriptions
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
                CatalogItem item = new CatalogItem();
                item.Path = args[1];
                List<CatalogItem> items = new List<CatalogItem>();
                items.Add(item);
                disableSubscriptions(rs, items);
                Console.WriteLine("\nFinished... Press any key");
                Console.ReadLine();
            }
        }

        private static void printUsage()
        {
            Console.WriteLine("\n\nUsage:\nDisableSSRSSubscriptions.exe [SSRS Web Service URL] [Folder or Report Path]\n" +
                              "Web servce URL is something like: 'http://reportweb-test.tyson.com/reportserver/reportservice2005.asmx'\nPress an key...");
        }


        private static int disableSubscriptions(ReportingService2005 rs, List<CatalogItem> items)
        {
            /* Set up schedule info for any time in the past */
            string scheduleXML =
                    @"<ScheduleDefinition>" +
                     "   <StartDateTime>2010-12-31T08:00:00-08:00" +
                     "   </StartDateTime>" +
                     "</ScheduleDefinition>";

            if (items.Count == 0)
            {
                return 0;
            }
            else
            {
                CatalogItem item = items[items.Count -1];
                items.RemoveAt(items.Count - 1);
                //Console.WriteLine("Hitting: " + item.Path);

                if (rs.GetItemType(item.Path) == ItemTypeEnum.Report)
                {
                    Console.WriteLine("Disabling subscriptions for: " + item.Path);
                    foreach (var sub in rs.ListSubscriptions(item.Path, null))
                    {
                        if (sub.EventType == "TimedSubscription" && sub.IsDataDriven == false)
                        {
                            ExtensionSettings es = sub.DeliverySettings;

                            if (sub.DeliverySettings.Extension == "Report Server FileShare")
                            {
                                es.ParameterValues = setPassword(es.ParameterValues);
                            }

                            string description = sub.Description;
                            string eventType = sub.EventType;
                            string matchData = scheduleXML;//Based on if schedule is shared schedule, make it stop now
                            ParameterValue[] parameters = null;

                            try
                            {
                                rs.SetSubscriptionProperties(sub.SubscriptionID, es, description, eventType, matchData, parameters);
                            }
                            catch (SoapException ex)
                            {
                                Console.WriteLine(ex.ToString());
                            }
                        }
                    }
                }
                else if (rs.GetItemType(item.Path) == ItemTypeEnum.Folder)
                {
                    Console.WriteLine("Descending into folder: " + item.Path);
                    items.InsertRange(0, rs.ListChildren(item.Path, false));
                    //disableSubscriptions(rs, new List<CatalogItem>(rs.ListChildren(item.Path, true)));

                    //foreach (var catalogItem in rs.ListChildren(path, true))
                    //{
                    //    disableSubscriptions(rs, catalogItem.Path);
                    //}
                }
                disableSubscriptions(rs, items);
                return 0;
            }
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
