using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ChangeSubscriptionEmail.ReportService2005;
using System.Net;
using System.Web.Services.Protocols;
using System.IO;

namespace ChangeSubscriptionEmail
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
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
                string emailAddressMapFile = args[2];
                Dictionary<string, string> emailAddressMap = populateEmailAddressMap(emailAddressMapFile);
                modSubscriptionEmail(rs, items, emailAddressMap);
                Console.WriteLine("\nFinished... Press any key");
                Console.ReadLine();
            }
        }


        private static Dictionary<string, string> populateEmailAddressMap(string emailAddressMapFile)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();
            StreamReader sr = File.OpenText(emailAddressMapFile);
            string line;
            string[] brokenValues;

            while ((line = sr.ReadLine()) != null)
            {
                brokenValues = line.Split('|');
                dict.Add(brokenValues[0].Trim().ToUpper(), brokenValues[1].Trim().ToUpper());
            }

            sr.Close();

            return dict;
        }



        private static void printUsage()
        {
            Console.WriteLine("\n\nUsage:\nChangeSubscriptionEmail.exe [SSRS Web Service URL] [Folder or Report Path] [Email address map]\n" +
                              "Web servce URL is something like: 'http://reportweb-test.tyson.com/reportserver/reportservice2005.asmx'\nPress an key...");
        }


        private static int modSubscriptionEmail(ReportingService2005 rs, List<CatalogItem> items, Dictionary<string, string> emailAddressMap)
        {

            if (items.Count == 0)
            {
                return 0;
            }
            else
            {
                CatalogItem item = items[items.Count - 1];
                items.RemoveAt(items.Count - 1);
                //Console.WriteLine("Hitting: " + item.Path);

                if (rs.GetItemType(item.Path) == ItemTypeEnum.Report)
                {
                    Console.WriteLine("Modifying email address for: " + item.Path);
                    foreach (var sub in rs.ListSubscriptions(item.Path, null))
                    {
                        if (sub.EventType == "TimedSubscription" && sub.IsDataDriven == false)
                        {
                            string subID = sub.SubscriptionID;
                            string owner = sub.Owner;
                            string description = sub.Description;
                            ActiveState activeState = sub.Active;
                            string status = sub.Status;
                            string eventType = sub.EventType;
                            string matchData;
                            ParameterValue[] parameters = null;
                            ExtensionSettings es = sub.DeliverySettings;

                            if (sub.DeliverySettings.Extension == "Report Server FileShare")
                            {
                                es.ParameterValues = setPassword(es.ParameterValues);
                            }

                            try
                            {
                                rs.GetSubscriptionProperties(subID, out es, out description, out activeState, out status, out eventType, out matchData, out parameters);
                                es = swapEmailAddresses(es, emailAddressMap, ref description);
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
                modSubscriptionEmail(rs, items, emailAddressMap);
                return 0;
            }
        }

        // Warning, this method has the side effect of setting description
        private static ExtensionSettings swapEmailAddresses(ExtensionSettings es, Dictionary<string, string> emailAddressMap, ref string description)
        {
            string currentEmail = getToLine(es);
            string newEmail = findNewEmail(currentEmail, emailAddressMap);
            description = "Send e-mail to " + newEmail;
            es = setNewEmail(newEmail, es);
            return es;

        }

        private static ExtensionSettings setNewEmail(string newEmail, ExtensionSettings es)
        {
            ParameterValueOrFieldReference[] paramVals = es.ParameterValues;
            ParameterValue paramVal;
            for (int i = 0; i < paramVals.Length; i++)
            {
                paramVal = (ParameterValue)paramVals[i];
                if (paramVal.Name == "TO")
                {
                    paramVal.Value = newEmail;
                    paramVals[i] = paramVal;
                }
            }

            es.ParameterValues = paramVals;
            return es;
        }

        private static string findNewEmail(string currentEmail, Dictionary<string, string> emailAddressMap)
        {
            string newEmail = null;

            // may have to clean up and remove elisha's email
            emailAddressMap.TryGetValue(currentEmail.ToUpper(), out newEmail);
            return newEmail;
        }

        private static string getToLine(ExtensionSettings es)
        {
            ParameterValueOrFieldReference[] paramVals = es.ParameterValues;
            ParameterValue paramVal;
            for (int i = 0; i < paramVals.Length; i++)
            {
                paramVal = (ParameterValue)paramVals[i];
                if (paramVal.Name == "TO")
                {
                    return paramVal.Value;
                }
            }

            return null;
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
