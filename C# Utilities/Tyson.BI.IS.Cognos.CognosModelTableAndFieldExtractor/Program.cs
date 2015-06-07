using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;

namespace Tyson.BI.IS.Cognos.ConogsModelTableAndFieldExtractor
{
    class Program
    {
        static void Main(string[] args)
        {
            XmlTextReader reader = new XmlTextReader("model.xml");
            XmlDocument modelDoc = new XmlDocument();
            string modelName = "Sales (Foodservice)";
            modelDoc.Load("model.xml");
            XmlNodeList namespaces = modelDoc.GetElementsByTagName("namespace");
            System.IO.StreamWriter writer = new StreamWriter("ModelTablesAndFields.csv");
            writer.WriteLine("Model Name|Table Name|Field Name");

            foreach (XmlElement modelNamespace in namespaces)
            {
                if (modelNamespace.ChildNodes[0].InnerText == "Database")
                {
                    XmlDocument dbDoc = new XmlDocument();
                    dbDoc.LoadXml("<?xml version=\"1.0\" encoding=\"UTF-8\" ?><root>" + modelNamespace.InnerXml + "</root>");
                    XmlNodeList querySubjects = dbDoc.GetElementsByTagName("querySubject");


                    foreach (XmlElement querySubject in querySubjects)
                    {
                        string tableName = "";
                        if (querySubject.ChildNodes[0].Name == "name")
                        {
                            tableName = querySubject.ChildNodes[0].InnerText;
                        }

                        foreach (XmlElement subElement in querySubject)
                        {
                            if (subElement.Name == "queryItem")
                            {
                                writer.WriteLine(modelName + "|" + tableName + "|" + subElement.ChildNodes[0].InnerText);
                            }
                        }
                    }
                }
            }

            writer.Close();
        }
    }
}

