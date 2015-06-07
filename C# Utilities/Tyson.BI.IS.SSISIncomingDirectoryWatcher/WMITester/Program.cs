using System;
using System.Collections.Generic;
using System.Text;
using System.Management;
using System.Text.RegularExpressions;
using System.IO;

namespace WMITester
{
    class Program
    {
        static void Main(string[] args)
        {
            string dirToWatch = "C:\\\\SSIS\\\\Files\\\\Tyson\\\\BI\\\\Incoming\\\\";
            string dirDest = "\\\\\\\\whqwssissb02\\\\SSIS\\\\Files\\\\Tyson\\\\BI\\\\Incoming\\\\";
            string dirLog = "C:\\\\SSIS\\\\Files\\\\Tyson\\\\BI\\\\";
            string logFileName = "SSISIncomingDirectoryWatcher.log";
            string drive = Regex.Replace(dirToWatch, @"(.*:).*", "$1");
            string path = Regex.Replace(dirToWatch, @".*:(.*)", "$1");
            FileInfo file;
            TextWriter tw = File.CreateText(dirLog + logFileName);

            // Create a query to be notified within 1 second of a file creation 
            WqlEventQuery query = new WqlEventQuery("__InstanceCreationEvent", new TimeSpan(0, 0, 1), "TargetInstance isa \"CIM_DataFile\" AND "
                                                                                                    + "TargetInstance.Drive=\"" + drive + "\" AND "
                                                                                                    + "TargetInstance.Path=\"" + path + "\"");
            
            // Initialize an event watcher and subscribe to events that match this query
            ManagementEventWatcher watcher = new ManagementEventWatcher(query);

            // Block until the next event occurs
            ManagementBaseObject e = watcher.WaitForNextEvent();

            string name = (string)((ManagementBaseObject)e["TargetInstance"])["Name"];
            file = new FileInfo(name);
            string destPath = Path.Combine(dirDest, file.Name);
            file.CopyTo(destPath);

            tw.WriteLine("Moved [{0}] to [{1}] at {2}", name, destPath, DateTime.Now);

            //Console.WriteLine("Name: {0}", ((ManagementBaseObject)e["TargetInstance"])["Name"]);
            //Console.WriteLine("FileName: {0}", ((ManagementBaseObject)e["TargetInstance"])["FileName"]);
            
            // Run this bitch to discover what's available.
            //ManagementBaseObject dataFile = (ManagementBaseObject)e["TargetInstance"];
            //PropertyDataCollection properties = dataFile.Properties;

            //foreach (PropertyData property in properties)
            //{
            //    Console.WriteLine("{0}: {1}", property.Name, property.Value);
            //}

            watcher.Stop();
            tw.Close();

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }
    }
}
