using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;

namespace Tyson.BI.IS.SSISIncomingDirectoryWatcher
{
    public partial class SSISIncomingDirectoryWatcher : ServiceBase
    {

        private string dirToWatch;
        private string dirDest;
        private string dirLog;
        private string logFileName;
        private string logFilePath;
        private string confFilePath;
        private EventLog Lg;
        private TextWriter tw;
        string destPath;
        FileSystemWatcher Watcher;

        public SSISIncomingDirectoryWatcher()
        {
            InitializeComponent();
            confFilePath = "D:\\\\SSIS\\\\Files\\\\Tyson\\\\BI\\\\SSISIncomingDirectoryWatcher.conf";
            dirLog = "D:\\\\SSIS\\\\Files\\\\Tyson\\\\BI\\\\";
            logFileName = "SSISIncomingDirectoryWatcher.log";
            logFilePath = dirLog + logFileName;
        }


        protected override void OnStart(string[] args)
        {
            if (!EventLog.SourceExists("SSISIncomingDirectoryWatcher", "."))
            {
                EventLog.CreateEventSource("SSISIncomingDirectoryWatcher", "Application");
            }

            Lg = new EventLog("Application", ".", "SSISIncomingDirectoryWatcher");
            Lg.WriteEntry("Service started at " + DateTime.Now, EventLogEntryType.Information);

            try
            {

                tw = File.CreateText(logFilePath);
                tw.WriteLine("Service started at {0}", DateTime.Now);
                readInConfigValues();
                Watcher = new FileSystemWatcher();
                Watcher.Path = dirToWatch;
                Watcher.IncludeSubdirectories = false;
                Watcher.Created += new FileSystemEventHandler(watcherChange);
                Watcher.EnableRaisingEvents = true;
            }
            catch (Exception ex)
            {
                Lg.WriteEntry(ex.Message, EventLogEntryType.Error);
            }
            finally
            {
                tw.Close();
            }
        }


        protected override void OnStop()
        {
            EventLog Lg = new EventLog("Application", ".", "SSISIncomingDirectoryWatcher");
            tw = (TextWriter)File.AppendText(logFilePath);
            Lg.WriteEntry("Service stopped at " + DateTime.Now, EventLogEntryType.Information);
            tw.WriteLine("Service stopped at {0}", DateTime.Now);
            tw.Close();
        }

        protected override void OnShutdown()
        {
            EventLog Lg = new EventLog("Application", ".", "SSISIncomingDirectoryWatcher");
            tw = (TextWriter)File.AppendText(logFilePath);
            Lg.WriteEntry("Service shutdown at " + DateTime.Now, EventLogEntryType.Information);
            tw.WriteLine("Service shutdown at {0}", DateTime.Now);
            tw.Close();
        }

        private void watcherChange(object sender, FileSystemEventArgs e)
        {
            EventLog Lg = new EventLog("Application", ".", "SSISIncomingDirectoryWatcher");

            try
            {
                tw = (TextWriter)File.AppendText(logFilePath);
                FileInfo file = new FileInfo(e.FullPath);
                destPath = Path.Combine(dirDest, file.Name);

                while (isFileInUse(file))
                {
                    System.Threading.Thread.Sleep(1000);
                }

                file.CopyTo(destPath);

                Lg.WriteEntry("Moved [" + e.FullPath + "] to [" + destPath + "] at " + DateTime.Now, EventLogEntryType.Information);
                tw.WriteLine("Moved [{0}] to [{1}] at {2}", e.FullPath, destPath, DateTime.Now);
            }
            catch (Exception ex)
            {
                Lg.WriteEntry(ex.Message, EventLogEntryType.Error);
            }
            finally
            {
                tw.Close();
            }
        }

        private bool isFileInUse(FileInfo file)
        {
            FileStream stream = null;

            try
            {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException ioex)
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
                {
                    stream.Close();
                }
            }

            return false;
        }

        private void readInConfigValues()
        {
            string line;
            string[] splitVals;
            TextReader tr = null;
            try
            {
                tr = File.OpenText(confFilePath);

                while ((line = tr.ReadLine()) != null)
                {
                    splitVals = Regex.Split(line, @"\|");
                    if (splitVals[0] == "Watch")
                    {
                        dirToWatch = splitVals[1];
                    }
                    else if (splitVals[0] == "Dest")
                    {
                        dirDest = splitVals[1];
                    }
                    else
                    {
                        Lg.WriteEntry("Ignoring unknown config value: " + splitVals[0], EventLogEntryType.Warning);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (tr != null)
                {
                    tr.Close();
                }
            }
        }
    }
}
