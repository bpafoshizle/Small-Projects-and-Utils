using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Text;

namespace Tyson.BI.IS.SSISIncomingDirectoryWatcher
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[] 
			{ 
				new SSISIncomingDirectoryWatcher() 
			};
            ServiceBase.Run(ServicesToRun);
            //((SSISIncomingDirectoryWatcher)ServicesToRun[0]).watchIncomingDirectory();
        }
    }
}
