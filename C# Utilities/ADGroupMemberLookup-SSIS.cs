/* Microsoft SQL Server Integration Services Script Component
*  Write scripts using Microsoft Visual C# 2008.
*  ScriptMain is the entry point class of the script.*/

using System;
using System.Data;
using System.DirectoryServices;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{

    public override void PreExecute()
    {
        base.PreExecute();
        /*
          Add your code here for preprocessing or remove if not needed
        */
    }

    public override void PostExecute()
    {
        base.PostExecute();
        /*
          Add your code here for postprocessing or remove if not needed
          You can set read/write variables here, for example:
          Variables.MyIntVar = 100
        */
    }

    public override void CreateNewOutputRows()
    {
        DirectorySearcher ADLookup = new DirectorySearcher();
        ADLookup.Filter = "(cn=IS BI Foundation Team)";
        ADLookup.PropertiesToLoad.Add("member");
        ADLookup.PropertiesToLoad.Add("cn");
        SearchResultCollection group = ADLookup.FindAll();

        foreach (SearchResult member in group)
        {
            foreach (Object memberObj in member.Properties["cn"])
            {
                DirectoryEntry user = new DirectoryEntry(memberObj);
                System.DirectoryServices.PropertyCollection userProps = user.Properties;
                Output0Buffer.AddRow();
                Output0Buffer.User = userProps["SAMAccountName"].Value.ToString();
            }
        }

        /*
        if (result != null)
        {
            for (int i = 0; i < result.Properties["member"].Count; i++)
            {
                DirectorySearcher ADUserLookup = new DirectorySearcher();
                ADUserLookup.Filter = (String)result.Properties["member"][i];
                ADUserLookup.PropertiesToLoad.Add("cn");
                SearchResult user = ADUserLookup.FindOne();

                if (user != null)
                {
                        Output0Buffer.AddRow();
                        Output0Buffer.User = (String)user.Properties["cn"][0];
                }

            }
        }*/
    }

}

