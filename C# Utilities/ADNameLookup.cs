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
    private DataTable RunLDAPQuery(string query, string column)
    {
        DirectorySearcher ds = new DirectorySearcher();
        string sFilter2 = query;
        ds.Filter = sFilter2;
        SearchResultCollection src2 = ds.FindAll();
        DataTable dt = new DataTable("ADGroups");
        dt.Columns.Add(column, typeof(string));
        foreach (SearchResult s2 in src2)
        {
            ResultPropertyCollection rpc2 = s2.Properties;
            if (rpc2[column].Count > 0)
            {
                for (int icount = 0; icount < rpc2[column].Count; icount++)
                {
                    DataRow dr = dt.NewRow();
                    dr[column] = rpc2[column][icount];

                    dt.Rows.Add(dr);
                }
            }
            else
            {
                DataRow dr = dt.NewRow();
                dr[column] = rpc2[column];
                dt.Rows.Add(dr);
            }
        }
        return dt;
    }

    private String getADName(string user)
    {
        // Remove TYSONET\ from the beginning of the string. May want to 
        // use regular expressions here and print out the username with a groups
        // not found message to help with possible other prefixes down the road

        DirectoryServicesPermission dsPerm = new DirectoryServicesPermission(System.Security.Permissions.PermissionState.Unrestricted);
        dsPerm.Assert();
        // Filter to get user's distinguished name (DN)
        string sFilter = "(&(objectClass=user)(SAMAccountName=" + user + "))";

        DataTable dt = new DataTable("Result");

        // Query for all users and return a table of their distinguished names
        dt = RunLDAPQuery(sFilter, "Name");

        if (dt.Rows.Count == 1)
        {
            DataRow dr = dt.Rows[0];
            return (string)dr["Name"];
        }
        else
        {
            return ("User not found");
        }

    }

    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        string userName = getADName(Row.USERID);
        int delimiter = userName.IndexOf(",");
        string firstName, lastName;

        if (delimiter != -1)
        {
            lastName = userName.Substring(0, delimiter);
            firstName = userName.Substring(delimiter + 2);
        }
        else
        {
            lastName = "N/A (Service Account)";
            firstName = "N/A (Service Account)";
        }

        Output0Buffer.AddRow();
        Output0Buffer.USERID = Row.USERID;
        Output0Buffer.FIRSTNAME = firstName;
        Output0Buffer.LASTNAME = lastName;
        Output0Buffer.MODUSERID = Variables.UserName;
        Output0Buffer.MODDATE = Row.MODDATE;
    }

}
