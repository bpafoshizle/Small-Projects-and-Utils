/* Microsoft SQL Server Integration Services Script Component
*  Write scripts using Microsoft Visual C# 2008.
*  ScriptMain is the entry point class of the script.*/

using System;
using System.Data;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.AnalysisServices.AdomdClient;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    AdomdConnection adoMdConn;
    AdomdDataReader dataReader;



    public override void PreExecute()
    {
        base.PreExecute();
        adoMdConn = new AdomdConnection("Data Source=WHQWSSASQRY1;Catalog=Tyson Analytics");
        adoMdConn.Open();
        AdomdCommand cmd = new AdomdCommand(Variables.qryCube, adoMdConn);
        dataReader = cmd.ExecuteReader();
    }

    public override void PostExecute()
    {
        base.PostExecute();
        dataReader.Close();
    }

    public override void CreateNewOutputRows()
    {
        while (dataReader.Read())
        {
            Output0Buffer.AddRow();
            Output0Buffer.FISCALYEARSTR = dataReader.GetString(0);
            Output0Buffer.FISCALMONTHSTR = dataReader.GetString(1);
            Output0Buffer.SELLINGGROUPSTR = dataReader.GetString(2);
            Output0Buffer.PAIDINVOICEAMOUNTSTR = dataReader.GetString(3);
        }
    }

}
