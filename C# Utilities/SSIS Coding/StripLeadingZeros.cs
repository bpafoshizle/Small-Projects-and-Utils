/* Microsoft SQL Server Integration Services Script Component
*  Write scripts using Microsoft Visual C# 2008.
*  ScriptMain is the entry point class of the script.*/

using System;
using System.Data;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    long num;

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

    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        /*
          Add your code here
        */
        if(isNumeric(Row.DAHDRSHIPTONUM, out num))
        {
            Row.DAHDRSHIPTONUM = num.ToString();
        }
        if(isNumeric(Row.PRODDTLPRODCODE, out num))
        {
            Row.PRODDTLPRODCODE = num.ToString();
        }
        if(isNumeric(Row.PRODDTLBRNDCODE, out num))
        {
            Row.PRODDTLBRNDCODE = num.ToString();
        }
        if(isNumeric(Row.DAHDRPRIMBRKRCODE, out num))
        {
            Row.DAHDRPRIMBRKRCODE = num.ToString();
        }
        if(isNumeric(Row.DAHDRSALESMANCODE, out num))
        {
            Row.DAHDRSALESMANCODE = num.ToString();
        }
        if(isNumeric(Row.DAHDRBILLTONUM, out num))
        {
            Row.DAHDRBILLTONUM = num.ToString();
        }
    }


    /* 
     * Method to test whether the input string is numeric and to pass 
     * back the numeric value of the string if it is numeric. 
     * This pass-by-reference approach is intended to prevent having
     * to convert the string twice, because that is expensive.
     */
    private bool isNumeric(string s, out long num)
    {
        try
        {
            num = Convert.ToInt64(s);
            return true;
        }
        catch
        {
            num = -1;
            return false;
        }
    }

}
