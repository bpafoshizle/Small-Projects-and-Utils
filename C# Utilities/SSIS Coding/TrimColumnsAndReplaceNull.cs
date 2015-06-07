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
    }

    public override void ProcessInput(int InputID, Microsoft.SqlServer.Dts.Pipeline.PipelineBuffer Buffer)
    {
        string cleanedColumnValue;
        while (Buffer.NextRow())
        {
            // Loop through all the columns in the buffer
            for (int i = 0; i < Buffer.ColumnCount; i++ )
            {
                
                if (!Buffer.IsNull(i))
                {
                    // All columns should be strings coming from a file, so setString and getString are all that is needed.
                    cleanedColumnValue = cleanColumnString(Buffer.GetString(i));
                    Buffer.SetString(i, cleanedColumnValue);
                }
            }
        }

        base.ProcessInput(InputID, Buffer);
    }

    // Return trimmed and NULL-replaced string
    private string cleanColumnString(string p)
    {
        return replaceNULL(p.Trim());
    }

    // Replace "NULL" with ""
    private string replaceNULL(string p)
    {
        if (p.Equals("NULL"))
            return "";
        else
            return p;
    }
}
