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
        bool type1Change = false;
        bool type2Change = false;
        bool fireAgain = true;

        /* Rows directed to Update output */
        if (!compareStrings(Row.Type1Hash, Row.TYPE1HASHDIM))
        {

            /* Debugging Stuff */
            if (Variables.blnDebug)
            {
                ComponentMetaData.FireInformation(0, "Separate Updates and Inserts", "Hash1 changed to: [" + Row.Type1Hash + "] from: [" + Row.TYPE1HASHDIM + "]", "", 0, ref fireAgain);
            }

            type1Change = true;
            Row.DirectRowToType1();

            /* Rows directed to Historical Insert output */
            if (!compareStrings(Row.Type2Hash, Row.TYPE2HASHDIM))
            {
                // Debugging
                if (Variables.blnDebug)
                {
                    ComponentMetaData.FireInformation(0, "Separate Updates and Inserts", "Hash2 changed to: [" + Row.Type2Hash + "] from: [" + Row.TYPE2HASHDIM + "]", "", 0, ref fireAgain);
                }
                type2Change = true;
                Row.DirectRowToType2();
            }

            /* Rows directed to unchanged output */
            if (!type1Change && !type2Change)
            {
                Row.DirectRowToUnchanged();
            }
        }
    }

     /*
     * Function to compare two strings and return success if both are either 
     * null or empty
     */
    bool compareStrings(string a, string b)
    {
        if (string.IsNullOrEmpty(a) && string.IsNullOrEmpty(b))
        {
            return true;
        }
        else
        {
            return a == b;
        }
    }
}
