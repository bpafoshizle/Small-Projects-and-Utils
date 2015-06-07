/* Microsoft SQL Server Integration Services Script Component
*  Write scripts using Microsoft Visual C# 2008.
*  ScriptMain is the entry point class of the script.*/

using System;
using System.Data;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System.Data.OleDb;
using System.Windows.Forms;
using System.IO;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    OleDbConnection oledbConn;
    OleDbCommand oledbCmd;
    OleDbParameter productCode;
    OleDbParameter brandCode;
    OleDbParameter loadDate;
    OleDbParameter modDate;
    string oraDate;
    string insertStmnt;
    int rowsAffected;
    int count;


    public override void PreExecute()
    {
        oledbConn = new OleDbConnection(Variables.conTysonDataStaging);
        oledbConn.Open();
        oledbCmd = oledbConn.CreateCommand();
        oraDate = convertToOraDate(Variables.StartTime);
        insertStmnt = buildInsert();
        oledbCmd.CommandText = insertStmnt;
        count = 0;
    }

    public override void PostExecute()
    {
        base.PostExecute();
        oledbConn.Close();
    }

    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        //MessageBox.Show("PROD_CODE: " + Row.PRODCODE + "\nBRND_CODE: " + Row.BRNDCODE);
        oledbCmd.Parameters.Add("@p1", OleDbType.Numeric).Value = Row.PRODCODE;
        oledbCmd.Parameters.Add("@p2", OleDbType.Numeric).Value = Row.BRNDCODE;
        oledbCmd.Parameters.Add("@p3", OleDbType.Char).Value = oraDate;
        oledbCmd.Parameters.Add("@p4", OleDbType.Char).Value = oraDate;

//        writeSqlCmdToFile(oledbCmd.CommandText);
//        MessageBox.Show(Convert.ToString(count));

        try
        {
            //writeSqlCmdToFile(oledbCmd.CommandText);
            rowsAffected = oledbCmd.ExecuteNonQuery();
            //MessageBox.Show(Convert.ToString(rowsAffected));
        }
        catch(Exception ex)
        {
            MessageBox.Show(ex.ToString());
        }

        oledbCmd.Parameters.Clear();
        count++; 
    }

    private void writeSqlCmdToFile(string sqlCmd)
    {
        TextWriter fw = new StreamWriter("C:\\Users\\millerbarr\\Desktop\\sql.txt");
        fw.Write(sqlCmd);
        fw.Close();
    }

    private string convertToOraDate(DateTime dateTime)
    {
        string oraDate = dateTime.Day + monthNumToString(dateTime.Month) + dateTime.Year;
        return oraDate;
    }

    private string monthNumToString(int p)
    {
        switch (p)
        {
            case 1:
                return "JAN";
            case 2:
                return "FEB";
            case 3:
                return "MAR";
            case 4:
                return "APR";
            case 5:
                return "MAY";
            case 6:
                return "JUN";
            case 7:
                return "JUL";
            case 8:
                return "AUG";
            case 9:
                return "SEP";
            case 10:
                return "OCT";
            case 11:
                return "NOV";
            case 12:
                return "DEC";
            default:
                return "AXE";
        }
    }

    private string buildInsert()
    {
        return @"INSERT INTO TDS_OWNER.NON_RESTATED_PRODUCT
			                    (NON_RESTATED_PRODUCT_KEY,
			                    VALID_FROM_DATE,
			                    VALID_TO_DATE,
			                    CURRENT_FLAG,
			                    DIM_CHANGE_CODE,
			                    PRODUCT_CODE,
			                    BRAND_CODE,
			                    PRODUCT_DESCR,
			                    BRAND_DESCR,
			                    BUSINESS_GROUP_CODE,
			                    BUSINESS_GROUP_DESCR,
			                    BUSINESS_DIVISION_CODE,
			                    BUSINESS_DIVISION_DESCR,
			                    SELLING_GROUP_CODE,
			                    SELLING_GROUP_DESCR,
			                    MINOR_LINE_CODE,
			                    MINOR_LINE_DESCR,
			                    MINOR_LINE_KEY,
			                    MATERIAL_NUMBER_VALUE,
			                    MATERIAL_GRP_CODE,
			                    MATERIAL_GRP_DESCR,
			                    PROD_HIER_CODE,
			                    PROD_PROTEIN_CODE,
			                    PROD_PROTEIN_DESCR,
			                    PROD_CAT_CODE,
			                    PROD_CAT_DESCR,
			                    PROD_SUB_CAT_CODE,
			                    PROD_SUB_CAT_DESCR,
			                    BRAND_GRP_CODE,
			                    BRAND_GRP_DESCR,
			                    SAP_DIV_CODE,
			                    SAP_DIV_DESCR,
			                    PRODUCT_GROUP_CODE,
			                    PRODUCT_GROUP_DESCR,
			                    MAJOR_LINE_CODE,
			                    MAJOR_LINE_DESCR,
			                    NON_PRODUCT_FLAG,
			                    PRODUCT_STATUS,
			                    UNIT_COST_AMOUNT,
			                    UNIT_COST_UOM_CODE,
			                    AVG_CASE_WEIGHT_QUANTITY,
			                    AVG_CASE_WEIGHT_UOM_CODE,
			                    STANDARD_UOM_CODE,
			                    CATCH_WEIGHT_IND,
			                    CASE_CUBE_QUANTITY,
			                    CASE_CUBE_UOM_CODE,
			                    TARE_WEIGHT_QUANTITY,
			                    TARE_WEIGHT_UOM,
			                    SECONDS_FLAG,
			                    COST_GROUP_IND,
			                    CREATE_DATE,
			                    MOD_DATE,
			                    MOD_USERNAME,
			                    SHELF_LIFE,
			                    CATEGORY_CODE,
			                    BROKER_RATE,
			                    BROKER_UOM,
			                    OTHER_SELLING_FCTR_LEVEL,
			                    SELLING_FACTOR,
			                    CASE_UPC_CODE,
			                    CASE_PER_PALLET,
			                    UPC_LABEL_FLAG,
			                    TIERS_PER_PALLET,
			                    DATE_CODE_IND,
			                    DYNAMIC_WEIGHTING_FACTOR,
			                    WEIGHTING_FACTOR_IND,
			                    STANDARD_PACKAGE_COST,
			                    LAST_COST_UPD_DATE,
			                    MARKET_COST_FLAG,
			                    SPEC_REQUIRED_FLAG,
			                    PCS_FLAG,
			                    PCS_BRAND,
			                    SPEC_EFFECTIVE_DATE,
			                    SPEC_MOD_USERNAME,
			                    LAYERED_PRODUCT_FLAG,
			                    LAYERED_PRODUCT_START_DATE,
			                    LAYERED_PRODUCT_MOD_DATE,
			                    LAYERED_PRODUCT_MOD_USRNM,
			                    SHIP_BY_DAYS,
			                    AGING_DAYS,
			                    TRANSFER_PRICE_METHOD_FLAG,
			                    LOAD_SIM_DAY_KEY,
			                    LOAD_SIM_TIME_KEY,
			                    SUPPLY_PLANNER,
			                    PRODUCT_PROCESS,
			                    PRODUCT_PROCESS_TYPE_DESCR,
			                    PROCESS_VALUE_CLASS_DESCR,
			                    PRODUCT_PART,
			                    PRODUCT_PART_HALF_DESCR,
			                    PRODUCT_PART_PRIMAL_DESCR,
			                    PRODUCT_SUB_PRIMAL_DESCR,
			                    LOAD_PPS_DAY_KEY,
			                    LOAD_PPS_TIME_KEY,
			                    VALUE_ENHANCEMENT,
			                    PRODUCT_PROCESS_LEVEL,
			                    UPGRADE_TARGET,
			                    INTER_DIVISION_CLASS,
			                    POUNDS_PER_PALLET,
			                    PIECE_COUNT_PER_CASE,
			                    HEAD_COUNT_PER_CASE,
			                    FORECAST_CONTACT,
			                    PRODUCE_TO_ORDER_FLAG,
			                    SAP_PRODUCT_BRAND_CODE,
			                    PORTION_TYPE,
			                    PORTION_SIZE,
			                    PORTION_CUT_TYPE,
			                    FRESH_PRODUCT_CATEGORY_CODE,
			                    FRESH_PRODUCT_CATEGORY_DESCR,
			                    BUSINESS_SEGMENT_CODE,
			                    BUSINESS_SEGMENT_DESCR,
			                    MOD_DAY_KEY,
			                    MOD_TIME_KEY,
			                    INTL_PROD_GRP_NUM,
			                    INTL_PROD_GRP_DESCR,
			                    INTL_SUPER_CAT_CODE,
			                    INTL_SUPER_CAT_DESCR,
			                    INTL_CAT_CODE,
			                    INTL_CAT_DESCR,
			                    INNER_PACK_AMT,
			                    INNER_PACK_WEIGHT,
			                    INNER_PACK_WEIGHT_UOM,
			                    XFER_PER_LB_MKT_COST_AMT,
			                    XFER_PER_CS_MKT_COST_AMT,
			                    COOL_CODE,
			                    COOL_CODE_DESCR)
                    VALUES
		                    (TDS_OWNER.DIMENSION_SEQ.NEXTVAL,
		                    TO_DATE('01-JAN-1970', 'DD-MON-YYYY'),
		                    TO_DATE('31-DEC-9999', 'DD-MON-YYYY'),
		                    'Y',
		                    'UNKNOWN',
		                    ?,
		                    ?,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    0,
		                    'UNKNOWN',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKN',
		                    'UNKNOWN - FROM DAS',
		                    'UNK',
		                    'UNKNOWN - FROM DAS',
		                    'UN',
		                    'UNKNOWN - FROM DAS',
		                    'UN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    ' ',
		                    ' ',
		                    0,
		                    'UNKNOWN',
		                    0,
		                    'UNKNOWN',
		                    'UNKNOWN',
		                    ' ',
		                    0,
		                    'UNKNOWN',
		                    0,
		                    'UNKNOWN',
		                    ' ',
		                    ' ',
		                    TO_DATE(?, 'DD-MON-YYYY'),
		                    TO_DATE(?, 'DD-MON-YYYY'),
		                    'UNKNOWN - FROM DAS',
		                    0,
		                    'UNKNOWN',
		                    0,
		                    'UNKNOWN',
		                    ' ',
		                    0,
		                    'UNKNOWN',
		                    0,
		                    ' ',
		                    'UNKNOWN',
		                    ' ',
		                    0,
		                    ' ',
		                    0,
		                    TO_DATE('01-JAN-1970', 'DD-MON-YYYY'),
		                    ' ',
		                    ' ',
		                    ' ',
		                    0,
		                    TO_DATE('01-JAN-1970', 'DD-MON-YYYY'),
		                    'UNKNOWN',
		                    ' ',
		                    TO_DATE('01-JAN-1970', 'DD-MON-YYYY'),
		                    TO_DATE('01-JAN-1970', 'DD-MON-YYYY'),
		                    'UNKNOWN',
		                    0,
		                    0,
		                    ' ',
		                    1,
		                    1,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    1,
		                    1,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS',
		                    0,
		                    0,
		                    0,
		                    'UNKNOWN - FROM DAS',
		                    ' ',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    0,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    1,
		                    1,
		                    0,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN',
		                    'UNKNOWN - FROM DAS',
		                    0,
		                    'UNKNOWN',
		                    'UNKNOWN',
		                    0,
		                    0,
		                    'UNKNOWN - FROM DAS',
		                    'UNKNOWN - FROM DAS')";
    }

}