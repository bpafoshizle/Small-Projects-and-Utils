using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Tyson;

namespace Active_Directory_Helper
{
   public partial class Form1 : Form
   {
      public Form1()
      {
         InitializeComponent();
      }

      private void button1_Click(object sender, EventArgs e)
      {
         Tyson.DirectoryServices.AdGroup adGroup;
         String[] adGroupNames = new String[] { "BI_OLAP_ALL",
                                                "BI_OLAP_ALL Partial",
                                                "BI_OLAP_BF_Consumer Products",
                                                "BI_OLAP_BF_Consumer Products Partial",
                                                "BI_OLAP_BF_Food Service",
                                                "BI_OLAP_BF_Food Service Partial",
                                                "BI_OLAP_BF_McDonalds",
                                                "BI_OLAP_BF_McDonalds Partial",
                                                "BI_OLAP_BF_Renewable Products",
                                                "BI_OLAP_BF_Renewable Products Partial",
                                                "BI_OLAP_BF_Supply Chain",
                                                "BI_OLAP_BF_Supply Chain Partial",
                                                "BI_OLAP_SG_Cobb Vantress",
                                                "BI_OLAP_SG_Cobb Vantress Partial",
                                                "BI_OLAP_SG_Consumer Products",
                                                "BI_OLAP_SG_Consumer Products Partial",
                                                "BI_OLAP_SG_Food Service",
                                                "BI_OLAP_SG_Food Service Partial",
                                                "BI_OLAP_SG_Fresh Meats",
                                                "BI_OLAP_SG_Fresh Meats Partial",
                                                "BI_OLAP_SG_International",
                                                "BI_OLAP_SG_International Partial",
                                                "BI_OLAP_SG_McDonalds",
                                                "BI_OLAP_SG_McDonalds Partial",
                                                "BI_OLAP_SG_Renewable Products",
                                                "BI_OLAP_SG_Renewable Products Partial"
                                                 };

         DataTable dt = new DataTable();
         dt.Columns.Add("AD Group Name", typeof(String));
         dt.Columns.Add("AD Account", typeof(String));
         dt.Columns.Add("AD Full Name", typeof(String));

         //* Build DataTable for distinct list and bind.
         DataTable dt2 = new DataTable();
         dt2.Columns.Add("AD Account", typeof(String));
         dt2.Columns.Add("AD Full Name", typeof(String));

         System.Collections.Generic.List<String> list = new List<string>();

         foreach (String adGroupName in adGroupNames)
         {
            adGroup = new Tyson.DirectoryServices.AdGroup("tysonet", adGroupName);

            foreach (Tyson.DirectoryServices.AdUser adUser in adGroup.Users)
            {
               if (list.Contains(adUser.NtUserAccount) == false)
               {
                  list.Add(adUser.NtUserAccount);
                  dt2.Rows.Add(new String[] { adUser.NtUserAccount, adUser.FullName });
               }

               dt.Rows.Add(new String[] { adGroupName, adUser.NtUserAccount, adUser.FullName });
               //System.Diagnostics.Debug.Print(adGroupName + " " + NtUserAccount);

            }
         }

         this.dgvAdMembers.DataSource = dt;
         this.dgvDistinctMembers.DataSource = dt2;

         return;
      }
   }
}