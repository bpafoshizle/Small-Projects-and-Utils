namespace Active_Directory_Helper
{
   partial class Form1
   {
      /// <summary>
      /// Required designer variable.
      /// </summary>
      private System.ComponentModel.IContainer components = null;

      /// <summary>
      /// Clean up any resources being used.
      /// </summary>
      /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
      protected override void Dispose(bool disposing)
      {
         if (disposing && (components != null))
         {
            components.Dispose();
         }
         base.Dispose(disposing);
      }

      #region Windows Form Designer generated code

      /// <summary>
      /// Required method for Designer support - do not modify
      /// the contents of this method with the code editor.
      /// </summary>
      private void InitializeComponent()
      {
         this.button1 = new System.Windows.Forms.Button();
         this.dgvAdMembers = new System.Windows.Forms.DataGridView();
         this.tabControl1 = new System.Windows.Forms.TabControl();
         this.tabPage1 = new System.Windows.Forms.TabPage();
         this.tabPage2 = new System.Windows.Forms.TabPage();
         this.dgvDistinctMembers = new System.Windows.Forms.DataGridView();
         ((System.ComponentModel.ISupportInitialize)(this.dgvAdMembers)).BeginInit();
         this.tabControl1.SuspendLayout();
         this.tabPage1.SuspendLayout();
         this.tabPage2.SuspendLayout();
         ((System.ComponentModel.ISupportInitialize)(this.dgvDistinctMembers)).BeginInit();
         this.SuspendLayout();
         // 
         // button1
         // 
         this.button1.Location = new System.Drawing.Point(829, 445);
         this.button1.Name = "button1";
         this.button1.Size = new System.Drawing.Size(125, 23);
         this.button1.TabIndex = 0;
         this.button1.Text = "Get AD Members";
         this.button1.UseVisualStyleBackColor = true;
         this.button1.Click += new System.EventHandler(this.button1_Click);
         // 
         // dgvAdMembers
         // 
         this.dgvAdMembers.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
         this.dgvAdMembers.Location = new System.Drawing.Point(6, 6);
         this.dgvAdMembers.Name = "dgvAdMembers";
         this.dgvAdMembers.Size = new System.Drawing.Size(922, 388);
         this.dgvAdMembers.TabIndex = 1;
         // 
         // tabControl1
         // 
         this.tabControl1.Controls.Add(this.tabPage1);
         this.tabControl1.Controls.Add(this.tabPage2);
         this.tabControl1.Location = new System.Drawing.Point(12, 13);
         this.tabControl1.Name = "tabControl1";
         this.tabControl1.SelectedIndex = 0;
         this.tabControl1.Size = new System.Drawing.Size(942, 426);
         this.tabControl1.TabIndex = 2;
         // 
         // tabPage1
         // 
         this.tabPage1.Controls.Add(this.dgvAdMembers);
         this.tabPage1.Location = new System.Drawing.Point(4, 22);
         this.tabPage1.Name = "tabPage1";
         this.tabPage1.Padding = new System.Windows.Forms.Padding(3);
         this.tabPage1.Size = new System.Drawing.Size(934, 400);
         this.tabPage1.TabIndex = 0;
         this.tabPage1.Text = "Role Members";
         this.tabPage1.UseVisualStyleBackColor = true;
         // 
         // tabPage2
         // 
         this.tabPage2.Controls.Add(this.dgvDistinctMembers);
         this.tabPage2.Location = new System.Drawing.Point(4, 22);
         this.tabPage2.Name = "tabPage2";
         this.tabPage2.Padding = new System.Windows.Forms.Padding(3);
         this.tabPage2.Size = new System.Drawing.Size(934, 400);
         this.tabPage2.TabIndex = 1;
         this.tabPage2.Text = "Distinct Members";
         this.tabPage2.UseVisualStyleBackColor = true;
         // 
         // dgvDistinctMembers
         // 
         this.dgvDistinctMembers.AllowUserToAddRows = false;
         this.dgvDistinctMembers.AllowUserToDeleteRows = false;
         this.dgvDistinctMembers.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
         this.dgvDistinctMembers.Location = new System.Drawing.Point(6, 6);
         this.dgvDistinctMembers.Name = "dgvDistinctMembers";
         this.dgvDistinctMembers.ReadOnly = true;
         this.dgvDistinctMembers.Size = new System.Drawing.Size(922, 388);
         this.dgvDistinctMembers.TabIndex = 0;
         // 
         // Form1
         // 
         this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
         this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
         this.ClientSize = new System.Drawing.Size(966, 480);
         this.Controls.Add(this.tabControl1);
         this.Controls.Add(this.button1);
         this.Name = "Form1";
         this.Text = "Form1";
         ((System.ComponentModel.ISupportInitialize)(this.dgvAdMembers)).EndInit();
         this.tabControl1.ResumeLayout(false);
         this.tabPage1.ResumeLayout(false);
         this.tabPage2.ResumeLayout(false);
         ((System.ComponentModel.ISupportInitialize)(this.dgvDistinctMembers)).EndInit();
         this.ResumeLayout(false);

      }

      #endregion

      private System.Windows.Forms.Button button1;
      private System.Windows.Forms.DataGridView dgvAdMembers;
      private System.Windows.Forms.TabControl tabControl1;
      private System.Windows.Forms.TabPage tabPage1;
      private System.Windows.Forms.TabPage tabPage2;
      private System.Windows.Forms.DataGridView dgvDistinctMembers;
   }
}

