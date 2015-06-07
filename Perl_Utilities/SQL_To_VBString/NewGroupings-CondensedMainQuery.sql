                 SELECT   
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,
                 CASE WHEN (md.FISCAL_MONTH_NUMBER = (:p_Month))THEN SUM (epf.expense_amt)  
                 END AS monthexpense_amt,  
                 0 AS monthpounds,  
                 0 AS monthnetreceivables,  
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		 mpbd.business_division_code,  
                 INITCAP (mpbd.business_division_descr) AS businessdivisiondescr,  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 SUM (epf.expense_amt) AS expense_amt,  
                 0 AS total_pounds_shipped,  
                 0 AS net_receivable_dollar  
                 FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd,  
                 tdw_owner.expense_payment_fact epf,  
                 tdw_owner.fsv_spend_category sc,  
                 tdw_owner.restated_market m,  
                 tdw_owner.month_dim md,  
                 tdw_owner.restated_product p,  
                 tdw_owner.acct_dim a,  
                 tdw_owner.expense_type et,  
                 tdw_owner.spend_request_dim s,  
                 tdw_owner.operator_dim o,  
                 tdw_owner.day_dim d1,  
                 tdw_owner.day_dim d2,  
                 tdw_owner.expense_attribute_dim ead,  
                 tdw_owner.expense_payee_dim pay,  
                 tdw_owner.group_detail gd,  
                 tdw_owner.group_description groupdescr  
                 WHERE mpbd.restated_mkt_prd_bus_div_key = epf.restated_mkt_prd_bus_div_key  
                 AND epf.expense_type_key = sc.expense_type_key  
                 AND epf.acct_key = sc.acct_key  
                 AND epf.restated_market_key = m.restated_market_key  
                 AND epf.month_key = md.month_key  
                 AND epf.restated_product_key = p.restated_product_key  
                 AND epf.acct_key = a.acct_key  
                 AND epf.expense_type_key = et.expense_type_key  
                 AND epf.spend_request_key = s.spend_request_key  
                 AND epf.operator_key = o.operator_key  
                 AND epf.report_period_begin_day_key = d1.day_key  
                 AND epf.report_period_end_day_key = d2.day_key  
                 AND epf.expense_attribute_key = ead.expense_attribute_key  
                 AND epf.expense_payee_key = pay.expense_payee_key  
                 AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                 AND gd.group_field_c  =  m.DIVISION_CODE  
                 AND sc.spend_base_seq <> 0  
                 AND sc.spend_subcategory_seq <> 0  
                 AND (mpbd.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11' ,'F12' ,'F15'))  
                 AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                 AND (md.year_descr IN (:p_year))  
                 and md.fiscal_month_number <= :p_Month  
                 AND (mpbd.business_division_code IN (:p_businessdivision))  
                 AND (m.division_code IN (:p_salesdivision))  
                 AND (m.region_code IN (:p_salesregion))  
                 AND (m.primary_broker_code IN (:p_primarybroker))  
                 AND gd.GROUP_CODE in (:vp)  
                 GROUP BY
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		 mpbd.business_division_code,  
                 INITCAP (mpbd.business_division_descr),  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 md.FISCAL_MONTH_NUMBER,  
                 mpbd.business_division_Descr  
				 
                 /*-Add Expense Total Value Added*/  
                 UNION  ALL
                 SELECT     
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,  
                 CASE WHEN (md.FISCAL_MONTH_NUMBER = (:p_Month))THEN SUM (epf.expense_amt)  
                 END AS monthexpense_amt,  
                 0 AS monthpounds,  
                 0 AS monthnetreceivables,  
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		 mpbd.business_division_code,  
                case when (mpbd.business_division_code IN ('F01', 'F08', 'F15'))Then 'Total Value Added'   
                      when (mpbd.business_division_code IN ('F03', 'F04'))Then 'Total Market/CVP'
					  when (mpbd.business_division_code IN ('F11', 'F12'))Then 'Government Sales Total'
                      End AS businessdivisiondescr,  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 SUM (epf.expense_amt) AS expense_amt,  
                 0 AS total_pounds_shipped,  
                 0 AS net_receivable_dollar  
                 FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd,  
                 tdw_owner.expense_payment_fact epf,  
                 tdw_owner.fsv_spend_category sc,  
                 tdw_owner.restated_market m,  
                 tdw_owner.month_dim md,  
                 tdw_owner.restated_product p,  
                 tdw_owner.acct_dim a,  
                 tdw_owner.expense_type et,  
                 tdw_owner.spend_request_dim s,  
                 tdw_owner.operator_dim o,  
                 tdw_owner.day_dim d1,  
                 tdw_owner.day_dim d2,  
                 tdw_owner.expense_attribute_dim ead,  
                 tdw_owner.expense_payee_dim pay,  
                 tdw_owner.group_detail gd,  
                 tdw_owner.group_description groupdescr  
                 WHERE mpbd.restated_mkt_prd_bus_div_key = epf.restated_mkt_prd_bus_div_key  
                 AND epf.expense_type_key = sc.expense_type_key  
                 AND epf.acct_key = sc.acct_key  
                 AND epf.restated_market_key = m.restated_market_key  
                 AND epf.month_key = md.month_key  
                 AND epf.restated_product_key = p.restated_product_key  
                 AND epf.acct_key = a.acct_key  
                 AND epf.expense_type_key = et.expense_type_key  
                 AND epf.spend_request_key = s.spend_request_key  
                 AND epf.operator_key = o.operator_key  
                 AND epf.report_period_begin_day_key = d1.day_key  
                 AND epf.report_period_end_day_key = d2.day_key  
                 AND epf.expense_attribute_key = ead.expense_attribute_key  
                 AND epf.expense_payee_key = pay.expense_payee_key  
                 AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                 AND gd.group_field_c  =  m.DIVISION_CODE  
                 AND sc.spend_base_seq <> 0  
                 AND sc.spend_subcategory_seq <> 0  
				 AND (mpbd.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11' ,'F12' ,'F15')) 
                 AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                 AND (md.year_descr IN (:p_year))  
                 and md.fiscal_month_number <= :p_Month  
                 AND (mpbd.business_division_code IN (:p_businessdivision))  
                 AND (m.division_code IN (:p_salesdivision))  
                 AND (m.region_code IN (:p_salesregion))  
                 AND (m.primary_broker_code IN (:p_primarybroker))  
                 AND gd.GROUP_CODE in (:vp)  
                 GROUP BY
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		 mpbd.business_division_code,  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 md.FISCAL_MONTH_NUMBER,  
                 mpbd.business_division_Descr  
       
                 UNION  ALL
                 SELECT     
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,  
                 CASE WHEN (md.FISCAL_MONTH_NUMBER = (:p_Month))THEN SUM (epf.expense_amt)  
                 END AS monthexpense_amt,  
                 0 AS monthpounds,  
                 0 AS monthnetreceivables,         		
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		          mpbd.business_division_code,  
                 case when (mpbd.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))Then 'Total Customer'   
                      End AS businessdivisiondescr,  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 SUM (epf.expense_amt) AS expense_amt,  
                 0 AS total_pounds_shipped,  
                 0 AS net_receivable_dollar  
                 FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd,  
                 tdw_owner.expense_payment_fact epf,  
                 tdw_owner.fsv_spend_category sc,  
                 tdw_owner.restated_market m,  
                 tdw_owner.month_dim md,  
                 tdw_owner.restated_product p,  
                 tdw_owner.acct_dim a,  
                 tdw_owner.expense_type et,  
                 tdw_owner.spend_request_dim s,  
                 tdw_owner.operator_dim o,  
                 tdw_owner.day_dim d1,  
                 tdw_owner.day_dim d2,  
                 tdw_owner.expense_attribute_dim ead,  
                 tdw_owner.expense_payee_dim pay,  
                 tdw_owner.group_detail gd,  
                 tdw_owner.group_description groupdescr  
                 WHERE mpbd.restated_mkt_prd_bus_div_key = epf.restated_mkt_prd_bus_div_key  
                 AND epf.expense_type_key = sc.expense_type_key  
                 AND epf.acct_key = sc.acct_key  
                 AND epf.restated_market_key = m.restated_market_key  
                 AND epf.month_key = md.month_key  
                 AND epf.restated_product_key = p.restated_product_key  
                 AND epf.acct_key = a.acct_key  
                 AND epf.expense_type_key = et.expense_type_key  
                 AND epf.spend_request_key = s.spend_request_key  
                 AND epf.operator_key = o.operator_key  
                 AND epf.report_period_begin_day_key = d1.day_key  
                 AND epf.report_period_end_day_key = d2.day_key  
                 AND epf.expense_attribute_key = ead.expense_attribute_key  
                 AND epf.expense_payee_key = pay.expense_payee_key  
                 AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                 AND gd.group_field_c  =  m.DIVISION_CODE  
                 AND sc.spend_base_seq <> 0  
                 AND sc.spend_subcategory_seq <> 0  
				 AND (mpbd.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15')) 
                 AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                 AND (md.year_descr IN (:p_year))  
                 and md.fiscal_month_number <= :p_Month  
                 AND (mpbd.business_division_code IN (:p_businessdivision))  
                 AND (m.division_code IN (:p_salesdivision))  
                 AND (m.region_code IN (:p_salesregion))  
                 AND (m.primary_broker_code IN (:p_primarybroker))  
                 AND gd.GROUP_CODE in (:vp)  
                 GROUP BY
                 sc.DEPT_LEVEL,
                 sc.DEPT_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL_SEQ,
                 sc.CUSTOMER_LEVEL,
                 sc.MANAGEMENT_LEVEL,
                 sc.MANAGEMENT_LEVEL_SEQ,
				 gd.GROUP_CODE, 
				 groupdescr.GROUP_DESCR, 
				 m.DIVISION_CODE, 
				 m.DIVISION_DESCR, 
				 m.REGION_CODE, 
				 m.REGION_DESCR, 
        		 mpbd.business_division_code,  
                 sc.spend_base_descr,  
                 sc.spend_base_seq,  
                 sc.spend_subcategory_descr,  
                 sc.spend_subcategory_seq,  
                 md.year_descr,  
                 md.month_descr,  
                 md.FISCAL_MONTH_NUMBER,  
                 mpbd.business_division_Descr  
         
        /*Begin Volume*/  
		UNION  ALL
			SELECT
            'zzz' AS DEPT_LEVEL,
            999 AS DEPT_LEVEL_SEQ,
            999 AS CUSTOMER_LEVEL_SEQ,
            'zzz' AS CUSTOMER_LEVEL,
            'zzz' AS MANAGEMENT_LEVEL,
            999 AS MANAGEMENT_LEVEL_SEQ,
            0 AS monthexpense_amt,  
            CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                 THEN SUM (  ids_mf.mf_pounds_shipped  
                             + ids_mf.ids_pounds_shipped)  
            END AS monthpounds,  
            CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                 THEN SUM (  ids_mf.mf_gross_sales_amount  
                           - ids_mf.mf_allow_amount  
                           - ids_mf.mf_off_invoice_amount  
                           + ids_mf.mf_revenue_adjustment                        
                           + ids_mf.ids_indirect_sales_amount)  
            END AS monthnetreceivables,  
            ids_mf.GROUP_CODE, 
            ids_mf.GROUP_DESCR, 
            ids_mf.DIVISION_CODE, 
            ids_mf.DIVISION_DESCR, 
            ids_mf.REGION_CODE, 
            ids_mf.REGION_DESCR, 
        	ids_mf.business_division_code,  
            INITCAP (ids_mf.business_division_descr) AS businessdivisiondescr,  
            NULL AS spend_base_descr,  
            NULL AS spend_base_seq,  
            NULL AS spend_subcategory_descr,  
            NULL AS spend_subcategory_seq,  
            ids_mf.year_descr,  
            ids_mf.month_descr,  
            0 AS expense_amt,  
            SUM (  ids_mf.mf_pounds_shipped  
                   + ids_mf.ids_pounds_shipped  
                   ) AS total_pounds_shipped,  
            SUM (  ids_mf.mf_gross_sales_amount  
                   - ids_mf.mf_allow_amount  
                   - ids_mf.mf_off_invoice_amount  
                   + ids_mf.mf_revenue_adjustment  
                   + ids_mf.ids_indirect_sales_amount  
                   ) AS net_receivable_dollar  
            FROM (
				SELECT   mpbd1.business_division_code,  
                mpbd1.business_division_descr,  
                gd.group_code,
                groupdescr.GROUP_DESCR,
                m1.DIVISION_CODE, 
                m1.DIVISION_DESCR, 
                m1.REGION_CODE, 
                m1.REGION_DESCR,
                wd1.year_descr,
                wd1.month_descr,
                wd1.FISCAL_MONTH_NUMBER,
                mf.pounds_shipped AS mf_pounds_shipped,  
                mf.gross_sales_amount AS mf_gross_sales_amount,  
                mf.allow_amount AS mf_allow_amount,  
                mf.gross_sales_variance AS mf_revenue_adjustment, 
                mf.off_invoice_amount AS mf_off_invoice_amount,  
                0 AS ids_pounds_shipped,  
                0 AS ids_indirect_sales_amount  
                FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                tdw_owner.restated_market m1,  
                tdw_owner.restated_product p1,  
                tdw_owner.ta_margin_fact_view mf,
                tdw_owner.week_dim wd1, 
                tdw_owner.group_detail gd,  
                tdw_owner.group_description groupdescr  
                WHERE mf.restated_market_key = m1.restated_market_key  
                AND mf.restated_product_key = p1.restated_product_key  
                AND mf.restated_market_key = mpbd1.restated_market_key  
                AND mf.restated_product_key = mpbd1.restated_product_key  
                AND To_DATE(mf.week_key,'YYYYMMDD')  = wd1.week_ending_date
                AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                AND gd.group_field_c  =  m1.DIVISION_CODE  
                AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))  
                AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))   
                AND (wd1.YEAR_DESCR IN (:p_year))
                AND wd1.FISCAL_MONTH_NUMBER <= :p_Month 
                AND (mpbd1.business_division_code IN (:p_businessdivision))  
                AND (m1.division_code IN (:p_salesdivision))  
                AND (m1.region_code IN (:p_salesregion))  
                AND (m1.primary_broker_code IN (:p_primarybroker))  
                AND gd.GROUP_CODE in (:vp)  
                 
                UNION  ALL
                
                SELECT 
                mpbd1.business_division_code,  
                mpbd1.business_division_descr,  
                gd.group_code,
                groupdescr.GROUP_DESCR,
                m1.DIVISION_CODE, 
                m1.DIVISION_DESCR, 
                m1.REGION_CODE, 
                m1.REGION_DESCR,
                md1.year_descr,  
                md1.month_descr,  
                md1.FISCAL_MONTH_NUMBER,  
                0 AS mf_pounds_shipped,  
                0 AS mf_gross_sales_amount,  
                0 AS mf_allow_amount,  
                0 AS mf_revenue_adjustment,  
                0 AS mf_off_invoice_amount,  
                ids.pounds_shipped AS ids_pounds_shipped,  
                ids.indirect_sales_amount AS ids_indirect_sales_amount  
                FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                tdw_owner.restated_market m1,  
                tdw_owner.month_dim md1,  
                tdw_owner.restated_product p1,  
                tdw_owner.indirect_sales ids,  
                tdw_owner.week_dim w1,  
                tdw_owner.group_detail gd,  
                tdw_owner.group_description groupdescr  
                WHERE ids.restated_market_key = m1.restated_market_key  
                AND ids.restated_product_key = p1.restated_product_key  
                AND ids.restated_market_key = mpbd1.restated_market_key  
                AND ids.restated_product_key = mpbd1.restated_product_key  
                AND ids.week_key = w1.week_key  
                AND w1.month_ending_date = md1.month_ending_date  
                AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                AND gd.group_field_c  =  m1.DIVISION_CODE  
                AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))  
                AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                AND (md1.year_descr IN (:p_year))  
                AND md1.fiscal_month_number <= :p_Month  
                AND (mpbd1.business_division_code IN (:p_businessdivision))  
                AND (m1.division_code IN (:p_salesdivision))  
                AND (m1.region_code IN (:p_salesregion))  
                AND (m1.primary_broker_code IN (:p_primarybroker))  
                AND gd.GROUP_CODE in (:vp)  
                )/*end sub query*/  
		    ids_mf
			GROUP BY
			1,
			2,
			3,
			4,
			5,
			6,
			ids_mf.GROUP_CODE, 
			ids_mf.GROUP_DESCR, 
			ids_mf.DIVISION_CODE, 
			ids_mf.DIVISION_DESCR, 
			ids_mf.REGION_CODE, 
			ids_mf.REGION_DESCR, 
			ids_mf.business_division_code,  
			INITCAP (ids_mf.business_division_descr),  
			ids_mf.year_descr,  
			ids_mf.month_descr,  
			ids_mf.FISCAL_MONTH_NUMBER,
			ids_mf.business_division_Descr
                 
                /*Begin Volume 'Total Value Added'*/  
		  UNION  ALL
                                SELECT     
                'zzz' AS DEPT_LEVEL,
                999 AS DEPT_LEVEL_SEQ,
                999 AS CUSTOMER_LEVEL_SEQ,
                'zzz' AS CUSTOMER_LEVEL,
                'zzz' AS MANAGEMENT_LEVEL,
                999 AS MANAGEMENT_LEVEL_SEQ,  
                0 AS monthexpense_amt,  
                CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                     THEN SUM (  ids_mf.mf_pounds_shipped  
                               + ids_mf.ids_pounds_shipped)  
                END AS monthpounds,  
                CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                     THEN SUM (  ids_mf.mf_gross_sales_amount  
                               - ids_mf.mf_allow_amount  
                               - ids_mf.mf_off_invoice_amount  
                                 + ids_mf.mf_revenue_adjustment  
                               + ids_mf.ids_indirect_sales_amount)  
                END AS monthnetreceivables,  
                ids_mf.GROUP_CODE, 
                ids_mf.GROUP_DESCR, 
                ids_mf.DIVISION_CODE, 
                ids_mf.DIVISION_DESCR, 
                ids_mf.REGION_CODE, 
                ids_mf.REGION_DESCR, 
                ids_mf.business_division_code,  
                case when (ids_mf.business_division_code IN ('F01', 'F08', 'F15'))Then 'Total Value Added'   
                         when (ids_mf.business_division_code IN ('F03', 'F04'))Then 'Total Market/CVP'
                         when (ids_mf.business_division_code IN ('F11', 'F12'))Then 'Government Sales Total'
                         End AS businessdivisiondescr,  
                NULL AS spend_base_descr,  
                NULL AS spend_base_seq,  
                NULL AS spend_subcategory_descr,  
                NULL AS spend_subcategory_seq,  
                ids_mf.year_descr,  
                ids_mf.month_descr,  
                0 AS expense_amt,  
                 SUM (  ids_mf.mf_pounds_shipped  
                       + ids_mf.ids_pounds_shipped  
                       ) AS total_pounds_shipped,  
                SUM (  ids_mf.mf_gross_sales_amount  
                       - ids_mf.mf_allow_amount  
                       - ids_mf.mf_off_invoice_amount  
                       + ids_mf.mf_revenue_adjustment  
                       + ids_mf.ids_indirect_sales_amount  
                       ) AS net_receivable_dollar  
                FROM (
                    SELECT   
                    mpbd1.business_division_code,  
                    mpbd1.business_division_descr,  
                    gd.group_code,
                    groupdescr.group_descr,
                    m1.DIVISION_CODE,
                    m1.DIVISION_DESCR,
                    m1.REGION_CODE,
                    m1.REGION_DESCR,
                    wd1.year_descr, 
                    wd1.month_descr, 
                    wd1.FISCAL_MONTH_NUMBER,
                    mf.pounds_shipped AS mf_pounds_shipped,   
                    mf.gross_sales_amount AS mf_gross_sales_amount,  
                    mf.allow_amount AS mf_allow_amount,  
                    mf.gross_sales_variance AS mf_revenue_adjustment,
                    mf.off_invoice_amount AS mf_off_invoice_amount,  
                    0 AS ids_pounds_shipped,  
                    0 AS ids_indirect_sales_amount  
                    FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                    tdw_owner.restated_market m1,  
                    tdw_owner.restated_product p1,  
                    tdw_owner.ta_margin_fact_view mf,
                    tdw_owner.week_dim wd1, 
                    tdw_owner.group_detail gd,  
                    tdw_owner.group_description groupdescr  
                    WHERE mf.restated_market_key = m1.restated_market_key  
                    AND mf.restated_product_key = p1.restated_product_key  
                    AND mf.restated_market_key = mpbd1.restated_market_key  
                    AND mf.restated_product_key = mpbd1.restated_product_key  
                    AND To_DATE(mf.week_key,'YYYYMMDD')  = wd1.week_ending_date
                    AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                    AND gd.group_field_c  =  m1.DIVISION_CODE  
                    AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))  
                    AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))
                    AND (wd1.YEAR_DESCR IN (:p_year))
                    AND wd1.FISCAL_MONTH_NUMBER <= :p_Month
                    AND (mpbd1.business_division_code IN (:p_businessdivision))  
                    AND (m1.division_code IN (:p_salesdivision))  
                    AND (m1.region_code IN (:p_salesregion))  
                    AND (m1.primary_broker_code IN (:p_primarybroker))  
                    AND gd.GROUP_CODE in (:vp)  

                    UNION  ALL
                    
                    SELECT  
                    mpbd1.business_division_code,  
                    mpbd1.business_division_descr,
                    gd.group_code,
                    groupdescr.group_descr,
                    m1.DIVISION_CODE,
                    m1.DIVISION_DESCR,
                    m1.REGION_CODE,
                    m1.REGION_DESCR,
                    md1.year_descr,  
                    md1.month_descr,  
                    md1.FISCAL_MONTH_NUMBER,  
                    0 AS mf_pounds_shipped,
                    0 AS mf_gross_sales_amount,  
                    0 AS mf_allow_amount,  
                    0 AS mf_revenue_adjustment,  
                    0 AS mf_off_invoice_amount,  
                    ids.pounds_shipped AS ids_pounds_shipped,  
                    ids.indirect_sales_amount AS ids_indirect_sales_amount  
                    FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                    tdw_owner.restated_market m1,  
                    tdw_owner.month_dim md1,  
                    tdw_owner.restated_product p1,  
                    tdw_owner.indirect_sales ids,  
                    tdw_owner.week_dim w1,  
                    tdw_owner.group_detail gd,  
                    tdw_owner.group_description groupdescr  
                    WHERE ids.restated_market_key = m1.restated_market_key  
                    AND ids.restated_product_key = p1.restated_product_key  
                    AND ids.restated_market_key = mpbd1.restated_market_key  
                    AND ids.restated_product_key = mpbd1.restated_product_key  
                    AND ids.week_key = w1.week_key  
                    AND w1.month_ending_date = md1.month_ending_date  
                    AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                    AND gd.group_field_c  =  m1.DIVISION_CODE  
                    AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15')) 
                    AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                    AND (md1.year_descr IN (:p_year))  
                    and md1.fiscal_month_number <= :p_Month  
                    AND (mpbd1.business_division_code IN (:p_businessdivision))  
                    AND (m1.division_code IN (:p_salesdivision))  
                    AND (m1.region_code IN (:p_salesregion))  
                    AND (m1.primary_broker_code IN (:p_primarybroker))  
                    AND gd.GROUP_CODE in (:vp)
                    ) /*end sub query*/  
                ids_mf
                GROUP BY
                1,
                2,
                3,
                4,
                5,
                6,
                ids_mf.GROUP_CODE, 
                ids_mf.GROUP_DESCR, 
                ids_mf.DIVISION_CODE, 
                ids_mf.DIVISION_DESCR, 
                ids_mf.REGION_CODE, 
                ids_mf.REGION_DESCR, 
                ids_mf.business_division_code,  
                ids_mf.year_descr,  
                ids_mf.month_descr,  
                ids_mf.FISCAL_MONTH_NUMBER,  
                ids_mf.business_division_Descr             
                 
                /*Begin Volume Total customer*/  
                UNION  ALL
                                SELECT     
                'zzz' AS DEPT_LEVEL,
                 999 AS DEPT_LEVEL_SEQ,
                 999 AS CUSTOMER_LEVEL_SEQ,
                 'zzz' AS CUSTOMER_LEVEL,
                 'zzz' AS MANAGEMENT_LEVEL,
                 999 AS MANAGEMENT_LEVEL_SEQ,  
                0 AS monthexpense_amt,  
                CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                     THEN SUM (  ids_mf.mf_pounds_shipped
                               + ids_mf.ids_pounds_shipped)  
                END AS monthpounds,  
                CASE WHEN (ids_mf.FISCAL_MONTH_NUMBER = (:p_Month))  
                     THEN SUM (  ids_mf.mf_gross_sales_amount  
                               - ids_mf.mf_allow_amount  
                               - ids_mf.mf_off_invoice_amount  
                                 + ids_mf.mf_revenue_adjustment  
                               + ids_mf.ids_indirect_sales_amount)  
                END AS monthnetreceivables,           
                ids_mf.GROUP_CODE, 
                ids_mf.GROUP_DESCR, 
                ids_mf.DIVISION_CODE, 
                ids_mf.DIVISION_DESCR, 
                ids_mf.REGION_CODE, 
                ids_mf.REGION_DESCR, 
                ids_mf.business_division_code,  
                case when (ids_mf.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))Then 'Total Customer'   
                     End AS businessdivisiondescr,  
                NULL AS spend_base_descr,  
                NULL AS spend_base_seq,  
                NULL AS spend_subcategory_descr,  
                NULL AS spend_subcategory_seq,  
                ids_mf.year_descr,  
                ids_mf.month_descr,  
                0 AS expense_amt,  
                SUM (  ids_mf.mf_pounds_shipped
                        + ids_mf.ids_pounds_shipped  
                        ) AS total_pounds_shipped,  
                SUM (  ids_mf.mf_gross_sales_amount  
                       - ids_mf.mf_allow_amount  
                       - ids_mf.mf_off_invoice_amount  
                       + ids_mf.mf_revenue_adjustment  
                       + ids_mf.ids_indirect_sales_amount  
                       ) AS net_receivable_dollar  
                FROM (  
                  /*Begin sub query*/  
                  SELECT   mpbd1.business_division_code,  
                  mpbd1.business_division_descr,
                  gd.group_code,
                  groupdescr.group_descr,
                  m1.division_code,
                  m1.division_descr,
                  m1.region_code,
                  m1.region_descr,
                  wd1.year_descr, 
                  wd1.month_descr,
                  wd1.FISCAL_MONTH_NUMBER,
                  mf.pounds_shipped AS mf_pounds_shipped,
                  mf.gross_sales_amount AS mf_gross_sales_amount,  
                  mf.allow_amount AS mf_allow_amount,
                  mf.gross_sales_variance AS mf_revenue_adjustment,
                  mf.off_invoice_amount AS mf_off_invoice_amount,  
                  0 AS ids_pounds_shipped,  
                  0 AS ids_indirect_sales_amount  
                  FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                  tdw_owner.restated_market m1, 
                  tdw_owner.restated_product p1,
                  tdw_owner.ta_margin_fact_view mf,
                  tdw_owner.week_dim wd1,
                  tdw_owner.group_detail gd,  
                  tdw_owner.group_description groupdescr  
                  WHERE mf.restated_market_key = m1.restated_market_key  
                  AND mf.restated_product_key = p1.restated_product_key  
                  AND mf.restated_market_key = mpbd1.restated_market_key  
                  AND mf.restated_product_key = mpbd1.restated_product_key
                  AND To_DATE(mf.week_key,'YYYYMMDD')  = wd1.week_ending_date
                  AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                  AND gd.group_field_c  =  m1.DIVISION_CODE  
                  AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))  
                  AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))
                  AND (wd1.YEAR_DESCR IN (:p_year))
                  AND wd1.FISCAL_MONTH_NUMBER <= :p_Month
                  AND (mpbd1.business_division_code IN (:p_businessdivision))  
                  AND (m1.division_code IN (:p_salesdivision))  
                  AND (m1.region_code IN (:p_salesregion))  
                  AND (m1.primary_broker_code IN (:p_primarybroker))  
                  AND gd.GROUP_CODE in (:vp)
                  
                  UNION  ALL
                  
                  SELECT  
                  mpbd1.business_division_code,  
                  mpbd1.business_division_descr,  
                  gd.group_code,
                  groupdescr.group_descr,
                  m1.division_code,
                  m1.division_descr,
                  m1.region_code,
                  m1.region_descr,
                  md1.year_descr,  
                  md1.month_descr,  
                  md1.FISCAL_MONTH_NUMBER,  
                  0 AS mf_pounds_shipped,  
                  0 AS mf_gross_sales_amount,  
                  0 AS mf_allow_amount,  
                  0 AS mf_revenue_adjustment,  
                  0 AS mf_off_invoice_amount,  
                  ids.pounds_shipped AS ids_pounds_shipped,  
                  ids.indirect_sales_amount AS ids_indirect_sales_amount  
                  FROM tdw_owner.restated_mkt_prd_bus_div_dim mpbd1,  
                  tdw_owner.restated_market m1,  
                  tdw_owner.month_dim md1,  
                  tdw_owner.restated_product p1,  
                  tdw_owner.indirect_sales ids,  
                  tdw_owner.week_dim w1,  
                  tdw_owner.group_detail gd,  
                  tdw_owner.group_description groupdescr  
                  WHERE ids.restated_market_key = m1.restated_market_key  
                  AND ids.restated_product_key = p1.restated_product_key  
                  AND ids.restated_market_key = mpbd1.restated_market_key  
                  AND ids.restated_product_key = mpbd1.restated_product_key  
                  AND ids.week_key = w1.week_key  
                  AND w1.month_ending_date = md1.month_ending_date  
                  AND gd.GROUP_CODE = groupdescr.GROUP_CODE  
                  AND gd.group_field_c  =  m1.DIVISION_CODE
                  AND (mpbd1.business_division_code IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15')) 				
                  AND (m1.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
                  AND (md1.year_descr IN (:p_year))  
                  and md1.fiscal_month_number <= :p_Month  
                  AND (mpbd1.business_division_code IN (:p_businessdivision))  
                  AND (m1.division_code IN (:p_salesdivision))  
                  AND (m1.region_code IN (:p_salesregion))  
                  AND (m1.primary_broker_code IN (:p_primarybroker))  
                  AND gd.GROUP_CODE in (:vp)  
                ) /*end sub query*/  
                ids_mf
                GROUP BY
                1,
                2,
                3,
                4,
                5,
                6,
                ids_mf.GROUP_CODE, 
                ids_mf.GROUP_DESCR, 
                ids_mf.DIVISION_CODE, 
                ids_mf.DIVISION_DESCR, 
                ids_mf.REGION_CODE, 
                ids_mf.REGION_DESCR, 
                ids_mf.business_division_code,  
                ids_mf.year_descr,  
                ids_mf.month_descr,  
                ids_mf.FISCAL_MONTH_NUMBER,  
                ids_mf.business_division_Descr  
                ORDER BY DEPT_LEVEL_SEQ, CUSTOMER_LEVEL_SEQ, MANAGEMENT_LEVEL_SEQ, spend_base_seq, spend_subcategory_seq 