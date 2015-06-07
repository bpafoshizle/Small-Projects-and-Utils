	  	   SELECT       sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 	  
	                       case when mpbd.sales_unit_code = 'F01' Then 'Distribution Frozen'   
	                            when mpbd.sales_unit_code =  'F08' Then 'Distribut Prep Foods' 
	                            when mpbd.sales_unit_code =  'F15' Then 'Distribution Other'   
	                            when mpbd.sales_unit_code =  'F03' Then 'Distribution Market'  
	                            when mpbd.sales_unit_code =  'F04' Then 'Distribution CVP'     
	                            when mpbd.sales_unit_code =  'F20' Then 'Distribution Fowl'    
	                            when mpbd.sales_unit_code =  'F30' Then 'Gov''t Sales-Poultry' 
	                            when mpbd.sales_unit_code =  'F31' Then 'Gov''t Sales-Prepared'
                                    End as sales_unit_descr,	  
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr  
	                                                               AS primary_broker_code_descr,  
	                       CASE  
	                          WHEN (LENGTH (TRIM (s.spend_request_id))) > 0  
	                             THEN s.spend_request_id  
	                          ELSE 'Not Available'  
	                       END AS spend_request_id,  
	                       a.acct_class_descr AS expense_descr,  
	                       INITCAP (o.operator_name) AS operator_name,  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr,  
	                       SUM (epf.expense_amt) AS expense_amt  
	           FROM        tdw_owner.restated_mkt_prd_bus_div_dim mpbd,  
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
	                       tdw_owner.expense_payee_dim pay  
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
	                   AND sc.spend_base_seq <> 0  
	                   AND sc.spend_subcategory_seq <> 0  
	                   AND (mpbd.sales_unit_code IN ('F01', 'F03', 'F04', 'F08', 'F30' ,'F31' ,'F15','F20'))  
	                   AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
	                   AND (md.year_descr IN (:p_year))  
	                   AND (md.fiscal_month_number = (:p_month))  
	                   AND (mpbd.sales_unit_code IN (:p_sales_unit))  
	                   AND (m.division_code IN (:p_salesdivision))  
	                   AND (m.region_code IN (:p_salesregion))  
	                   AND (m.primary_broker_code IN (:p_primarybroker))  
	              GROUP BY  
	  			sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 
	  			mpbd.sales_unit_code,  
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr,  
	                       s.spend_request_id,  
	                       a.acct_class_descr,  
	                       INITCAP (o.operator_name),  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr 
	              UNION  
	              SELECT    
	  			sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 	  
	                       case when (mpbd.sales_unit_code IN  ('F01', 'F08', 'F15'))Then 'Total Value Added'  
	                            when (mpbd.sales_unit_code IN  ('F03', 'F04'))Then 'Total Market/CVP'          
	                            when (mpbd.sales_unit_code IN  ('F30', 'F31'))Then 'Total Government Sales'    
	                            when (mpbd.sales_unit_code IN  ('F01', 'F08','F15', 'F03', 'F04','F20'))Then 'Total Distribution'                          
                                    End as sales_unit_descr,
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr  
	                                                               AS primary_broker_code_descr,  
	                       CASE  
	                          WHEN (LENGTH (TRIM (s.spend_request_id))) > 0  
	                             THEN s.spend_request_id  
	                          ELSE 'Not Available'  
	                       END AS spend_request_id,  
	                       a.acct_class_descr AS expense_descr,  
	                       INITCAP (o.operator_name) AS operator_name,  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr,  
	                       SUM (epf.expense_amt) AS expense_amt  
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
	                       tdw_owner.expense_payee_dim pay  
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
	                   AND sc.spend_base_seq <> 0  
	                   AND sc.spend_subcategory_seq <> 0  
	                   AND (mpbd.sales_unit_code IN ('F01', 'F03', 'F04', 'F08', 'F30' ,'F31' ,'F15','F20')) 
	                   AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))      
	                   AND (md.year_descr IN (:p_year))  
	                   AND (md.fiscal_month_number = (:p_month))  
	                   AND (mpbd.sales_unit_code IN (:p_sales_unit))  
	                   AND (m.division_code IN (:p_salesdivision))  
	                   AND (m.region_code IN (:p_salesregion))  
	                   AND (m.primary_broker_code IN (:p_primarybroker))  
	              GROUP BY  
	  			sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 
	  			case when (mpbd.sales_unit_code IN  ('F01', 'F08', 'F15'))Then 'Total Value Added'   
	  				  when (mpbd.sales_unit_code IN  ('F03', 'F04'))Then 'Total Market/CVP'      
	  				  when (mpbd.sales_unit_code IN  ('F30', 'F31'))Then 'Total Government Sales' 
	                                 when (mpbd.sales_unit_code IN  ('F01', 'F08','F15', 'F03', 'F04','F20'))Then 'Total Distribution'
	  				     END, 
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr,  
	                       s.spend_request_id,  
	                       a.acct_class_descr,  
	                       INITCAP (o.operator_name),  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr 
	              UNION  
	              SELECT    
	  			sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 
	                       case when (mpbd.sales_unit_code IN  ('F01', 'F03', 'F04', 'F08', 'F30', 'F31', 'F15','F20'))Then 'Total Customer' 
	  			     End as sales_unit_descr, 
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr  AS primary_broker_code_descr,  
	                       CASE  WHEN (LENGTH (TRIM (s.spend_request_id))) > 0  
	                             THEN s.spend_request_id  
	                          ELSE 'Not Available'  
	                       END AS spend_request_id,  
	                       a.acct_class_descr AS expense_descr,  
	                       INITCAP (o.operator_name) AS operator_name,  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr,  
	                       SUM (epf.expense_amt) AS expense_amt  
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
	                       tdw_owner.expense_payee_dim pay  
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
	                   AND sc.spend_base_seq <> 0  
	                   AND sc.spend_subcategory_seq <> 0  
	                   AND (mpbd.sales_unit_code IN ('F01', 'F03', 'F04', 'F08', 'F30' ,'F31' ,'F15','F20'))  
	                   AND (m.selling_group_code IN ('119000', '125000', '610000', '800000', '423000'))  
	                   AND (md.year_descr IN (:p_year))  
	                   AND (md.fiscal_month_number = (:p_month))  
	                   AND (mpbd.sales_unit_code IN (:p_sales_unit)) 
	                   AND (m.division_code IN (:p_salesdivision))  
	                   AND (m.region_code IN (:p_salesregion))  
	                   AND (m.primary_broker_code IN (:p_primarybroker))  
	              GROUP BY  
	  			sc.DEPT_LEVEL, 
	  			sc.DEPT_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL_SEQ, 
	  			sc.CUSTOMER_LEVEL, 
	  			sc.MANAGEMENT_LEVEL, 
	  			sc.MANAGEMENT_LEVEL_SEQ, 
	  			 case when (mpbd.sales_unit_code IN  ('F01', 'F03', 'F04', 'F08', 'F30', 'F31', 'F15','F20'))Then 'Total Customer' 
	  					 END, 
	                       sc.spend_base_descr,  
	                       sc.spend_base_seq,  
	                       sc.spend_subcategory_descr,  
	                       sc.spend_subcategory_seq,  
	                       m.REGION_CODE,  
	                       m.REGION_DESCR,  
	                       m.primary_broker_code,  
	                       m.primary_broker_code || '--' || m.primary_broker_descr,  
	                       s.spend_request_id,  
	                       a.acct_class_descr,  
	                       INITCAP (o.operator_name),  
	                       INITCAP (pay.payee_name),  
	                       md.year_descr,  
	                       md.month_descr 
	              ORDER BY  
	  					 DEPT_LEVEL_SEQ, 
	  					 CUSTOMER_LEVEL_SEQ, 
	  					 MANAGEMENT_LEVEL_SEQ, 
	                       spend_base_seq,  
	                       spend_subcategory_seq,  
	                       spend_request_id