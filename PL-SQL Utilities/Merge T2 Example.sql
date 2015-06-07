/* There will never be a case where a t2 change occurs without a t1 */
merge into tdw_owner.dim_cust_billto tdw
using tds_owner.dim_cust_billto tds on (tdw.billto_key = tds.billto_key) -- use keys, but could use AK(billto_code and eff_beg_date)
when matched then 
  update set  -- eff_beg_date will never change
              tdw.eff_end_date = tds.eff_end_date, -- have to set this. t1 change implies t2 change; deprecates old row
              tdw.current_code = tds.current_code,
              -- skip load_date for existing records
              tdw.mod_date = tds.mod_date,
              -- skip usage in fact flag
              -- no inferred_member_flag on tdw
              tdw.t1_checksum = tds.t1_checksum,
              -- t2_checksum will never change
              tdw.billto_name = tds.billto_name,
              tdw.billto_name_hist = tds.billto_name_hist,
              tdw.billto_long_name = tds.billto_long_name,
              tdw.billto_long_name_hist = tds.billto_long_name_hist,
              tdw.billto_status_code = tds.billto_status_code,
              tdw.billto_status_code_hist = tds.billto_status_code_hist,
              tdw.billto_mstr_addr_key = tds.billto_mstr_addr_key,
              tdw.billto_mstr_addr_hist_key = tds.billto_mstr_addr_hist_key
         where tdw.t1_checksum <> tds.t1_checksum -- t2_checksum will never change, so no need to check it here. If t1 changes, there is a t2 change as well
when not matched then 
  insert (billto_key, 
          billto_code, 
          eff_beg_date, 
          eff_end_date,
          current_code,
          load_date,
          mod_date,
          usage_in_fact_flag,
          t1_checksum,
          t2_checksum,
          billto_name,
          billto_name_hist,
          billto_long_name,
          billto_long_name_hist,
          billto_status_code,
          billto_status_code_hist,
          billto_mstr_addr_key,
          billto_mstr_addr_hist_key)
  values (tds.billto_key,
          tds.billto_code,
          tds.eff_beg_date,
          tds.eff_end_date,
          tds.current_code,
          tds.load_date,
          tds.mod_date,
          tds.usage_in_fact_flag,
          tds.t1_checksum,
          tds.t2_checksum,
          tds.billto_name,
          tds.billto_name_hist,
          tds.billto_long_name,
          tds.billto_long_name_hist,
          tds.billto_status_code,
          tds.billto_status_code_hist,
          tds.billto_mstr_addr_key,
          tds.billto_mstr_addr_hist_key);