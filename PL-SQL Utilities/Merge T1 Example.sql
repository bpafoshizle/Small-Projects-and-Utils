merge into tdw_owner.dim_mstr_addr tdw
using tds_owner.mstr_addr tds on (tdw.mstr_addr_key = tds.mstr_addr_key)
when matched then 
  update set -- do not update load_date on pre-existing records
             tdw.mod_date = tds.mod_date,
             tdw.t1_checksum = tds.t1_checksum,
             tdw.addr_ln_1 = tds.addr_ln_1,
             tdw.addr_ln_2 = tds.addr_ln_2,
             tdw.addr_ln_3 = tds.addr_ln_3,
             tdw.city = tds.city,
             tdw.state_abbr = tds.state_abbr,
             tdw.state_name = tds.state_name,
             tdw.postal_code = tds.postal_code,
             tdw.iso_cntry_code = tds.iso_cntry_code,
             tdw.iso_cntry_name = tds.iso_cntry_name,
             tdw.cnty_name = tds.cnty_name
         where tdw.t1_checksum <> tds.t1_checksum
when not matched then
  insert (mstr_addr_key,
          load_date,
          mod_date,
          t1_checksum,
          addr_ln_1,
          addr_ln_2,
          addr_ln_3,
          city,
          state_abbr,
          state_name,
          postal_code,
          iso_cntry_code,
          iso_cntry_name,
          cnty_name)
  values (tds.mstr_addr_key,
          tds.load_date,
          tds.mod_date,
          tds.t1_checksum,
          tds.addr_ln_1,
          tds.addr_ln_2,
          tds.addr_ln_3,
          tds.city,
          tds.state_abbr,
          tds.state_name,
          tds.postal_code,
          tds.iso_cntry_code,
          tds.iso_cntry_name,
          tds.cnty_name);

