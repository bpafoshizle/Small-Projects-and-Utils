create or replace
FUNCTION          eval_date
         (p_datevalue in varchar2, output_type in varchar2)

return date is v_returndate date;

begin

  if p_datevalue = 'Current' then
    case
      -- Current accounting week_ending_date
      when output_type = 'WE' then select wd.week_ending_date into v_returndate from tdw_owner.week_dim wd where wd.current_acctg_wk_flag = 'Y';
      -- Current accounting accounting month_ending_date
      when output_type = 'ME' then select wd.month_ending_date into v_returndate from tdw_owner.week_dim wd where wd.current_acctg_mth_flag = 'Y' and rownum = 1;
      -- Current accounting quarter_ending_date
      when output_type = 'QE' then select wd.quarter_ending_date into v_returndate from tdw_owner.week_dim wd where wd.current_acctg_qtr_flag = 'Y' and rownum = 1;
      -- Current accounting year_ending_date
      when output_type = 'YE' then select wd.year_ending_date into v_returndate from tdw_owner.week_dim wd where wd.current_acctg_yr_flag = 'Y' and rownum = 1;
      -- Default to sysdate
      else v_returndate := sysdate;
    end case;
  elsif p_datevalue = 'CurrentCal' then
      case
        -- Current day
        when output_type = 'D' then select dd.day into v_returndate from tdw_owner.day_dim dd where dd.current_day_flag = 'Y';
        -- Current week_ending_date
        when output_type = 'WE' then select dd.week_ending_date into v_returndate from tdw_owner.day_dim dd where dd.current_week_flag = 'Y' and rownum = 1;
        -- Current month_ending_date
        when output_type = 'ME' then select dd.month_ending_date into v_returndate from tdw_owner.day_dim dd where dd.current_month_flag = 'Y' and rownum = 1;
        -- Current quarter_ending_date
        when output_type = 'QE' then select dd.quarter_ending_date into v_returndate from tdw_owner.day_dim dd where dd.current_quarter_flag = 'Y' and rownum = 1;
        -- Current year_ending_date
        when output_type = 'YE' then select dd.year_ending_date into v_returndate from tdw_owner.day_dim dd where dd.current_year_flag = 'Y' and rownum = 1;
      end case;
  else
    case 
      -- The same date back
      when output_type = 'D' then select dd.day into v_returndate from tdw_owner.day_dim dd where dd.day = cast(p_datevalue as date);
      -- The week ending date for the passed-in date
      when output_type = 'WE' then select dd.week_ending_date into v_returndate from tdw_owner.day_dim dd where dd.day = cast(p_datevalue as date);
      -- The month ending date for the passed-in date
      when output_type = 'ME' then select dd.month_ending_date into v_returndate from tdw_owner.day_dim dd where dd.day = cast(p_datevalue as date);
      -- The quarter ending date for the passed-in date
      when output_type = 'QE' then select dd.quarter_ending_date into v_returndate from tdw_owner.day_dim dd where dd.day = cast(p_datevalue as date);
      -- The year ending date for the passed-in date
      when output_type = 'YE' then select dd.year_ending_date into v_returndate from tdw_owner.day_dim dd where dd.day = cast(p_datevalue as date);
      -- Default to sysdate
      else v_returndate := sysdate;
    end case;
  end if;

  return v_returndate;

exception
   when others then
   return sysdate;
end;