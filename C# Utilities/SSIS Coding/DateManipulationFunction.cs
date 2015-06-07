    private string reformatSimDate(string simDateString)
    {
        var reformatedDateString = "";
        //try
        //{
            if (!simDateString.Trim().Equals(""))
            {
                DateTime dtVal = Convert.ToDateTime(simDateString.Replace("00:00:00.00", ""));
                reformatedDateString = dtVal.Year.ToString() + "-" + dtVal.Month.ToString() + "-" + dtVal.Day.ToString();
            }
            else
            {
                reformatedDateString = "";
            }
        //}
        //catch
        //{
        this.Log(String.Format("simDateString [{0}], reformatedDateString [{1}] ", simDateString, reformatedDateString), 1, null);
        //}

        return reformatedDateString;
    }