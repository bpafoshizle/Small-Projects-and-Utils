// Get current credentials of running thread
WindowsIdentity.GetCurrent().Name

public static void WriteLog(StringBuilder sb)
{
	try
	{
		FileStream fs = new FileStream("C:\\Development\\Business Intelligence\\BI\\Tyson.BI.IS\\Tyson.BI.IS.SSRS.DeliveryExtensions\\Tyson.BI.IS.SSRS.PrinterDelivery\\bin\\Debug\\PrinterDeliveryLog.txt", FileMode.Append,
		   FileAccess.Write);
		StreamWriter writer = new StreamWriter(fs);
		writer.Write(sb.ToString());
		writer.Flush();
		writer.Close();
	}

	catch (Exception ex)
	{
		throw new IOException("Error writing to log file: " + ex.Message);
	}
}