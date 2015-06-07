-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Barret Miller>
-- Create date: <08/31/2009,,>
-- Description:	<,,>
-- =============================================
CREATE PROCEDURE dbo.Cleanup 

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	

	--
	-- Script to cleanup data in the Reporting Services Execution Log subsystem
	--
	-- (c) 2003 Microsoft Corproation, All Rights Reserved
	--
	-- Dave Wickert, BI Practices Team
	-- Sept 11, 2003
	--

	SET NOCOUNT ON

	/* 
	 Change this constant for the earliest data
	 Everything earlier will be deleted 
	 */
	DECLARE @EarliestTimeStart datetime
	SET @EarliestTimeStart = dateadd(Year, -1, GetDate())
							 -- ** Always use ODBC cannonical form **
							 -- i.e. yyyy-mm-dd hh:mi:ss(24h) 

	/* Additional variables used for the audit trail */
	DECLARE @rc INT
	DECLARE @event NVARCHAR(4000)

	/* Delete parameter data first . . . */
	DELETE dbo.FactReportExecutionParameters
	FROM dbo.FactReportExecutionParameters p INNER JOIN
		dbo.FactReportExecution l ON l.LogEntryID = p.LogEntryID
	WHERE l.TimeStart < @EarliestTimeStart

	/* Now we can delete requested Execution Log data itself */
	DELETE FROM dbo.FactReportExecution
	WHERE TimeStart < @EarliestTimeStart
	
	/* Clean up data-driven dimension tables based on data 
	   left in the facts 
	 */
    
    -- The FormatTypes table
	DELETE dbo.DimFormat 
	FROM dbo.DimFormat f
	WHERE f.Format <> -1
		  AND NOT EXISTS (SELECT FormatKey FROM dbo.FactReportExecution l WHERE l.FormatKey = f.FormatKey)
	SET @rc = @@ROWCOUNT
	SET @event = N'Deleted ' + CONVERT(NVARCHAR(20),@rc) + N' rows from the DimFormat table.'
	INSERT INTO dbo.RunLogs (Event) VALUES (@event)   
	
END
GO
