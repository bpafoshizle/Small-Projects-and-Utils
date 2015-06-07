Sub FillMkDirBatchSheet()
    Dim mkDirBatchSheet As Worksheet, currentSheet As Worksheet
    Dim highCol As Integer, highRow As Integer, rowNum As Integer, colNum As Integer, insertRow As Integer
    Dim BookName As String, day As String
    
    ' Turn off screen updating to increase performance.
    Application.ScreenUpdating = False
    
    ' Add new MkDirBatch sheet, if it doesn't exist
    ' Delete it first, if it does exist
    If Not SheetExists("MkDirBatch") Then
        Sheets.Add After:=Worksheets(Sheets.Count)
    Else
        Application.DisplayAlerts = False
        Sheets("MkDirBatch").Delete
        Application.DisplayAlerts = False
        Sheets.Add After:=Worksheets(Sheets.Count)
    End If
    
    Set mkDirBatchSheet = ActiveSheet
    mkDirBatchSheet.Name = "MkDirBatch"
    insertRow = 1
    
    ' Loop through each sheet that contains subscription information
    For Each currentSheet In Sheets
        If (currentSheet.Name <> "Inserts" And currentSheet.Name <> "MkDirBatch" And currentSheet.Name <> "All Business Levels" And currentSheet.Name <> "All Sales Levels" And currentSheet.Name <> "All Memos Levels") Then
            day = currentSheet.Name
            
            ' Get the max row num and max col num
            'highCol = currentSheet.UsedRange.Columns.Count
            highCol = currentSheet.Range("D1").End(xlToRight).Column
            'highRow = currentSheet.UsedRange.Rows.Count
            highRow = currentSheet.Range("C2").End(xlDown).Row
            
            'Debugging
            'MsgBox ("High col: " & highCol)
            'MsgBox ("High row: " & highRow)
            
            ' Loop through rows
            For rowNum = 2 To highRow
                BookName = currentSheet.Cells(rowNum, 3).Value
                mkDirBatchSheet.Range("A" & insertRow).Value = createMkDirStatement(day, BookName)
                insertRow = insertRow + 1
            Next rowNum
        End If
    Next currentSheet
    
End Sub


Sub FillInsertSheet()
    Dim insertSheet As Worksheet, currentSheet As Worksheet
    Dim cell As Object
    Dim highCol As Integer, highRow As Integer, rowNum As Integer, colNum As Integer, insertRow As Integer
    Dim day As String, mark As String
    
    ' Turn off screen updating to increase performance.
    Application.ScreenUpdating = False
    
    ' Add new Inserts sheet, if it doesn't exist
    ' Delete it first, if it does exist
    If Not SheetExists("Inserts") Then
        Sheets.Add After:=Worksheets(Sheets.Count)
    Else
        Application.DisplayAlerts = False
        Sheets("Inserts").Delete
        Application.DisplayAlerts = False
        Sheets.Add After:=Worksheets(Sheets.Count)
    End If
    
    Set insertSheet = ActiveSheet
    insertSheet.Name = "Inserts"
    insertRow = 1
    
    ' Loop through each sheet that contains subscription information
    For Each currentSheet In Sheets
        If (currentSheet.Name <> "Inserts" And currentSheet.Name <> "MkDirBatch" And currentSheet.Name <> "All Business Levels" And currentSheet.Name <> "All Sales Levels" And currentSheet.Name <> "All Memos Levels") Then
            day = currentSheet.Name
            
            ' Get the max row num and max col num
            'highCol = currentSheet.UsedRange.Columns.Count
            highCol = currentSheet.Range("D1").End(xlToRight).Column
            'highRow = currentSheet.UsedRange.Rows.Count
            highRow = currentSheet.Range("C2").End(xlDown).Row
            
            'Debugging
            'MsgBox ("High col: " & highCol)
            'MsgBox ("High row: " & highRow)
            
            ' Loop through rows
            For rowNum = 2 To highRow
                'Loop through columns in each row
                For colNum = 4 To highCol
                    mark = currentSheet.Cells(rowNum, colNum).Value
                    If mark <> "" Then
                         Call createInsertSet(currentSheet.Name, currentSheet.Cells(1, colNum).Value, currentSheet.Cells(rowNum, 3).Value, currentSheet.Cells(rowNum, 2), currentSheet.Cells(rowNum, 1), insertSheet, insertRow, mark)
                    End If
                Next colNum
            Next rowNum
        End If
    Next currentSheet
    Application.ScreenUpdating = True
End Sub

' Function to loop through the worksheets in the workbook checking the names
Private Function SheetExists(sheetName As String) As Boolean
    Dim sheet As Worksheet
    SheetExists = False
    For Each ws In ThisWorkbook.Worksheets
        If ws.Name = sheetName Then
            SheetExists = True
            Exit Function
        End If
    Next ws
End Function

Private Sub createInsertSet(sheetName As String, reportName As String, BookName As String, level_num As String, hier As String, insertSheet As Worksheet, insertRow As Integer, mark As String)
    ' Create 4 inserts for each monthly version
    Call createPeriodInserts(sheetName, getShortReportName(reportName), BookName, level_num, hier, insertSheet, insertRow, "M", mark)
    ' Create 6 inserts for each quarterly version
    Call createPeriodInserts(sheetName, getShortReportName(reportName), BookName, level_num, hier, insertSheet, insertRow, "Q", mark)
End Sub

Private Sub createPeriodInserts(sheetName As String, reportName As String, BookName As String, level_num As String, hier As String, insertSheet As Worksheet, insertRow As Integer, period As String, mark As String)
        'If actual
        If LCase(mark) = "a" Then
            Call createPriorActualInserts(sheetName, reportName, BookName, level_num, hier, insertSheet, insertRow, period)
        'If budget
        ElseIf LCase(mark) = "b" Then
            Call createBudgetActualInserts(sheetName, reportName, BookName, level_num, hier, insertSheet, insertRow, period)
        'If both (any char besides 'a' or 'b')
        Else
            Call createPriorActualInserts(sheetName, reportName, BookName, level_num, hier, insertSheet, insertRow, period)
            Call createBudgetActualInserts(sheetName, reportName, BookName, level_num, hier, insertSheet, insertRow, period)
        End If
End Sub

Private Sub createPriorActualInserts(sheetName As String, reportName As String, BookName As String, level_num As String, hier As String, insertSheet As Worksheet, insertRow As Integer, period As String)

    ' Monthly
    If period = "M" Then
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "MP", sheetName, "M", BookName, level_num, hier, getSequenceNum(reportName, "MP"), getOldID(reportName, "MP"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "YP", sheetName, "M", BookName, level_num, hier, getSequenceNum(reportName, "YP"), getOldID(reportName, "YP"))
        insertRow = insertRow + 1
    ' Quarterly
    ElseIf period = "Q" Then
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "MP", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "MP"), getOldID(reportName, "MP"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "QP", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "QP"), getOldID(reportName, "QP"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "YP", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "YP"), getOldID(reportName, "YP"))
        insertRow = insertRow + 1
    End If
    
End Sub

Private Sub createBudgetActualInserts(sheetName As String, reportName As String, BookName As String, level_num As String, hier As String, insertSheet As Worksheet, insertRow As Integer, period As String)
    ' Monthly
    If period = "M" Then
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "MB", sheetName, "M", BookName, level_num, hier, getSequenceNum(reportName, "MB"), getOldID(reportName, "MB"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "YB", sheetName, "M", BookName, level_num, hier, getSequenceNum(reportName, "YB"), getOldID(reportName, "YB"))
        insertRow = insertRow + 1
    'Quarterly
    ElseIf period = "Q" Then
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "MB", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "MB"), getOldID(reportName, "MB"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "QB", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "QB"), getOldID(reportName, "QB"))
        insertRow = insertRow + 1
        
        insertSheet.Range("A" & insertRow).Value = createInsertStatement(reportName, "YB", sheetName, "Q", BookName, level_num, hier, getSequenceNum(reportName, "YB"), getOldID(reportName, "YB"))
        insertRow = insertRow + 1
    End If
End Sub

Private Function createInsertStatement(reportName As String, variation As String, day As String, period As String, BookName As String, level_num As String, hier As String, sequenceNum As String, oldReportNum As String) As String
    Dim tableName As String
    Dim insertStmnt As String
    tableName = "DWA_OWNER.SSRS_SUBSCRIPTION_FILE"
    insertStmnt = "INSERT INTO " & tableName & " " & "( SUBSCRIPTION_NAME, FILE_NAME, FILE_PATH, APPEND_FILE_EXT, RENDER_FRMT, WRITE_MODE, ACTIVE_FLAG, FILE_ACCESS_NAME, FILE_PARAM,REPORT_PARAM_STREAM ) VALUES ("
    insertStmnt = insertStmnt & "'" & getSubscriptionID(period, day, reportName) & "', "
    insertStmnt = insertStmnt & "'" & getFileName(sequenceNum, period, oldReportNum) & "', "
    insertStmnt = insertStmnt & "'" & getFilePath(day, scrubBookName(BookName, True)) & "', "
    insertStmnt = insertStmnt & "'TRUE', 'PDF', 'Overwrite', 'Y', 'svc-financials', 'rpt*@1S2V01C!int', "
    insertStmnt = insertStmnt & "'" & getParamStream(Replace(BookName, "'", "''"), level_num, hier, variation) & "');"
    'MsgBox (Len(insertStmnt))
    'MsgBox (insertStmnt)
    createInsertStatement = insertStmnt
End Function

Private Function getSubscriptionID(period As String, day As String, reportName As String)
    Dim dayNum As String
    If day = "Monday" Then
        dayNum = "1"
    ElseIf day = "Wednesday" Then
        dayNum = "2"
    Else
        dayNum = "0"
    End If
    
    getSubscriptionID = "FINS" & period & dayNum & reportName
End Function

Private Function getFileName(sequenceNum As String, period As String, oldReportNum As String)
    getFileName = "S" & sequenceNum & "N" & period & "R" & oldReportNum
End Function
    
Private Function getFilePath(day As String, BookName As String) As String
    getFilePath = "\\tyson.com\tysondata\whq\app_data\InternalSalesReporting\Financials\Reports\" & day & "\" & BookName
End Function

Private Function getParamStream(BookName As String, level_num As String, hier As String, variation As String)
    getParamStream = "!" & level_num & "|" & BookName & "|" & hier & "!" & variation & "!"
End Function

Private Function getSequenceNum(reportName As String, variation As String)
    Select Case reportName & "-" & variation
        Case "BSD-MP"
            getSequenceNum = "001"
            Exit Function
        Case "BSD-MB"
            getSequenceNum = "004"
            Exit Function
        Case "BSD-QP"
            getSequenceNum = "007"
            Exit Function
        Case "BSD-QB"
            getSequenceNum = "010"
            Exit Function
        Case "BSD-YP"
            getSequenceNum = "013"
            Exit Function
        Case "BSD-YB"
            getSequenceNum = "016"
            Exit Function
        Case "CFS-MP"
            getSequenceNum = "002"
            Exit Function
        Case "CFS-MB"
            getSequenceNum = "005"
            Exit Function
        Case "CFS-QP"
            getSequenceNum = "008"
            Exit Function
        Case "CFS-QB"
            getSequenceNum = "011"
            Exit Function
        Case "CFS-YP"
            getSequenceNum = "014"
            Exit Function
        Case "CFS-YB"
            getSequenceNum = "017"
            Exit Function
        Case "PGD-MP"
            getSequenceNum = "003"
            Exit Function
        Case "PGD-MB"
            getSequenceNum = "006"
            Exit Function
        Case "PGD-QP"
            getSequenceNum = "009"
            Exit Function
        Case "PGD-QB"
            getSequenceNum = "012"
            Exit Function
        Case "PGD-YP"
            getSequenceNum = "015"
            Exit Function
        Case "PGD-YB"
            getSequenceNum = "018"
            Exit Function
        Case "ED-MP"
            getSequenceNum = "019"
            Exit Function
        Case "ED-MB"
            getSequenceNum = "020"
            Exit Function
        Case "ED-QP"
            getSequenceNum = "021"
            Exit Function
        Case "ED-QB"
            getSequenceNum = "022"
            Exit Function
        Case "ED-YP"
            getSequenceNum = "023"
            Exit Function
        Case "ED-YB"
            getSequenceNum = "024"
            Exit Function
        Case "COGS-MP"
            getSequenceNum = "025"
            Exit Function
        Case "COGS-MB"
            getSequenceNum = "026"
            Exit Function
        Case "COGS-QP"
            getSequenceNum = "027"
            Exit Function
        Case "COGS-QB"
            getSequenceNum = "028"
            Exit Function
        Case "COGS-YP"
            getSequenceNum = "029"
            Exit Function
        Case "COGS-YB"
            getSequenceNum = "030"
            Exit Function
        Case Else
            getSequenceNum = "000"
    End Select
End Function

Private Function getOldID(reportName As String, variation As String)
        Select Case reportName & "-" & variation
        Case "BSD-MP"
            getOldID = "360"
            Exit Function
        Case "BSD-MB"
            getOldID = "361"
            Exit Function
        Case "BSD-QP"
            getOldID = "365"
            Exit Function
        Case "BSD-QB"
            getOldID = "366"
            Exit Function
        Case "BSD-YP"
            getOldID = "363"
            Exit Function
        Case "BSD-YB"
            getOldID = "364"
            Exit Function
        Case "CFS-MP"
            getOldID = "328"
            Exit Function
        Case "CFS-MB"
            getOldID = "329"
            Exit Function
        Case "CFS-QP"
            getOldID = "333"
            Exit Function
        Case "CFS-QB"
            getOldID = "334"
            Exit Function
        Case "CFS-YP"
            getOldID = "331"
            Exit Function
        Case "CFS-YB"
            getOldID = "332"
            Exit Function
        Case "PGD-MP"
            getOldID = "352"
            Exit Function
        Case "PGD-MB"
            getOldID = "353"
            Exit Function
        Case "PGD-QP"
            getOldID = "357"
            Exit Function
        Case "PGD-QB"
            getOldID = "358"
            Exit Function
        Case "PGD-YP"
            getOldID = "355"
            Exit Function
        Case "PGD-YB"
            getOldID = "356"
            Exit Function
        Case "ED-MP"
            getOldID = "336"
            Exit Function
        Case "ED-MB"
            getOldID = "337"
            Exit Function
        Case "ED-QP"
            getOldID = "341"
            Exit Function
        Case "ED-QB"
            getOldID = "342"
            Exit Function
        Case "ED-YP"
            getOldID = "339"
            Exit Function
        Case "ED-YB"
            getOldID = "340"
            Exit Function
        Case "COGS-MP"
            getOldID = "344"
            Exit Function
        Case "COGS-MB"
            getOldID = "345"
            Exit Function
        Case "COGS-QP"
            getOldID = "349"
            Exit Function
        Case "COGS-QB"
            getOldID = "350"
            Exit Function
        Case "COGS-YP"
            getOldID = "347"
            Exit Function
        Case "COGS-YB"
            getOldID = "348"
            Exit Function
        Case Else
            getOldID = "000"
    End Select
End Function

Private Function getShortReportName(longReportName As String)
    If longReportName = "Comparative Financial Statement" Then
        getShortReportName = "CFS"
    ElseIf longReportName = "Product Group Detail" Then
        getShortReportName = "PGD"
    ElseIf longReportName = "Business Segment Detail" Then
        getShortReportName = "BSD"
    ElseIf longReportName = "Expense Detail" Then
        getShortReportName = "ED"
    ElseIf longReportName = "COGS Detail" Then
        getShortReportName = "COGS"
    End If
End Function

Private Function createMkDirStatement(day As String, BookName As String) As String
    createMkDirStatement = "mkdir """ & getFilePath(day, scrubBookName(BookName, False)) & """"
End Function

' Function to remove or substitute characters that are invalid in a Windows pathname
' Ampersand is removed because Oracle can't handle it
Private Function scrubBookName(BookName As String, ReplaceSingleQuote As Boolean) As String
    Dim returnName As String
    returnName = BookName
    returnName = Replace(returnName, "<", "-")
    returnName = Replace(returnName, ">", "-")
    returnName = Replace(returnName, ":", "-")
    returnName = Replace(returnName, """", "")
    returnName = Replace(returnName, " / ", " ")
    returnName = Replace(returnName, "/", " ")
    returnName = Replace(returnName, "\", " ")
    returnName = Replace(returnName, "|", "-")
    returnName = Replace(returnName, "?", " ")
    returnName = Replace(returnName, "*", " ")
    returnName = Replace(returnName, " & ", " ")
    
    If (ReplaceSingleQuote) Then
        returnName = Replace(returnName, "'", "''")
    End If
    
    scrubBookName = returnName
End Function
