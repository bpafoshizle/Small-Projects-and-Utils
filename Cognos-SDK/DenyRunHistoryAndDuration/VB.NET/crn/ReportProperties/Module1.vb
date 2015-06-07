
'**
'* ReportProperties.vb
'*
'* Copyright © 2007 Cognos Incorporated. All Rights Reserved.
'* Cognos and the Cognos logo are trademarks of Cognos Incorporated.
'* 
'* Description: (KB 1008110) - CRN SDK VB.NET Sample to Change a Report's Output Versions and Run History Property Settings
'*
'* 		This code sample demonstrates how to change a report's
'* 		'report output versions' property setting to 5 days and
'* 		its 'run history' property setting to 10 occurrences.
'*
'*

Module Module1

    Private oCrn As New CRN.CognosReportNetService
    'connection to the ReportNet service
    Dim endPoint As String = "http://localhost/crn/cgi-bin/cognos.cgi"

    Private namespcID As String = "SDK"
    Private passwd As String = "Education1!"
    Private userId As String = "admin"

    'search path of the report whose properties will be changed
    Private reportSearchPath As String = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']"

    'This format indicates 5 days.  The format for 5 months would be P5M.
    Private reportOutputVersionsDuration As String = "P5D"
    Private runHistoryOccurrences As String = "10"

    Public Sub connectToReportServer()
        Try
            oCrn = New CRN.CognosReportNetService
            oCrn.Url = endPoint

            logon(namespcID, userId, passwd)
        Catch ex As Exception
            Console.WriteLine("connectToReportServer()")
        End Try
        
    End Sub

    'log in as a System Administrator to ensure you have the necessary permissions to do the changes.
    'Change namespaceID, userID and password to the correct one

    Private Sub logon(ByVal strNamespc As String, ByVal strUserId As String, ByVal strPasswd As String)

        Try
            '-------------------------
            'Call the logon method and pass userId, password, and namespace, in
            'an XML encoded string
            '-------------------------
            Call oCrn.logon("<credential><namespace>" & strNamespc & "</namespace><username>" & strUserId & "</username><password>" & strPasswd & "</password></credential>", Nothing)

        Catch ex As Exception
            Console.WriteLine("Error when login")
        End Try

    End Sub

    Private Sub setReportOutputVersionsAndRunHistorySettings(ByVal reportSearchPath As String, ByVal reportOutputVersionsDuration As String, ByVal runHistoryOccurrences As String)
        Dim props As CRN.propEnum()
        Dim bc As CRN.baseClass()
        Dim retentionRules As CRN.retentionRuleArrayProp

        ' Dim outputRule As CRN.retentionRule()

        Dim i As Integer
        Try
            props = New CRN.propEnum() {CRN.propEnum.searchPath}

            retentionRules = New CRN.retentionRuleArrayProp
            Dim outputRule(1) As CRN.retentionRule


            bc = oCrn.query(reportSearchPath, props, New CRN.sort() {}, New CRN.queryOptions)

            If (bc.Length > 0) Then

                For i = 0 To bc.Length - 1
                    outputRule(0) = New CRN.retentionRule
                    outputRule(0).objectClass = CRN.classEnum.reportVersion
                    outputRule(0).prop = CRN.propEnum.creationTime
                    outputRule(0).maxDuration = reportOutputVersionsDuration

                    outputRule(1) = New CRN.retentionRule
                    outputRule(1).objectClass = CRN.classEnum.reportHistory
                    outputRule(1).prop = CRN.propEnum.creationTime
                    outputRule(1).maxObjects = runHistoryOccurrences

                    retentionRules.value = outputRule
                    CType(bc(i), CRN.report).retentions = retentionRules

                    'do the update here 
                    oCrn.update(bc)
                Next

            End If

        Catch ex As Exception
            Console.WriteLine(ex.StackTrace)
        End Try

    End Sub

    Sub Main()
        connectToReportServer()
        setReportOutputVersionsAndRunHistorySettings(reportSearchPath, reportOutputVersionsDuration, runHistoryOccurrences)

        Console.WriteLine("Done.")
    End Sub

End Module
