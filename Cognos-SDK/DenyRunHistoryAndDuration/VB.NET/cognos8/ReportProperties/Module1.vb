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
Imports cognosdotnet

Module Module1

    Private cmService As contentManagerService1
    'connection to the ReportNet service
    Dim endPoint As String = "http://localhost:9300/p2pd/servlet/dispatch"

    Private namespcID As String = "LocalNT"
    Private passwd As String = "Education1!"
    Private userId As String = "admin"

    'search path of the report whose properties will be changed
    Private reportSearchPath As String = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']/report[@name='Add Color']"

    'This format indicates 5 days.  The format for 5 months would be P5M.
    Private reportOutputVersionsDuration As String = "P5D"
    Private runHistoryOccurrences As String = "10"

 
    Public Sub connectToReportServer()
        Try           
            cmService = New contentManagerService1
            cmService.Url = endPoint

            logon(namespcID, userId, passwd)
        Catch ex As Exception
            Console.WriteLine(ex.StackTrace)
        End Try
    End Sub
    '
    'log in as a System Administrator to ensure you have the necessary permissions to do the changes.
    'Change namespaceID, userID and password to the correct one
    Public Sub logon(ByVal strNameSpace As String, ByVal strUserName As String, ByVal strPassword As String)

        Dim strLogon As String = "<credential><namespace>" & strNameSpace & _
                                 "</namespace><username>" & strUserName & _
                                 "</username><password>" & strPassword & _
                                 "</password></credential>"

        Dim xmlEncodedCredentials As New xmlEncodedXML

        Try
            xmlEncodedCredentials.Value = strLogon
            cmService.logon(xmlEncodedCredentials, Nothing)

        Catch ex As Exception
            Console.WriteLine("Error when login")
        End Try

    End Sub

    Private Sub setReportOutputVersionsAndRunHistorySettings(ByVal reportSearchPath As String, ByVal reportOutputVersionsDuration As String, ByVal runHistoryOccurrences As String)
        Dim props As propEnum()
        Dim bc As baseClass()
        Dim retentionRules As retentionRuleArrayProp
        Dim searchPath As New searchPathMultipleObject

        Dim i As Integer

        Try
            props = New propEnum() {propEnum.searchPath}

            retentionRules = New retentionRuleArrayProp
            Dim outputRule(1) As retentionRule

            searchPath.Value = reportSearchPath
            bc = cmService.query(searchPath, props, New sort() {}, New queryOptions)

            If (bc.Length > 0) Then

                For i = 0 To bc.Length - 1
                    outputRule(0) = New retentionRule
                    outputRule(0).objectClass = classEnum.reportVersion
                    outputRule(0).prop = propEnum.creationTime
                    outputRule(0).maxDuration = reportOutputVersionsDuration

                    outputRule(1) = New retentionRule
                    outputRule(1).objectClass = classEnum.history
                    outputRule(1).prop = propEnum.creationTime
                    outputRule(1).maxObjects = runHistoryOccurrences

                    retentionRules.value = outputRule
                    CType(bc(i), report).retentions = retentionRules

                    'do the update here 
                    cmService.update(bc, New updateOptions)
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
