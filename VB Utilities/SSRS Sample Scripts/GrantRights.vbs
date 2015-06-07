'=============================================================================
'  File:     GrantRights.vbs
'
'  Summary:  This sample script generates a SQL Script that can be used 
'            to grant a user rights to the report server database.
'
'---------------------------------------------------------------------
' THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY
' KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
' IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
' PARTICULAR PURPOSE.
'============================================================================*/
'
' 1.0 Documentation
'
' Read the following in order to familiarize yourself with the sample script.
' 
' 1.1 Overview
'
' The following sample script generates a SQL Script that can be used 
' to grant a user rights to the report server database, by calling the 
' GenerateDatabaseRightsScript Method in the MSReportServer_ConfigurationSetting 
' class for the RS WMI provider. It also provides functionality to run the 
' script directly on the Report Server database. 
' 
' 1.2 Script Variables
'
' Switches that are passed on the command line:
'
' (a) /rs <ReportServerName> - 	The instance name of the Report Server 
'                               (e.g. your machine name)
'
' (b) /db <DatabaseName> - The database name of the Report Server 
'                          (e.g. "ReportServer")
' 
' (c) /inst <InstanceName> - The instance name of SQL Server
'                            (e.g. "MSSQLSERVER")
'
' (d) /u <UserName(s)> - The user name(s) to whom access should be granted. 
'                        To specify multiple user names, use a comma-separated
'                        list (e.g. "DOMAINNAME\mblythe"
' 
' (e) /w - Specifies whether this is a Windows account.
'
' (f) /r - Specifies whether this is a remote account.

' 
' 1.3 Sample Command Lines
' 
' 
' 1.3.1 Grant rights to user DOMAINNAME\mblythe on report server named SQLSERVER-1
' 
'       cscript GrantRights.vbs /rs SQLSERVER-1 /db ReportServer 
'       /inst MSSQLSERVER /u DOMAINNAME\mblythe
'

Option Explicit

'Command Line Variables
Dim strUserList, strScriptFile, blnWindows, blnRemote 

'Global Variables
Dim strReportServer, strReportServerInstance, strReportServerDatabase, strCommands(12)

strScriptFile = "GrantRights.sql"
strReportServer = "."
strReportServerDatabase = "ReportServer"
strReportServerInstance = "MSSQLSERVER"
blnWindows = False
blnRemote = False

'**************************************************************************************************

' Parse the command line arguments
ParseArguments
           
' Get the grant rights script based on the command line input            
GetScript            
            
'**************************************************************************************************

Sub GetScript()

    Dim objUsers, i, j, strScript 

    objUsers = Split(strUserList, ",", -1, 1)

    For i = 0 to UBound(objUsers)
       
        ' Generate the rights script for the current user
        strScript = GetDatabaseRightsScript(strReportServerDatabase, objUsers(i))
        
        ' Save the script for the current user
        Call SaveScript(strScript, objUsers(i))

    Next

End Sub

' Saves the script to the file system
Sub SaveScript(strScript, strUserName)

    Dim objFSO, objScriptFile

    If strScript <> "" Then
    
        strScriptFile = "GrantRights " & Replace(strUserName, "\", "-") & ".sql"
        
        Set objFSO = CreateObject("Scripting.FileSystemObject")
        Set objScriptFile = objFSO.CreateTextFile(strScriptFile, True)
        
        objScriptFile.WriteLine(strScript)
            
    End If

End Sub


' Parses the command line arguments and set the global variables
Sub ParseArguments()
    
    Dim strHelpText, objArgs, i
    
    strHelpText = "Usage: GrantRights.vbs [options]" & vbCrLf & vbCrLf & _
                  "Example:" & vbCrLf & _
                  " cscript GrantRights.vbs /rs <MachineName> /db ReportServer /inst MSSQLSERVER /u <UserName>" & vbCrLf & vbCrLf & _
                  "Options:" & vbCrLf & _
                  "   /u    <User List>" & vbTab & vbTab & "A comma seperated list of user names." & vbCrLf & _
                  "   /rs   <Report Server Name>" & vbTab & "The name of the Report Server." & vbCrLf & _
                  "   /db  <Database Name>" & vbTab & vbTab & "The database name of the Report Server." & vbCrLf & _
                  "   /inst <Instance>" & vbTab & vbTab & "The instance name of the Report Server." & vbCrLf & _
                  "   /w" & vbTab & vbTab & vbTab & "Specifies that the accounts are Windows accounts." & vbCrLf & _
                  "   /r" & vbTab & vbTab & vbTab & "Specifies that the accounts are remote accounts."

    Set objArgs = WScript.Arguments
    
    If objArgs.Count = 0 Then
        
        ' Display the usage text
        Wscript.Echo strHelpText
    
    ElseIf objArgs(0) = "/?" Or objArgs(0) = "-?" Then
        
        ' Display the usage text
        Wscript.Echo strHelpText
    
    Else
                
        For i = 0 to objArgs.Count - 1
        
            ' Get user name list
            If UCase(objArgs(i)) = "/U" Then
            
                strUserList = objArgs(i + 1)
            
            ' Set whether the accounts are Windows accounts 
            ElseIf UCase(objArgs(i)) = "/W" Then
            
                blnWindows = True
                
            ' Set whether the accounts are remote accounts
            ElseIf UCase(objArgs(i)) = "/R" Then
            
                blnRemote = True
            
            ' Get the name of the Report Server
            ElseIf UCase(objArgs(i)) = "/RS" Then
            
                strReportServer = objArgs(i + 1)
            
            ' Get the name of the Report Server database 
            ElseIf UCase(objArgs(i)) = "/DB" Then
            
                strReportServerDatabase = objArgs(i + 1)
                
            ' Get the name of the Report Server instance
            ElseIf UCase(objArgs(i)) = "/INST" Then
            
                strReportServerInstance = objArgs(i + 1)
                        
            End If

        Next
        
    End If
        
End Sub


' Get the script for granting rights to the specified user
Function GetDatabaseRightsScript(strReportServerDatabaseName, strUserName)

    Dim objWMIService, objShare, objInParam, objOutParams

    Set objWMIService = GetObject("winmgmts:\\" & strReportServer & _
        "\root\Microsoft\SqlServer\ReportServer\RS_" & strReportServerInstance & "\v10\Admin") 

    ' Obtain an instance of the Configuration setting class
    Set objShare = objWMIService.Get("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'")

    ' Obtain an InParameters object specific to the method
    Set objInParam = objShare.Methods_("GenerateDatabaseRightsScript"). _
        inParameters.SpawnInstance_()

    ' Add the input parameters
    objInParam.Properties_.Item("DatabaseName") = strReportServerDatabaseName
    objInParam.Properties_.Item("IsRemote") = blnRemote
    objInParam.Properties_.Item("IsWindowsUser") = blnWindows
    objInParam.Properties_.Item("UserName") = strUserName

    ' Execute the method
    Set objOutParams = objWMIService.ExecMethod("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'", "GenerateDatabaseRightsScript", objInParam)
    
    GetDatabaseRightsScript = objOutParams.Script

End Function