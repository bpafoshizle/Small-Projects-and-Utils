'=============================================================================
'  File:     UpgradeDatabase.vbs
'
'  Summary:  This sample script generates a SQL Script that can be used 
'            to to upgrade a report server database.
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
' The following sample script generates a SQL Script that can be used to 
' upgrade a report server database by calling the GenerateDatabaseUpgradeScript 
' Method in the on MSReportServer_ConfigurationSetting class for the RS WMI 
' provider. It also provides functionality to run the script directly on the 
' Report Server database.
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
' (d) /o <FileName> - The file name of the SQL Script that is generated
'                     (e.g. "UpgradeDatabase.sql")
'
' (e) /ver <DBVersion> - The version of the database to upgrade to (e.g. C.0.8.54)
' 
' 1.3 Sample Command Lines
' 
' 
' 1.3.1 Upgrade database to version C.0.8.54 on report server named SQLSERVER-1
' 
'       cscript UpgradeDatabase.vbs /rs SQLSERVER-1 /db ReportServer 
'       /inst MSSQLSERVER /o UpgradeDatabase.sql /ver C.0.8.54


Option Explicit

'Command Line Variables
Dim strScriptFile, strDBVersion
Dim strReportServer, strReportServerInstance, strReportServerDatabase

strScriptFile = "UpgradeDatabase.sql"
strReportServer = "."
strReportServerDatabase = "ReportServer"
strReportServerInstance = "MSSQLSERVER"
strDBVersion = "C.0.8.54"

'**************************************************************************************************

' Parse the command line arguments
ParseArguments
           
' Get the upgrade database script based on the command line input            
GetScript            
            
'**************************************************************************************************

Sub GetScript()

    Dim strScript 

    ' Generate the upgrade database script
    strScript = GetUpgradeDatabaseScript(strReportServerDatabase)
        
    ' Save the script to the file system
    SaveScript(strScript)

End Sub

' Saves the script to the file system
Sub SaveScript(strScript)

    Dim objFSO, objScriptFile

    If strScript <> "" Then
        
        Set objFSO = CreateObject("Scripting.FileSystemObject")
        Set objScriptFile = objFSO.CreateTextFile(strScriptFile, True)
        
        objScriptFile.WriteLine(strScript)
            
    End If

End Sub

' Parses the command line arguments and set the global variables
Sub ParseArguments()
    
    Dim strHelpText, objArgs, i
    
    strHelpText = "Usage: UpgradeDatabase.vbs [options]" & vbCrLf & vbCrLf & _
                  "Example:" & vbCrLf & _
                  " cscript UpgradeDatabase.vbs /rs <MachineName> /db ReportServer /inst MSSQLSERVER /o UpgradeDatabase.sql /ver C.0.8.54" & vbCrLf & vbCrLf & _
                  "Options:" & vbCrLf & _
                  "   /o    <File Name>" & vbTab & vbTab & "Script file name." & vbCrLf & _
                  "   /rs   <Report Server>" & vbTab & "The name of the Report Server." & vbCrLf & _
                  "   /db  <Database>" & vbTab & vbTab & "The database name of the Report Server." & vbCrLf & _
                  "   /inst <Instance>" & vbTab & vbTab & "The instance name of the Report Server." & vbCrLf & _
                  "   /ver <DB Version>" & vbTab & vbTab & "The version to upgrade the database to."
    
    Set objArgs = WScript.Arguments
    
    If objArgs.Count = 0 Then
        
        ' Display the usage text
        Wscript.Echo strHelpText
    
    ElseIf objArgs(0) = "/?" Or objArgs(0) = "-?" Then
        
        ' Display the usage text
        Wscript.Echo strHelpText
    
    Else
                
        For i = 0 to objArgs.Count - 1
        
            ' Get the name for the script file
            If UCase(objArgs(i)) = "/O" Then
            
                strScriptFile = objArgs(i + 1)

            ' Get the name of the Report Server
            ElseIf UCase(objArgs(i)) = "/RS" Then
            
                strReportServer = objArgs(i + 1)
            
            ' Get the name of the Report Server database 
            ElseIf UCase(objArgs(i)) = "/DB" Then
            
                strReportServerDatabase = objArgs(i + 1)
                
            ' Get the name of the Report Server instance
            ElseIf UCase(objArgs(i)) = "/INST" Then
            
                strReportServerInstance = objArgs(i + 1)
                
            ' Get the version to upgrade the DB to
            ElseIf UCase(objArgs(i)) = "/VER" Then
            
                strDBVersion = objArgs(i + 1)                
                        
            End If

        Next
        
    End If
        
End Sub


' Get the script for upgrading the Report Server databases
Function GetUpgradeDatabaseScript(strReportServerDatabaseName)

    Dim objWMIService, objShare, objInParam, objOutParams

    Set objWMIService = GetObject("winmgmts:\\" & strReportServer & _
        "\root\Microsoft\SqlServer\ReportServer\RS_" & strReportServerInstance & "\v10\Admin") 

    ' Obtain an instance of the Configuration setting class
    Set objShare = objWMIService.Get("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'")

    ' Obtain an InParameters object specific to the method.
    Set objInParam = objShare.Methods_("GenerateDatabaseUpgradeScript"). _
        inParameters.SpawnInstance_()

    ' Add the input parameters.
    objInParam.Properties_.Item("DatabaseName") =  strReportServer
    objInParam.Properties_.Item("ServerVersion") =  strDBVersion

    ' Execute the method
    Set objOutParams = objWMIService.ExecMethod("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'", "GenerateDatabaseUpgradeScript", objInParam)

    GetUpgradeDatabaseScript = objOutParams.Script

End Function