'=============================================================================
'  File:     CreateDatabase.vbs
'
'  Summary:  This sample script generates a SQL Script that can be used 
'            to create a report server database.
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
' create a report server database by calling the GenerateDatabaseCreationScript 
' Method in the MSReportServer_ConfigurationSetting class for the RS WMI 
' provider. It also provides functionality to run the script directly on 
' the Report Server database.
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
' (e) /lcid <LCID> - The locale ID (LCID) for the script. By default, 
'                    the LCID is 1033 which is the assigned LCID for 
'                    English - United States.  
' 
' 1.3.1 Create SQL Script to create database on report server named SQLSERVER-1
' 
'       cscript CreateDatabase.vbs /rs SQL-SERVER-1
'       /db ReportServer /inst MSSQLSERVER /o CreateDatabase.sql

Option Explicit

'Command Line Variables
Dim strScriptFile, blnIsSharePointMode, intLCID
Dim strReportServer, strReportServerInstance, strReportServerDatabase

strScriptFile = "CreateDatabase.sql"
strReportServer = "."
strReportServerDatabase = "ReportServer"
strReportServerInstance = "MSSQLSERVER"
intLCID = 1033
blnIsSharePointMode = False

'**************************************************************************************************

' Parse the command line arguments
ParseArguments
           
' Get the create database script based on the command line input            
GetScript            
            
'**************************************************************************************************

Sub GetScript()

    Dim strScript 

    ' Generate the create database script
    strScript = GetCreateDatabaseScript(strReportServerDatabase)
    
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
    
    strHelpText = "Usage: CreateDatabase.vbs [options]" & vbCrLf & vbCrLf & _
                  "Example:" & vbCrLf & _
                  " cscript CreateDatabase.vbs /rs <MachineName> /db ReportServer /inst MSSQLSERVER /o CreateDatabase.sql" & vbCrLf & vbCrLf & _
                  "Options:" & vbCrLf & _
                  "   /rs   <Report Server>" & vbTab & "The name of the Report Server." & vbCrLf & _
                  "   /db  <Database>" & vbTab & vbTab & "Report Server database name." & vbCrLf & _
                  "   /inst <Instance>" & vbTab & vbTab & "Report Server instance name." & vbCrLf & _
                  "   /o    <File Name>" & vbTab & vbTab & "Script file name." & vbCrLf & _
                  "   /lcid <Locale ID>" & vbTab & vbTab & "The Locale ID for the script. By default, the Locale ID is 1033 which is the assigned LCID for English - United States." & vbCrLf & _     
                  "   /sp" & vbTab & vbTab & vbTab & "Specifies whether the DB is in SharePoint mode."                  

    
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

            ' Set whether the DB should be created in 
            ' SharePoint mode
            ElseIf UCase(objArgs(i)) = "/SP" Then
            
                blnIsSharePointMode = True
            
            ' Get the name of the Report Server
            ElseIf UCase(objArgs(i)) = "/RS" Then
            
                strReportServer = objArgs(i + 1)
            
            ' Get the name of the Report Server database 
            ElseIf UCase(objArgs(i)) = "/DB" Then
            
                strReportServerDatabase = objArgs(i + 1)
                
            ' Get the name of the Report Server instance
            ElseIf UCase(objArgs(i)) = "/INST" Then
            
                strReportServerInstance = objArgs(i + 1)
                
            ' Get the name of the Report Server instance
            ElseIf UCase(objArgs(i)) = "/LCID" Then
            
                intLCID = objArgs(i + 1)                
                        
            End If

        Next
        
    End If
        
End Sub


' Get the script for creating the Report Server databases
Function GetCreateDatabaseScript(strReportServerDatabaseName)

    Dim objWMIService, objShare, objInParam, objOutParams

    Set objWMIService = GetObject("winmgmts:\\" & strReportServer & _
        "\root\Microsoft\SqlServer\ReportServer\RS_" & strReportServerInstance & "\v10\Admin") 

    ' Obtain an instance of the Configuration setting class
    Set objShare = objWMIService.Get("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'")

    ' Obtain an InParameters object specific to the method.
    Set objInParam = objShare.Methods_("GenerateDatabaseCreationScript"). _
        inParameters.SpawnInstance_()

    ' Add the input parameters.
    objInParam.Properties_.Item("DatabaseName") =  strReportServerDatabaseName
    objInParam.Properties_.Item("IsSharePointMode") =  blnIsSharePointMode
    objInParam.Properties_.Item("Lcid") =  intLCID

    ' Execute the method
    Set objOutParams = objWMIService.ExecMethod("MSReportServer_ConfigurationSetting.InstanceName='" & _
        strReportServerInstance & "'", "GenerateDatabaseCreationScript", objInParam)

    GetCreateDatabaseScript = objOutParams.Script

End Function