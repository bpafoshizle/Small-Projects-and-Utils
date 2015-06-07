' Call from rs.exe in a manner like:
' rs -i ChangeModAndCreateUsers.rss -s http://localhost/reportserver_ISDEV -v itemPath="/Report Builder Reports/Consume Me" -v newName="TYSONET\\SIKESDR"

Public Sub Main()
  Dim properties(2) as [Property]
  Dim propCreatedByID as New [Property]
  Dim propModifiedByID as New [Property]
  
  propCreatedByID.Name = "CreatedBy"
  propCreatedByID.Value = newName
  
  propModifiedByID.Name = "ModifiedBy"
  propModifiedByID.Value = newName
  
  properties(0) = propCreatedByID
  properties(1) = propModifiedByID
  
  If itemPath Is Nothing Then
    Console.WriteLine("Parameter itemPath not specified")
  Else
    Try
	   rs.SetProperties(itemPath, properties)
	   Console.WriteLine("New description set on item {0}.", itemPath)

    Catch ex As SoapException
	   Console.WriteLine(ex.Detail.OuterXml)
    End Try
  End If

End Sub
