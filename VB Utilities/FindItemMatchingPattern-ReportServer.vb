    ' Call from rs.exe in a manner like:
    ' rs -i FindItemMatchingPattern-ReportServer.rss -s http://reportweb.tyson.com/reportserver -v Folder="/Report Builder Reports/Invoice Analytics" -v searchItem="MSC*" -v creatorName="HOLTKE" -v modifierName="HOLTKE"

    Public Sub Main()

        ' Default folder to the root directory
        Dim Folder As String = "/"
        ' Default creator to anyone
        Dim creatorName As String = "*"
        ' Default modifier name to anyone
        Dim modifierName As String = "*"
        ' searchItem isn't defaulted because it must be supplied
        Dim searchItem As String = Nothing

		
        'Dim rs As New ReportingService2005()
        Dim BooleanOperator As BooleanOperatorEnum = BooleanOperatorEnum.And
        Dim conditions(0 To 2) As SearchCondition
        Dim returnValue As CatalogItem()

        ' Set up name from what was supplied
        Dim itemName As New SearchCondition
        itemName.Name = "Name"
        itemName.Value = searchItem

        ' Set up createUser from what was supplied
        Dim itemCreateUser As New SearchCondition
        itemCreateUser.Name = "Created By"
        itemCreateUser.Value = creatorName

        ' Set up modifyUser from what was supplied
        Dim itemModUser As New SearchCondition
        itemModUser.Name = "Modified By"
        itemModUser.Value = modifierName

        conditions(0) = itemName
        conditions(1) = itemCreateUser
        conditions(2) = itemModUser


        If searchItem Is Nothing Then
            Console.WriteLine("Parameter searchItem not specified")
        Else
            Try
                returnValue = rs.FindItems(Folder, BooleanOperator, conditions)
                printReturnedItems(returnValue)
            Catch ex As SoapException
                Console.WriteLine(ex.Detail.OuterXml)
            End Try
        End If

    End Sub

    Function printReturnedItems(ByVal items As CatalogItem()) As Integer
        Dim ci As CatalogItem
        For Each ci In items
            Console.WriteLine(ci.Path + "\\" + ci.Name)
        Next

        Return True
    End Function