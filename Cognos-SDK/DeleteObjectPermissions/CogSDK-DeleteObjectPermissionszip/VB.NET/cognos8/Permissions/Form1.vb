'
' Description: KB 1020887 - SDK sample to remove a security object from Permissions tab 
'

Imports System
Imports System.Web.Services
Imports System.Web.Services.Protocols
Imports System.IO
Imports cognosdotnet


Public Class Form1
    Inherits System.Windows.Forms.Form

#Region " Windows Form Designer generated code "

    Public Sub New()
        MyBase.New()

        'This call is required by the Windows Form Designer.
        InitializeComponent()

        'Add any initialization after the InitializeComponent() call

    End Sub

    'Form overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    'Required by the Windows Form Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Windows Form Designer
    'It can be modified using the Windows Form Designer.  
    'Do not modify it using the code editor.
    Friend WithEvents Button1 As System.Windows.Forms.Button
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        Me.Button1 = New System.Windows.Forms.Button
        Me.SuspendLayout()
        '
        'Button1
        '
        Me.Button1.Location = New System.Drawing.Point(72, 64)
        Me.Button1.Name = "Button1"
        Me.Button1.Size = New System.Drawing.Size(168, 48)
        Me.Button1.TabIndex = 0
        Me.Button1.Text = "Click Me"
        '
        'Form1
        '
        Me.AutoScaleBaseSize = New System.Drawing.Size(5, 13)
        Me.ClientSize = New System.Drawing.Size(292, 150)
        Me.Controls.Add(Me.Button1)
        Me.Name = "Form1"
        Me.Text = "Form1"
        Me.ResumeLayout(False)

    End Sub

#End Region

    
    Private intReturn As Integer
    Private booReturn As Boolean

    'Logon to Cognos 8
    Public Function Logon(ByVal strNameSpace As String, _
                          ByVal strUserName As String, _
                          ByVal strPassword As String, _
                          ByRef cmService As contentManagerService1) As Boolean

        Dim strLogon As String = "<credential><namespace>" & strNameSpace & _
                                 "</namespace><username>" & strUserName & _
                                 "</username><password>" & strPassword & _
                                 "</password></credential>"

        Dim xmlEncodedCredentials As New xmlEncodedXML

        Try
            xmlEncodedCredentials.Value = strLogon

            cmService.logon(xmlEncodedCredentials, Nothing)

            Logon = True

        Catch ex As SoapHeaderException
            Logon = False
        End Try
    End Function

    'Query Content Store for specific searchPath
    Public Function GetPath(ByVal strCognosPath As String, _
                            ByRef bcaPackages() As baseClass, _
                            ByRef cmService As contentManagerService1) As Boolean

        Dim props() As propEnum = {propEnum.defaultName, propEnum.searchPath, propEnum.permissions, propEnum.policies}
        Dim query_options As New queryOptions
        Dim sortOptionArray(0) As sort
        Dim searchPath As New searchPathMultipleObject

        Try

            sortOptionArray(0) = New sort
            sortOptionArray(0).order = orderEnum.ascending
            sortOptionArray(0).propName = propEnum.defaultName

            searchPath.Value = strCognosPath
            bcaPackages = cmService.query(searchPath, props, sortOptionArray, query_options)

            GetPath = True

        Catch ex As SoapHeaderException
            GetPath = False
        End Try

    End Function

    'Set (grant/deny) strAccess (write, read, etc.) access for strGroupPath to strObjectPath object
    Public Function SetAccess(ByVal strObjectPath As String, _
                              ByVal strGroupPath As String, _
                              ByVal strAccess As String, _
                              ByVal intGrant As Integer, _
                              ByRef cmService As contentManagerService1) As Boolean

        Dim bcaObject() As baseClass
        Dim bcaGroup() As baseClass

        Dim objPermission As New permission

        Dim tmpPolicy As policy
        Dim tmpGroupPathCAMID As String
        Dim intCounter As Integer
        Dim intSubCounter As Integer
        Dim booSecurityObjFound As Boolean = False
        Dim booPermissionFound As Boolean = False

        Try
            booReturn = GetPath(strObjectPath, bcaObject, cmService)
            booReturn = GetPath(strGroupPath, bcaGroup, cmService)

            tmpGroupPathCAMID = bcaGroup(0).searchPath.value

            objPermission.name = strAccess
            If intGrant = 1 Then
                objPermission.access = accessEnum.grant
            ElseIf intGrant = 0 Then
                objPermission.access = accessEnum.deny
            Else
                objPermission.access = Nothing
            End If

            'Check to see if the object exists
            For intCounter = 0 To bcaObject(0).policies.value.Length - 1
                tmpPolicy = bcaObject(0).policies.value(intCounter)
                'Look for the right Permission
                If tmpPolicy.securityObject.searchPath.value.Equals(tmpGroupPathCAMID) Then
                    booSecurityObjFound = True
                    Dim tmpPermission(tmpPolicy.permissions.Length) As permission
                    For intSubCounter = 0 To tmpPolicy.permissions.Length - 1
                        tmpPermission(intSubCounter) = tmpPolicy.permissions(intSubCounter)
                        'If Permission Object exist use it and just adjust security
                        If tmpPolicy.permissions(intSubCounter).name = objPermission.name Then
                            booPermissionFound = True
                            tmpPolicy.permissions(intSubCounter).access = objPermission.access
                            Exit For
                        End If
                    Next
                    If Not booPermissionFound Then
                        tmpPermission(tmpPolicy.permissions.Length) = objPermission
                        tmpPolicy.permissions = tmpPermission
                    End If
                    Exit For 'Found it, we can stop the loop
                End If
            Next

            'Create Security Object
            If Not booSecurityObjFound Then
                tmpPolicy = New policy
                tmpPolicy.securityObject = bcaGroup(0)
                Dim tmpPermission(0) As permission
                tmpPermission(0) = objPermission
                tmpPolicy.permissions = tmpPermission

                Dim tmpPolicies(bcaObject(0).policies.value.Length) As policy
                For intCounter = 0 To bcaObject(0).policies.value.Length - 1
                    tmpPolicies(intCounter) = bcaObject(0).policies.value(intCounter)
                Next
                tmpPolicies(bcaObject(0).policies.value.Length) = tmpPolicy
                bcaObject(0).policies.value = tmpPolicies
            End If
            Dim UpdateOption As New updateOptions
            cmService.update(bcaObject, UpdateOption)
            SetAccess = True

        Catch ex As SoapHeaderException
            SetAccess = False
        End Try

    End Function

    Public Function deleteUserFromPermissions(ByVal userSearchPath As String, _
                ByRef cmService As contentManagerService1, ByVal strObjectPath As String) As Boolean

        Dim bcaObject() As baseClass
        Dim found As New Boolean
        Dim tmpPolicy As New policy
        Dim i As Integer

        booReturn = GetPath(strObjectPath, bcaObject, cmService)
        'keep trak if the aecurity object was found in the permissions
        found = False

        For i = 0 To bcaObject(0).policies.value.Length - 1 And Not found
            tmpPolicy = bcaObject(0).policies.value(i)
            'Find out if the security object already exists
            If (tmpPolicy.securityObject.searchPath.value.Equals(userSearchPath)) Then
                found = True
            End If
        Next
        'if the security object exists, remove it from the array of permissions
        If found Then
            '{
            Dim newPolicies(bcaObject(0).policies.value.Length - 2) As policy
            Dim policyArrayProp As New policyArrayProp
            For i = 0 To bcaObject(0).policies.value.Length - 1
                tmpPolicy = bcaObject(0).policies.value(i)
                If Not tmpPolicy.securityObject.searchPath.value.Equals(userSearchPath) Then
                    newPolicies(i) = tmpPolicy
                End If
            Next
            'If the security object does not exist, nothing to do
            policyArrayProp.value = newPolicies
            bcaObject(0).policies = policyArrayProp
            cmService.update(bcaObject, New updateOptions)
            '}
        End If
    End Function

    Private Sub Button1_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles Button1.Click

        Dim C8URL As String = "http://localhost:9330/p2pd/servlet/dispatch"
        Dim strNameSpace As String = "DLDAP"
        Dim strUserName As String = "admin"
        Dim strPassword As String = "password"
        Dim strObjectPath As String = "/content/package[@name='GO Sales and Retailers']/folder[@name='Documentation Report Samples']"
        Dim strSecurityPath As String = "CAMID(""DLDAP:u:uid=dd,ou=people"")"
        Dim typeAccess As String = "write"   'what kind of access - read, write, traverse, etc.
        Dim cmService As New contentManagerService1
        Dim result As New Boolean

        'Initialize Cognos 8 service
        cmService.Url = C8URL

        result = Logon(strNameSpace, strUserName, strPassword, cmService)

        'Uncomment the following line if you want to add access for specific user/group/role
        ' result = SetAccess(strObjectPath, strSecurityPath, typeAccess, 0, cmService)

        result = deleteUserFromPermissions(strSecurityPath, cmService, strObjectPath)

    End Sub
End Class
