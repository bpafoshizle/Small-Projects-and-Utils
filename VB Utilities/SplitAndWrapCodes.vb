Function splitAndWrapCodes(dimension As String, attr As String,  codeString As String) As String()
	Dim codes() As String = Split(codeString, ",")
	Dim code As String = ""
	
	For i As Integer = 0 To codes.Length - 1
		code = Trim(codes(i))
		If(code = "All") Then
			codes(i) = "[" + dimension + "].[" + attr + "].[" + code + "]"
		Else
			codes(i) = "[" + dimension + "].[" + attr + "].&[" + code + "]"
		End If
	Next
	Return codes
End Function

Function splitAndWrapCodesSciNot(dimension As String, attr As String,  codeString As String) As String()
	Dim codes() As String = Split(codeString, ",")
	Dim code As String = ""
	Dim intCode As Integer

	For i As Integer = 0 To codes.Length - 1
		intCode = CInt(Trim(codes(i)))
		code = intCode.ToString("0.##########E0")

		If(code = "All") Then
			codes(i) = "[" + dimension + "].[" + attr + "].[" + code + "]"
		Else
			codes(i) = "[" + dimension + "].[" + attr + "].&[" + code + "]"
		End If
	Next
	Return codes
End Function