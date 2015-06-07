Function extractCode(attribute As String) As String
	Return System.Text.RegularExpressions.Regex.Match(attribute, "\((.*)\)").Groups(1).Value
End Function

Function extractDescription(attribute As String) As String
	Return Trim(System.Text.RegularExpressions.Regex.Match(attribute, "\(.*\)(.*)").Groups(1).Value)
End Function