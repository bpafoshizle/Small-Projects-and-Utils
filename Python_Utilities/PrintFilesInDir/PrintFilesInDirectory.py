import os
import re
import win32net as wn
import win32clipboard as wcb
import win32con

def main(argv):
	
	#sourceDir = r"C:\\Documents and Settings\\MILLERBARR\\Desktop\\DeleteTest"
	sourceDir = r"\\whqwssissb02\ssis\packages\tyson\bi_prod_copy"
	dirStack = [sourceDir]
	patternString = r".*"
	pattern = re.compile(patternString)
	resultText = ""
	
	# Loop through breadth first searching of directories
	while(dirStack):
		currentDir = dirStack.pop()
		# Get just the sub directories, and add them to the stack
		dirStack.extend([os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isdir(os.path.join(currentDir, child))])
		
		# Go through just the files in the current directory and grab the greatest modDate
		fileList = [os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isfile(os.path.join(currentDir, child))]
		resultText += printFilesMatchingPattern(pattern, fileList)
	
	wcb.OpenClipboard()
	wcb.EmptyClipboard()
	wcb.SetClipboardData(win32con.CF_TEXT, resultText)
	wcb.CloseClipboard()		

	
def printFilesMatchingPattern(pattern, fileList):
	myResultText = ""
	for curFile in fileList:
		if(pattern.match(curFile)):
			print((curFile))
			myResultText += curFile + "\r\n"
		
	return myResultText

if(__name__ == "__main__"):
	main(None)