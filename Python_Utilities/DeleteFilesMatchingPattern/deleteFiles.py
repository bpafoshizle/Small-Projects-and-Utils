import os
import re

def main(argv):
	
	#sourceDir = r"C:\\Documents and Settings\\MILLERBARR\\Desktop\\DeleteTest"
	sourceDir = r"\\tyson.com\tysondata\WHQ\App_Data\InternalSalesReporting\Financials\Reports\Monday"
	dirStack = [sourceDir]
	patternString = r".*NMR.*\.pdf$"
	pattern = re.compile(patternString)
	numDeleted = 0
	
	# Loop through breadth first searching of directories
	while(dirStack):
		currentDir = dirStack.pop()
		# Get just the sub directories, and add them to the stack
		dirStack.extend([os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isdir(os.path.join(currentDir, child))])
		
		# Go through just the files in the current directory and grab the greatest modDate
		fileList = [os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isfile(os.path.join(currentDir, child))]
		numDeleted += deleteFilesMatchingPattern(pattern, fileList)

	print("Deleted %s files total" % (numDeleted))
	
def deleteFilesMatchingPattern(pattern, fileList):
	numFilesDeleted = 0
	for curFile in fileList:
		if(pattern.match(curFile)):
			# delete file and print deletion
			numFilesDeleted += 1
			print("Deleting: %s" % (curFile))
			os.remove(curFile)
		
	return numFilesDeleted

if(__name__ == "__main__"):
	main(None)