""" This program takes a top level directory as an argument. It will
	iterate through all files, in all sub directories and produce the
	greatest mod time that it sees across all files along with the file's 
	path """

import sys
import os
import time

def main(argv):
	
	sourceDir = None
	dirStack = None
	curGreatestDict = {"fileName":None, "modTime":0}
	timeString = None
	
	
	# Make sure argument is a valid directory
	if(argv == None or (not os.path.isdir(argv[0]))):
		usage()
		sys.exit(2)
	
	# Start with the source directory on the stack
	sourceDir = argv[0]
	dirStack = [sourceDir]
	
	print("Searching...")
	
	# Loop through breadth first searching of directories
	while(dirStack):
		currentDir = dirStack.pop()
		# Get just the sub directories, and add them to the stack
		dirStack.extend([os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isdir(os.path.join(currentDir, child))])
		
		# Go through just the files in the current directory and grab the greatest modDate
		fileList = [os.path.join(currentDir, child) for child in os.listdir(currentDir) if os.path.isfile(os.path.join(currentDir, child))]
		curGreatestDict = getGreaterModTimeDict(curGreatestDict, fileList)
		
	timeString = "%s-%s-%s %s:%s:%s" % (curGreatestDict["modTime"].tm_mon, curGreatestDict["modTime"].tm_mday, curGreatestDict["modTime"].tm_year, curGreatestDict["modTime"].tm_hour, curGreatestDict["modTime"].tm_min, curGreatestDict["modTime"].tm_sec)
	print("Greatest mod time: '%s' \nFile:%s" % (timeString, curGreatestDict["fileName"]))
	

	# Function to print usage
def usage():
	print "\nUsage: GetGreatestModTime.py full_path_to_source_dir"

""" Compares the current greatest time dictionary to the current
 	file's mod time, and if the current file has been modified
    later, then the new filename and mod time are put in the 
    dictionary """
def getGreaterModTimeDict(curGreatestDict, fileList):
	for curFile in fileList:
		curGreatTime = curGreatestDict["modTime"]
		curFileTime = time.localtime(os.path.getmtime(curFile))
		if(curGreatTime < curFileTime):
			curGreatestDict["fileName"] = curFile
			curGreatestDict["modTime"] = curFileTime
		
	return curGreatestDict
	
if __name__ == "__main__":
	main(sys.argv[1:])