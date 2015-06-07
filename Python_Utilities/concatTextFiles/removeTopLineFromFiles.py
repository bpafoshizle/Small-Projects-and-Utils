import sys
import os
import time
import getopt
import shutil
from sets import Set

def main(argv):
	sourceDir = None
	destDir = None
	
	saveFilePath = None
	inFileNames = None
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "s:d:", ["source=","dest="])
	except getopt.GetoptError:
			usage()
			sys.exit(2)
			
	for opt, arg in opts:
		if opt in ("-s", "--source"):
			sourceDir = arg
		elif opt in ("-d", "--dest"):
			destDir = arg
		else:
			assert False, "unhandled option"
	
	# Make sure these variables are defined
	if sourceDir is None or destDir is None:
		usage()
		sys.exit(2)
	
	inFileNames = [element for element in os.listdir(sourceDir) if os.path.isfile(os.path.join(sourceDir, element))]
		
	# Make sure that files were actually found in the subdir to be combined
	if inFileNames:
		removeTopLines(inFileNames, sourceDir, destDir)
		inFileNames = None
	
# Function to loop through a list of files and combine them
def removeTopLines(inFileNames, sourceDir, destDir):
	# If there is at least 1 file 
	if  inFileNames[0] != "":
		# Loop through each text file
		for fileName in inFileNames:
			inFilePath = os.path.join(sourceDir, fileName)
			saveFilePath = os.path.join(destDir, fileName)
			print "Deleteing top line from: ", inFilePath
			print "Writing out to: ", saveFilePath
			removeTopLineFromFile(inFilePath, saveFilePath)

# Function to append 2 files	
def removeTopLineFromFile(inFilePath, saveFilePath):
	fin = open(inFilePath, "rb")
	fileLines = fin.readlines()
	fin.close()
	
	fout = open(saveFilePath, "wb")
	fout.write(''.join(fileLines[1:]))
	fout.close()
	
	
	
		
# Function to print usage
def usage():
	print "\nUsage:\nconcatText.py -s <srcDir> | --source <srcDir>, -d <dstDir> | --dest <dstDir>"
		
if __name__ == "__main__":
	main(sys.argv[1:])
