import sys
import os
import time
import getopt
import shutil
from sets import Set

def main(argv):
	sourceDir = None
	destDir = None
	
	filesToCombine = None
	saveFileName = None
	
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
	
	filesToCombine = []

	# Get current timestamp in yyyymmdd format
	date = time.strftime("%Y%m%d", time.localtime())
	saveFileName = "combined-" + date + ".txt"
	
	textFiles = [os.path.join(sourceDir, element) for element in os.listdir(sourceDir) if os.path.isfile(os.path.join(sourceDir, element))]
		
	# Make sure that the two files were actually found in the subdir to be combined
	if textFiles:
		combine(textFiles, os.path.join(destDir, saveFileName))
		textFiles = None
	
# Function to loop through a list of files and combine them
def combine(filesToCombine, saveFileName):
	
	# If there is at least 1 file 
	if  filesToCombine[0] != "" and saveFileName != "":
		fout = open(saveFileName, "wb")
		# Loop through each text file
		for fileName in filesToCombine:
			print "Combining: ", fileName
			append_files(fileName, fout)
	
	fout.close()
	filesToCombine = []
	saveFileName = "combined.txt"

# Function to append 2 files	
def append_files(inputName, fout):
	fin = open(inputName, "rb")
	shutil.copyfileobj(fin, fout)
	fin.close()
	
		
# Function to print usage
def usage():
	print "\nUsage:\nconcatText.py -s <srcDir> | --source <srcDir>, -d <dstDir> | --dest <dstDir>"
		
if __name__ == "__main__":
	main(sys.argv[1:])
