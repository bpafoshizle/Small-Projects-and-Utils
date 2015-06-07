import sys
import os
import time
import getopt
import shutil
import logging
import re
import zipfile
#from sets import Set

def main(argv):

	sourceDir = None
	searchText = None
	searchTextSplitter = None
	filePathPattern = None
	logLevel = "CRITICAL"
	
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "pat:d:s:spltr:log", ["filePathPattern=","directory=","searchText=","searchTextSplitter=","logLevel="])
	except getopt.GetoptError:
			usage()
			sys.exit(2)
			
	for opt, arg in opts:
		if opt in ("-d", "--directory"):
			sourceDir = arg
		elif opt in ("-s", "--searchText"):
			searchText = arg
		elif opt in ("-spltr", "--searchTextSplitter"):
			searchTextSplitter = arg
		elif opt in ("-pat", "--filePathPattern"):
			filePathPattern = arg
		elif opt in ("log", "--logLevel"):
			logLevel = arg
		else:
			assert False, "unhandled option"
	
	
	logging.basicConfig(filename="searchFilesForText.log", level=logLevel.upper())
	
	# Make sure these variables are defined
	if sourceDir is None:
		usage()
		sys.exit(2)
	
	inFileNames = []
	
	for root, subFolders, files in os.walk(sourceDir):
		for file in files:
			inFileNames.append(os.path.join(root,file))

	
	# Make sure that files were actually found in the subdir to be combined
	if inFileNames:
		for file in inFileNames:
			if(re.search(filePathPattern, file) <> None):
				if(file.endswith(".zip")):
					fileNameWithoutExt, fileExt = os.path.splitext(file)
					tempUnzipDirectory = os.path.join(os.path.dirname(file), os.path.basename(fileNameWithoutExt)+"_tempUnzip")
					decompressZipFileToTemp(file, tempUnzipDirectory)				
					searchTempUnzipDirForText(searchText, searchTextSplitter, tempUnzipDirectory)
					removeDir(tempUnzipDirectory)
				else:
					logging.debug("Searching %s" % file)
					searchFileForText(file, searchText, searchTextSplitter)
	
def searchTempUnzipDirForText(searchText, searchTextSplitter, tempUnzipDirectory):
		myInFileNames = []
		for root, subFolders, files in os.walk(tempUnzipDirectory):
			for file in files:
				myInFileNames.append(os.path.join(root,file))
		for file in myInFileNames:			
			searchFileForText(file, searchText, searchTextSplitter)
		
			
# Function to loop through a list of files and combine them
def searchFileForText(file, searchText, searchTextSplitter):
	f = open(file, "r")
	data=f.read()
	f.close()
	
	#m = re.match(searchText, data)
	if(searchTextSplitter <> None):
		splitText = searchText.split(searchTextSplitter)
		for text in splitText:
			logging.debug("Searching %s in %s" % (text, file))
			if(text in data):
				logging.info("%s found in file: %s" % (text, file))
				print "%s found in file: %s" % (text, file)
	else:
		logging.debug("Searching %s in %s" % (saerchText, file))
		if(searchText in data):
			logging.info("%s found in file: %s" % (text, file))
			print "%s found in file: %s" % (text, file)
		
def decompressZipFileToTemp(file, tempUnzipDirectory):
	logging.debug("Decompressing %s" % file)
	ensureDir(tempUnzipDirectory)
	zfile = zipfile.ZipFile(file)
	for name in zfile.namelist():
		(dirname, filename) = os.path.split(name)
		logging.debug("Decompressing %s on %s" % (filename, tempUnzipDirectory))
		try:
			fd = open(os.path.join(tempUnzipDirectory,filename),"w")
			fd.write(zfile.read(name))
		except IOError, e:
			logging.critical(e)
			raise
		except NotImplementedError, e:
			logging.error(e)
		except Exception, e:
			logging.critical("Unexpected error: %s" % e)
			raise
		finally:
			fd.close()
	
	
def ensureDir(d):
	if not os.path.exists(d):
		logging.debug("Creating temp directory: %s" % d)
		os.makedirs(d)
	
def removeDir(d):
	logging.debug("Removing temp directory: %s" % d)
	shutil.rmtree(d)
	
# Function to print usage
def usage():
	print "\nUsage:\nconcatText.py -s <srcDir> | --source <srcDir>, -d <dstDir> | --dest <dstDir>"
		
if __name__ == "__main__":
	main(sys.argv[1:])
