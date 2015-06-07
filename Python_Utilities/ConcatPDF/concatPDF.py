import sys
import os
import time
import getopt
from sets import Set

from pyPdf import PdfFileWriter, PdfFileReader

def main(argv):
	sourceDir = None
	destDir = None
	subDirs = None
	
	pdfsToCombine = None
	saveFileName = None
	
	try:
		opts, args = getopt.getopt(sys.argv[1:], "s:d:i:", ["source=","dest=","include="])
	except getopt.GetoptError:
			usage()
			sys.exit(2)
			
	for opt, arg in opts:
		if opt in ("-s", "--source"):
			sourceDir = arg
		elif opt in ("-d", "--dest"):
			destDir = arg
		elif opt in ("-i", "--include"):
			subDirs = arg
			subDirs = subDirs.split(",")
		else:
			assert False, "unhandled option"
	
	# Make sure these variables are defined
	if sourceDir is None or destDir is None:
		usage()
		sys.exit(2)
		
	
	
	pdfsToCombine = []
	saveFileName = "default.pdf"

	# Get current timestamp in yyyymmdd format
	date = time.strftime("%Y%m%d", time.localtime())

	# Get just the sub directories 
	sourceSubDirs = [element for element in os.listdir(sourceDir) if os.path.isdir(os.path.join(sourceDir, element))]
	
	# If directories were specified on command line,
	# turn the two lists into sets, and perform and intersection
	if subDirs:
		sourceSubDirSet = Set(sourceSubDirs)
		subDirsSet = Set(subDirs)
		sourceSubDirSet = sourceSubDirSet.intersection(subDirsSet)
		sourceSubDirs = list(sourceSubDirSet)
	
	# Loop through all the subdirectories in the given input directory
	# and do the business
	for subDir in sourceSubDirs:
		# Set up the output file with destination path and formatted with date
		saveFileName = os.path.join(destDir, subDir + "_" + date + ".pdf")
		
		# List the elements in the subDir and pull out the ones that are files only
		# Assumes that only pdfs to be combined will be in each subdirectory
		subDirPdfFiles = [os.path.join(os.path.join(sourceDir, subDir), e) for e in os.listdir(os.path.join(sourceDir, subDir))]
		pdfsToCombine = [e for e in subDirPdfFiles if os.path.isfile(e)]
		
		# Make sure that PDFs were actually found in the subdir to be combined
		if pdfsToCombine:
			combine(pdfsToCombine, saveFileName)
		
		pdfsToCombine = None
	
# Function to loop through a list of pdfs and combine them
def combine(pdfsToCombine, saveFileName):
	
	# If there is at least 1 file 
	if  pdfsToCombine[0] != "" and saveFileName != "":
		output = PdfFileWriter()
		
		# Loop through each pdf file entered
		for pdf in pdfsToCombine:
			print "Combining: ", pdf
			input = file(pdf, "rb")
			append_pdf(PdfFileReader(input), output)
			#input.close() # Can't do this because it doesn't seem to actually try to read the file until it writes it
		
		print "Writing to file: ", saveFileName
		output.write(file(saveFileName, "wb"))
	
	pdfsToCombine = []
	saveFileName = "default.pdf"

# Function to append 2 pdfs	
def append_pdf(input, output):
	for page_num in range(input.numPages):
		output.addPage(input.getPage(page_num))
		
# Function to print usage
def usage():
	print "\nUsage:\nconcatPDF.py -s <srcDir> | --source <srcDir>, -d <dstDir> | --dest <dstDir>, [-i subDir1,subDir2,... | --include subDir1,subDir2,...]"
		
if __name__ == "__main__":
	main(sys.argv[1:])
