import os
import re

def main(argv):
	
	dcpgDir = r"\\tyson.com\tysondata\WHQ\App_Data\InternalSalesReporting\Financials\DCPG Reports"
	starDir = r"\\tyson.com\tysondata\WHQ\App_Data\InternalSalesReporting\Financials\Star Reports"
	
	pgdDCPGDir = r"\\tyson.com\tysondata\WHQ\App_Data\InternalSalesReporting\Financials\PGD DCPG Reports"
	pgdStarDir = r"\\tyson.com\tysondata\WHQ\App_Data\InternalSalesReporting\Financials\PGD Star Reports"
	
	# All product group detail old report numbers
	pattern = re.compile(r".*352\.csv|.*353\.csv|.*357\.csv|.*358\.csv|.*355\.csv|.*356\.csv")
	
	for file in os.listdir(dcpgDir):
		if(pattern.match(file)):
			# move to PGD dcpg directory
			print("Moving: %s\n%s" % (os.path.join(dcpgDir, file), os.path.join(pgdDCPGDir, file)))
			os.rename(os.path.join(dcpgDir, file), os.path.join(pgdDCPGDir, file))
	
	for file in os.listdir(starDir):
		if(pattern.match(file)):
			# move to PGD dcpg directory
			print("Moving: %s\n%s" % (os.path.join(starDir, file), os.path.join(pgdStarDir, file)))
			os.rename(os.path.join(starDir, file), os.path.join(pgdStarDir, file))
			

if(__name__ == "__main__"):
	main(None)