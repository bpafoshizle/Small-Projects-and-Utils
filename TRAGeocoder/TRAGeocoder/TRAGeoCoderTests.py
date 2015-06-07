from . import *

def testAll():
	testGeocode()

""" Function to test renaming reports """
def testGeocode():
	print("\r\n********  BEGIN TEST FOR testGeocode()  **************")
	geo = GoogleGeocoder()
	geo.geocode(address="4901 Avondale Ln. Springdale, AR 72762")
	print(geo.getLatLon())
	