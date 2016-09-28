import json
import urllib
raw_json = urllib.urlopen("http://40.84.148.12:9995/api/notebook").read()
parsed_json = json.loads(raw_json)
urlList = ['curl -X "DELETE"  http://40.84.148.12:9995/api/notebook/' + e["id"] for e in parsed_json["body"] if e["name"] ==  "Workshop-Analyzing_Web_Log_Files"]
for u in urlList: print(u)
