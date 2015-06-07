""" This program queries Active Directory for all the groups a user 
	belongs to that match a given pattern. It outputs the line-delimited list
	to the clipboard for easy pasting. """

import sys
import re
import win32net as wn
import win32clipboard as wcb
import win32con

userName = sys.argv[1]
filter = re.compile(sys.argv[2])
groupList = []
resultText = ""

for group in wn.NetUserGetGroups('tyson.com', userName):
	if filter.match(group[0]):
		groupList.append(str(group[0]))


groupList.sort()

resultText = "\r\n".join(groupList)

wcb.OpenClipboard()
wcb.EmptyClipboard()
wcb.SetClipboardData(win32con.CF_TEXT, resultText)
wcb.CloseClipboard()