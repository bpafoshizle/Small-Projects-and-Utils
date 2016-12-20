# -*- coding: utf-8 -*-
"""
Created on Mon Sep  7 11:35:44 2015

@author: bpafoshizle
"""

import requests
from lxml import html

proxies = {
  "http": "http://127.0.0.1:8080",
  "https": "http://127.0.0.1:8080"
}

s = requests.Session()

landingUrl = "http://www.erikadotsonphotography.com/clients.html"
r = s.get(landingUrl, proxies = proxies, verify=False)

loginUrl = "http://www.erikadotsonphotography.com/zf/layout/client.asmx"
payload = {
    "id": 0, 
    "method" : "ClientAccessSubmit",
    "params" : [{"code" : "rebeccaandbarret", "username" : "erikadotson"}]
}
r = s.post(loginUrl, json=payload
               ,proxies=proxies, verify=False)

listUrl = "http://www.erikadotsonphotography.com/rebeccaandbarretengagements"
r = s.get(listUrl, proxies = proxies, verify=False)
    
with open("list.html","wb") as f:
    f.write(r.content)
    
tree = html.fromstring(r.content)

imgLinks = tree.xpath("//a[@class='pv-inner']/@href")