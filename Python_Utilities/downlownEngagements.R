require(httr)
library("XML")


edp <- handle("http://erikadotsonphotography.com")
url <- "http://www.erikadotsonphotography.com/rebeccaandbarretengagements"
proxyUrl <- "127.0.0.1"
proxyPort <- "8080"

r <- 
   if(!is.null(proxyUrl)){
      GET(url
          ,add_headers(.headers = headers)
          ,use_proxy(proxyUrl,proxyPort) 
          ,handle=yahoo
      )            
   }