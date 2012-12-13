.packageName <- 'anametrix'

library(RCurl)
library(XML)

uploadData <-
function(auth, reportSuiteId, tableObject, dataframe, columns, tag, truncation) {
  
  uploadXML <- constructUploadXML(auth, reportSuiteId, tableObject, dataframe, columns, tag, truncation)
  if(is.null(uploadXML)) {
    cat("Unable to upload data.","\n")
    return(NULL)
  }
  queryXMLreq <- paste (sep="", 'cmd=', toString.XMLNode(uploadXML));
  tryCatch({
    h = getCurlHandle()
    getURL(auth$axapiUri, ssl.verifypeer = FALSE, postfields=queryXMLreq, encoding="UTF-8", curl=h,
           .opts = list(timeout = 1600, verbose = TRUE), useragent = "R", verbose = FALSE)
  }, error=function(err) {
    cat("Error while uploading data:", conditionMessage(err), "\n")
    rm(h)
    return(NULL)
  })
  
  rm(h)
  cat("Upload Successful...","\n")
}

rowToXML <- function(ds, columns, header) {
  node <- newXMLNode("row")
  
  for (columnName in columns) {
    col <- grep(columnName, header)
    value <- sub("^ +", "", ds[col]) #trim leading white space
    xmlAttrs(node)[columnName] = value
  }
  rm(col,value)
  node
}

constructUploadXML <-  function(auth, reportSuiteId, tableObject, dataframe, columns, tag, truncation) {
  escapedToken <- curlEscape(auth$authtoken)
  
  root <- newXMLNode("uploadData", attrs = c('token' = toString(escapedToken)), namespaceDefinitions = 'http://api.anametrix.com/api-envelope.xsd')
  properties <- newXMLNode("properties", parent = root)
  newXMLNode("reportSuite", reportSuiteId , parent=properties)
  
  if(!is.null(tag))  newXMLNode("tag", tag , parent=properties)
	currentDate <- format(Sys.time(), "%Y-%m-%d")
  newXMLNode("date", currentDate , parent=properties)
  if(is.null(tableObject$tableXML) || as.character(tableObject$tableXML) == ""){
    cat("Unable to upload data. Table not specified.","\n")
    return(NULL)
  }
  newXMLNode("table", xmlAttrs(tableObject$tableXML)["tableName"], parent=properties)
  if(!is.null(truncation)) 
    newXMLNode("truncation", truncation , parent=properties)
  else    
    newXMLNode("truncation", "FALSE" , parent=properties)
  
  level1 <- newXMLNode("data", parent = root)
  rowsparent <- newXMLNode("data", namespaceDefinitions = toString(" "), parent = level1)
  
  rows <- apply(dataframe, 1, rowToXML, columns=columns, header=colnames(dataframe))
  addChildren(rowsparent, rows)
  
  rm(rows)
  
  return(root)
}