.packageName <- 'anametrix'

library(RCurl)
library(XML)

getAxDataset <-
  function(auth, reportSuiteId, tableObject, columns, datestart, dateend, count, dateFormat, sortColumn, sortDirection, verbose) {
    if(is.null(auth$axapiUri) || as.character(auth$axapiUri) == "") {  
      cat("You need to specify API Endpoint URL first: newAuthentication <- authenticate(apiURL,username,password)","\n") 
      return(NULL)
    }
    if(as.character(auth$authtoken) == "" || is.null(auth$authtoken))  {	
      cat("You need to get Authentication Token first: newAuthentication <- authenticate(apiURL,username,password)","\n") 
      return(NULL)
    }
    #if(verbose)	{	options(warn=1)	}
    #else		{	options(warn=-1)	}
    
    count <- formatC(count, format = "f", digits = 0)
    queryXML <- constructQueryXML(auth, reportSuiteId, tableObject, columns, datestart, dateend, count, dateFormat, sortColumn, sortDirection)
    queryXMLreq <- paste (sep="", 'cmd=', toString.XMLNode(queryXML));
    
    h = getCurlHandle()
    reader = queryReader(columns, verbose)
    
    tryCatch({  
      getURL(auth$axapiUri, ssl.verifypeer = FALSE, followLocation = TRUE, postfields=queryXMLreq, encoding="UTF-8", curl=h,
             .opts = list(timeout = 3600, verbose = FALSE),  useragent = "R",
             write=chunkToLineReader(reader$read)$read, verbose = FALSE)
    }, error=function(err) {
      cat("Error occured when retrieving data:", conditionMessage(err), "\n")
      close(reader$fileCon())
      file.remove(reader$filename())
      return(NULL)
    })
    
    if(verbose)	print("Got response from Anametrix server...")
    
    #print(reader$filename())
    close(reader$fileCon())
    
    if(file.info(reader$filename())$size > 0) {
      DF <- read.table(reader$filename(), sep = ",", header=T)
    }else {
      cat("Error occured when retrieving data:", "Make sure you specified correct columns, table and Data suite", "\n")
      #close(reader$fileCon())
      file.remove(reader$filename())
      return(NULL)
    }
    
    file.remove(reader$filename())
    rm(reader)
    
    DF	
  }

getAxDatasetFromEncodedQuery <-
  function(auth, encodedQuery, startDate, endDate, verbose) {
    if(is.null(auth$axapiUri) || as.character(auth$axapiUri) == "") {  
      cat("You need to specify API Endpoint URL first: newAuthentication <- authenticate(apiURL,username,password)","\n") 
      return(NULL)
    }
    if(as.character(auth$authtoken) == "" || is.null(auth$authtoken))  {  
      cat("You need to get Authentication Token first: newAuthentication <- authenticate(apiURL,username,password)","\n") 
      return(NULL)
    }
    
    doc <- xmlTreeParse(base64Decode(encodedQuery))
    innerQueryXML <- xmlRoot(doc)
    xmlAttrs(innerQueryXML[["environment"]][["dateRanges"]][["range"]])["start"] <- startDate
    xmlAttrs(innerQueryXML[["environment"]][["dateRanges"]][["range"]])["end"] <- endDate
    
    uid <- paste("R-Module", randomAlphaNumericUID(), sep="-")
    
    outerQueryXML <- xmlNode("query", attrs = c('id' = uid, 'resultSetMode'='stream', 'resultSetFormat'='csv'), namespaceDefinitions = toString(" "))
    outerQueryXML [[ "environment" ]] <- innerQueryXML [["environment"]]
    outerQueryXML [[ "query" ]] <- innerQueryXML [["query"]]
    
    rootLevel <- xmlNode("query")
    rootLevel [[ "query" ]] <- outerQueryXML
    
    commandNode <- xmlNode("executeQuery", attrs = c('token' = toString(auth$authtoken)), namespaceDefinitions = 'http://api.anametrix.com/api-envelope.xsd')
    commandNode[[ "query" ]] <- rootLevel 
    
    columns <- innerQueryXML [["query"]][["columns"]]
    columnvector <- character(0)
    for(i in 1:xmlSize(columns)) {
      columnXML <- columns[[i]]
      if(!is.na(xmlAttrs(columnXML)["virtual"]))  next;
      displayName <- xmlAttrs(columnXML)["displayName"]
      displayName <- gsub("(^ +)|( +$)", "", displayName)
      columnvector <- c(columnvector, displayName)
    }
    names(columnvector) <- NULL
    
    h = getCurlHandle()
    reader = queryReader(columnvector, verbose)
    
    cmdNode <- toString(commandNode)
    cmdNode <- gsub("\\&apos;", "'", cmdNode)
    cmdNode <- curlEscape(cmdNode)
    
    #print(cmdNode)
    queryXMLreq <- paste (sep="", 'cmd=', cmdNode)
    
    tryCatch({  
      getURI(auth$axapiUri, ssl.verifypeer = FALSE, followLocation = TRUE, postfields=queryXMLreq, .encoding="UTF-8", curl=h,
             .opts = list(timeout = 3600, verbose = verbose), useragent = "R",
             write=chunkToLineReader(reader$read)$read, verbose = verbose)
      
      
    }, error=function(err) {
      cat("Error occured when retrieving data:", conditionMessage(err), "\n")
      close(reader$fileCon())
      file.remove(reader$filename())
      return(NULL)
    }, finally = {
      close(reader$fileCon())
    })
    
    if(verbose)  print("Got response from Anametrix server...")
    #print(reader$filename())
    
    #close(reader$fileCon())
    
    
    if(file.info(reader$filename())$size > 0) {
      DF <- read.table(reader$filename(), sep = ",", header=T)
    } else {
      cat("Error occured when retrieving data:", "Make sure you specified correct columns, table and Data suite", "\n")
      #close(reader$fileCon())
      file.remove(reader$filename())
      return(NULL)
    }
    
    file.remove(reader$filename())
    rm(reader)
    
    DF
  }

#Region authentication
authenticate <- function(axapiUri, username, password)  {
  if(as.character(axapiUri) == "")	{	
    cat("You need to specify API Endpoint URL correctly. API URL Invalid","\n")
    return(NULL)
  }
  authXMLreq = paste (sep="", 'cmd=<getAuthenticationToken xmlns="http://api.anametrix.com/api-envelope.xsd">', 
                      "<username>", 	username, 		"</username>
   	<password>"	,  	password, 		"</password>
                      <apikey>"	,	"R-AX-MODULE-API-KEY",	"</apikey>
                      <client>"	,	"R", 				"</client></getAuthenticationToken>")
	
  tryCatch({
    resp = getURL(axapiUri, ssl.verifypeer = FALSE, postfields=authXMLreq, .opts = list(timeout = 100), useragent = "R-Authentication")
  }, error=function(err) {
    cat("Failed to authenticate with Anametrix API","\n")
    cat("Error:", conditionMessage(err), "\n")
    return(NULL)
  })
  
  doc = xmlTreeParse(resp)
  root = xmlRoot(doc);
  
  authtoken = xmlAttrs(root)["data"];
  if(is.na(authtoken)) {
    cat("Failed to authenticate with Anametrix API","\n","Make sure you have specified correct credentials","\n")
    return(NULL)
  }
  cat("Connected...","\n")
  
  object <- list(axapiUri=axapiUri, username=username, password=password, authtoken=authtoken)
  class(object) <- "authentication"
  return (object)
}
#End Region authentication



#Region Getters
getFullTableListXML <- function(auth, reportsuiteId) {
  escapedToken <- curlEscape(auth$authtoken)
  tableListXMLreq = paste (sep="", 'cmd=	<getTableList xmlns="http://api.anametrix.com/api-envelope.xsd" token="', escapedToken, '">
  									<reportSuite>', reportsuiteId, "</reportSuite>
								</getTableList>");

  resp = getURL(auth$axapiUri, ssl.verifypeer = FALSE, postfields=tableListXMLreq, .opts = list(timeout = 100));
  
  doc = xmlTreeParse(resp);
  root = xmlRoot(doc);
  
  rm(doc)
  rm(escapedToken)
  rm(resp)
  
  return(root);
}

getTableConfiguration <- function(auth, reportsuiteId, tableName) {
  if(as.character(auth$axapiUri) == "")	{	
    cat("You need to specify API URL first: newAuthentication <- authentication(apiURL,username,password)","\n") 
    return(NULL)
  }
  
  if(as.character(auth$authtoken) == "" || is.na(auth$authtoken))	{	
    cat("You need to get Authentication Token first. Use: connect(newAuthentication)","\n") 
    return(NULL)
  }
  
  fullTableListXML <- getFullTableListXML(auth, reportsuiteId);
  tableListXML <- fullTableListXML[[1]];
  #get table xml from tablelistXML
  
  
  for(i in 1:xmlSize(tableListXML)) {
    tableXML <- tableListXML[[i]]
    name = xmlAttrs(tableXML)["name"]
    if(as.character(name) == tableName) {
      object <- list(tableName=tableName, tableXML=tableXML)
      class(object) <- "tableObject"
      return (object)
      #return(tableXML)
      break;
    }
  }
  rm(i);
  print(paste(sep=" ", "Table", tableName, "could not be found..."))
  return(NULL);
}

#End Region Getters

#Region Support functions
isdefined <- function(object) {
  exists(as.character(substitute(object)))
}

printTable <- function(tableObject) {
  cat( paste (sep=" ", "Table:", xmlAttrs(tableObject$tableXML)["name"], ", Data source:", xmlAttrs(tableObject$tableXML)["datasource"],"\n"))
  
  columnListXML <- tableObject$tableXML[[1]]
  for(i in 1:xmlSize(columnListXML)) {
    columnXML <- columnListXML[[i]]
    name = xmlAttrs(columnXML)["name"]
    columnName = xmlAttrs(columnXML)["columnName"]
    cat( paste (sep=" ", "Column:", name, ", internal name:", columnName,"\n"))
  }
  rm(i,name,columnName)
}
#End Region Support functions