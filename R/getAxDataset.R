.packageName <- 'anametrix'

library(RCurl)
library(XML)

getAxDataset <-
  function(auth, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count, verbose) {
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
    queryXML <- constructQueryXML(auth, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count)
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
      return(tableXML)
      break;
    }
  }
  rm(i);
  print(paste(sep=" ", "Table", tableName, "could not be found..."))
  return(NULL);
}

getColumnTypeId <- function(cName, tableConfigurationXML) {
  columnListXML <- tableConfigurationXML[[1]]
  for(i in 1:xmlSize(columnListXML)) {
    columnXML <- columnListXML[[i]]
    columnName = xmlAttrs(columnXML)["columnName"]
    columnTypeId = as.numeric(xmlAttrs(columnXML)["tableColumnTypeId"])
    if(as.character(columnName) == cName) {
      return(columnTypeId)
    }
  }
  rm(i)
  rm(columnName)
  return(columnTypeId)
}

#End Region Getters

#Region Query
constructQueryXML <- function(auth, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count) {
  escapedToken <- curlEscape(auth$authtoken)
  
  root <- newXMLNode("executeQuery", attrs = c('token' = toString(escapedToken)), namespaceDefinitions = 'http://api.anametrix.com/api-envelope.xsd')
  level1 <- newXMLNode("query", parent = root)
  level2 <- newXMLNode("query", attrs = c('id' ='R-module','resultSetMode'='stream',
                                          'resultSetFormat'='csv'), namespaceDefinitions = toString(" "), parent =level1)
  level3 <- newXMLNode("environment", parent =level2)
  dateranges <- newXMLNode("dateRanges", parent =level3)
  newXMLNode("range", attrs = c('start'=toString(datestart), 'end'=toString(dateend)), parent=dateranges)
  reportSuite <- newXMLNode("reportSuite", reportSuiteId , parent=level3)
  
  query <- newXMLNode("query", attrs = 	c(	'minGranularity'='hour', 
                                           'table'=toString(xmlAttrs(tableConfigurationXML)["tableName"]),
                                           'dateColumn'='date',
                                           'start'='0',
                                           'count'=count), parent = level2)
  
  columnsXML <- newXMLNode("columns", parent=query)
  mIndex=1;
  cIndex=1;
  tableColumnTypeId = 1
  for (columnName in columns) {
    tableColumnTypeId = getColumnTypeId(columnName, tableConfigurationXML)
    if(tableColumnTypeId == 2) {
      internalname <- paste(sep="_", "m", mIndex)
      mIndex = mIndex + 1
    } else {
      internalname <- paste(sep="_", "c", cIndex)
      cIndex = cIndex + 1
    }
    if(as.character(columnName)=='date') {
      newXMLNode("column", attrs = c(	'name'=internalname, 
                                      'columnName'=toString(columnName),
                                      'returnAsDate'='true',
                                      'dateFormat'='day'), parent=columnsXML)
    }
    else {
      newXMLNode("column", attrs = c(	'name'=internalname, 
                                      'columnName'=toString(columnName)), parent=columnsXML)
    }
  }
  
  sortOrder <- newXMLNode("sortOrder", parent=query)
  sortOrderColumn <- newXMLNode("column", attrs = c(	'name'='m_1', 
                                                     'order'='DESC'), parent=sortOrder)
  
  return(root)
}

queryReader=
  function(columns, verbose) {
    #Function that appends the values to the centrally stored vector
    i = 1
    batchsize = 1
    needToStore <- FALSE
    #d <- data.frame(matrix(nrow=1, ncol=length(columns)))
    
    filename <- tempfile(pattern = "file", tmpdir = tempdir(), fileext=".txt")
    fileCon <- file(filename, "wt") # a file connection, opened for writing text
    #print(filename)
    read = function(chunk) {
      con = textConnection(chunk)
      on.exit(close(con))
      #print(as.character(chunk))
      while (length(oneLine <- readLines(con, n = batchsize, warn = TRUE, ok = TRUE)) > 0) {
        if(needToStore) {
          tryCatch({
            c <- textConnection(oneLine)
            tempDF <- read.table(c, header = FALSE, sep=",", quote = "\"", comment.char="", as.is = TRUE, blank.lines.skip = TRUE)
            
            if(ncol(tempDF) == length(columns)) {
              if(i == 1) {
                names(tempDF) = columns
                write.table(tempDF, file=fileCon, sep = ",", col.names = TRUE, row.names=FALSE, qmethod = "double")
              }
              else {
                write.table(tempDF, file=fileCon, sep = ",", col.names = FALSE, row.names=FALSE,qmethod = "double")
              }
              
              i <<- i + 1
            }
            rm(tempDF)
            close(c)
          }, error=function(err) {
            if(verbose) {
              cat("Error when parsing line:", conditionMessage(err), "\n")
              cat("Corrupted line:", oneLine, "\n")
              cat("\n")
            }   
          })
        }
        else if(as.character(oneLine) == '</result>')	{
          needToStore <<- TRUE
          batchsize <<- 200
        }
      }
    }
    list(read = read,
         fileCon = function() fileCon,
         filename = function() filename #accessor to get result on completion
    )
  }

#End Region Query


#Region Support functions
isdefined <- function(object) {
  exists(as.character(substitute(object)))
}

printTable <- function(tableConfigurationXML) {
  cat( paste (sep=" ", "Table:", xmlAttrs(tableConfigurationXML)["name"], ", Data source:", xmlAttrs(tableConfigurationXML)["datasource"]))
  
  columnListXML <- tableConfigurationXML[[1]]
  for(i in 1:xmlSize(columnListXML)) {
    columnXML <- columnListXML[[i]]
    name = xmlAttrs(columnXML)["name"]
    columnName = xmlAttrs(columnXML)["columnName"]
    cat( paste (sep=" ", "Column:", name, ", internal name:", columnName,"\n"))
  }
  rm(i,name,columnName)
}
#End Region Support functions
