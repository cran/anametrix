.packageName <- 'anametrix'

library(RCurl)
library(XML)

#Region Query
constructQueryXML <- function(auth, reportSuiteId, tableObject, columns, datestart, dateend, count, dateFormat, sortColumn, sortDirection) {
  escapedToken <- curlEscape(auth$authtoken)
  
  root <- newXMLNode("executeQuery", attrs = c('token' = toString(escapedToken)), namespaceDefinitions = 'http://api.anametrix.com/api-envelope.xsd')
  level1 <- newXMLNode("query", parent = root)
  uid <- paste("R-Module", randomAlphaNumericUID(), sep="-")
  level2 <- newXMLNode("query", attrs = c('id'=uid,'resultSetMode'='stream',
                                          'resultSetFormat'='csv'), namespaceDefinitions = toString(" "), parent =level1)
  level3 <- newXMLNode("environment", parent =level2)
  dateranges <- newXMLNode("dateRanges", parent =level3)
  newXMLNode("range", attrs = c('start'=toString(datestart), 'end'=toString(dateend)), parent=dateranges)
  reportSuite <- newXMLNode("reportSuite", reportSuiteId , parent=level3)
  
  query <- newXMLNode("query", attrs =   c(	'minGranularity'='hour', 
                                            'table'=toString(xmlAttrs(tableObject$tableXML)["tableName"]),
                                            'dateColumn'='date',
                                            'start'='0',
                                            'count'=count), parent = level2)
  
  columnsXML <- newXMLNode("columns", parent=query)
  mIndex=1;
  cIndex=1;
  tableColumnTypeId = 1;
  dateFormat <- getDBDateFormat(dateFormat)
  returnAsDate = as.character(dateFormat) == 'hour' || as.character(dateFormat) == 'day' || as.character(dateFormat) == 'month' || as.character(dateFormat) == 'year'
  sortColumnInternalName = 'm_1'
  for (columnName in columns) {
   
    tableColumnTypeId = getColumnTypeId(columnName, tableObject)
    if(tableColumnTypeId == 2) {
      internalname <- paste(sep="_", "m", mIndex)
      mIndex = mIndex + 1
    } else {
      internalname <- paste(sep="_", "c", cIndex)
      cIndex = cIndex + 1
    }
    if(as.character(columnName) == 'date') {
      newXMLNode("column", attrs = c(	'name'=internalname, 
                                      'columnName'=toString(columnName),
                                      'returnAsDate'=tolower(returnAsDate),
                                      'dateFormat'=dateFormat), parent=columnsXML)
    }
    else {
      newXMLNode("column", attrs = c(	'name'=internalname, 
                                      'columnName'=toString(columnName)), parent=columnsXML)
    }
    if(as.character(columnName) == sortColumn) {
      sortColumnInternalName = internalname
    }
  }
    
  sortOrderXML <- newXMLNode("sortOrder", parent=query)
  sortOrderColumnXML <- newXMLNode("column", attrs = c(	'name'=sortColumnInternalName, 
                                                     'order'=toupper(sortDirection)), parent=sortOrderXML)
  
  return(root)
}

queryReader <-
  function(columns, verbose) {
    #Function that appends the values to the centrally stored vector
    i = 1
    batchsize = 1
    needToStore <- FALSE
    #d <- data.frame(matrix(nrow=1, ncol=length(columns)))
    
    filename <- tempfile(pattern = "file", tmpdir = tempdir(), fileext=".txt")
    fileCon <- file(filename, "wt") # a file connection, opened for writing text
    #print(filename)
    
    read <- function(chunk) {
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

#Region Getters
getColumnTypeId <- function(cName, tableObject) {
  columnListXML <- tableObject$tableXML[[1]]
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

#Region Support Methods
getDBDateFormat <- function(df) {
  newdatef <-  ifelse(df=='hour',  "yyyy-MM-dd HH:mm:ss",
               ifelse(df=='day',   "yyyy-MM-dd",
               ifelse(df=='month', "yyyy-MM-01",
               ifelse(df=='year',  "yyyy-01-01", df))))
  return(newdatef)
}

randomAlphaNumericUID <- function(n=1, length=10) {
  randomString <- c(1:n)
  for (i in 1:n) {
    randomString[i] <- paste(sample(c(0:9, letters, LETTERS), length, replace=TRUE), collapse="")
  }
  return( toupper(randomString))
}

#End Region Support Methods