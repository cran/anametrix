.packageName <- 'anametrix'

library(RCurl)
library(XML)

getAxDataset <- function(axapiUri, authtoken, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count) {
	queryXML <- constructQueryXML(authtoken, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count)
	queryXMLreq <- paste (sep="", 'cmd=', toString.XMLNode(queryXML));

	resp = getURL(axapiUri, ssl.verifypeer = FALSE, postfields=queryXMLreq , verbose = TRUE, encoding="UTF-8", .opts = list(timeout = 15))
	print("Got response from Anametrix server...")

	inputcon <- textConnection(resp)

	#d <- rbind(columns)

	xmlpart <- ""
	rowcount <- 100

	needToStore = FALSE
	i = 1;
	while (length(oneLine <- readLines(inputcon, n = 1, warn = TRUE)) > 0) {
		if(needToStore) {
			v <- unlist(strsplit(oneLine, split=","))
			if(length(v) == length(columns))	d[i, ] <- v
			i = i + 1;
		} else {
			xmlpart <- paste(xmlpart, oneLine, sep = "")
		}

		if(as.character(oneLine) == '</result>')	{
			needToStore = TRUE
			doc = xmlTreeParse(xmlpart)
			root = xmlRoot(doc)
			rowcount <- as.integer(xmlAttrs(root[[1]])["totalRowCount"])	
			
			d <- data.frame(matrix(nrow=rowcount,ncol=length(columns)))
			
			#print(doc)
			rm(root)
			rm(xmlpart)
		}
	}

	close(inputcon)
	rm(resp)
	
	#d=as.data.frame(d, stringsAsFactors=F)
	names(d) = columns
	
	return(d)
}

getToken <- function(axapiUri, username, password) {
  authXMLreq = paste (sep="", 'cmd=<getAuthenticationToken xmlns="http://api.anametrix.com/api-envelope.xsd">', 
  "<username>", 	username, 		"</username>
   <password>"	,  	password, 		"</password>
   <apikey>"	,	"R-AX-MODULE-API-KEY",	"</apikey>
   <client>"	,	"R", 				"</client></getAuthenticationToken>")
	
	print("token requested...");

  	resp = getURL(axapiUri, ssl.verifypeer = FALSE, postfields=authXMLreq, .opts = list(timeout = 10))
	#print(resp)
	doc = xmlTreeParse(resp)
	root = xmlRoot(doc);
	authtoken = xmlAttrs(root)["data"];

	print("token received...");

  	return(authtoken)
}

getFullTableListXML <- function(axapiUri, authtoken, username, password, reportsuiteId) {
	escapedToken <- curlEscape(authtoken)
	tableListXMLreq = paste (sep="", 'cmd=	<getTableList xmlns="http://api.anametrix.com/api-envelope.xsd" token="', escapedToken, '">
  									<reportSuite>', reportsuiteId, "</reportSuite>
								</getTableList>");

	resp = getURL(axapiUri, ssl.verifypeer = FALSE, postfields=tableListXMLreq, verbose = TRUE, .opts = list(timeout = 10));
	
	doc = xmlTreeParse(resp);
	root = xmlRoot(doc);
	
	rm(doc)
	rm(escapedToken)
	rm(resp)

	return(root);
}

getTableConfiguration <- function(axapiUri, authtoken, username, password, reportsuiteId, tableName) {
	if(!exists("authtoken")){
		print("token doesn't exist...")
		print("you need to request token first, use: getToken(username, password)")
		return("token needed");
	}

      fullTableListXML <- getFullTableListXML(axapiUri, authtoken, username, password, reportsuiteId)
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


printTable <- function(tableConfigurationXML) {
	print( paste (sep=" ", "Table:", xmlAttrs(tableConfigurationXML)["name"], ", Data source:", xmlAttrs(tableConfigurationXML)["datasource"]))

	columnListXML <- tableConfigurationXML[[1]]
	for(i in 1:xmlSize(columnListXML)) {
		columnXML <- columnListXML[[i]]
		name = xmlAttrs(columnXML)["name"]
		columnName = xmlAttrs(columnXML)["columnName"]
		print( paste (sep=" ", "Column:", name, ", internal name:", columnName))
	}
	rm(i)
	rm(name)
	rm(columnName)
}

constructQueryXML <- function(authtoken, reportSuiteId, tableConfigurationXML, columns, datestart, dateend, count) {
	escapedToken <- curlEscape(authtoken)

	root <- newXMLNode("executeQuery", attrs = c('token' = toString(escapedToken)), namespaceDefinitions = 'http://api.anametrix.com/api-envelope.xsd')
	level1 <- newXMLNode("query", parent = root)
	level2 <- newXMLNode("query", attrs = c('id' ='R','resultSetMode'='stream',
									  'resultSetFormat'='csv'), namespaceDefinitions = toString(" "), parent =level1)
	level3 <- newXMLNode("environment", parent =level2)
	dateranges <- newXMLNode("dateRanges", parent =level3)
	range <- newXMLNode("range", attrs = c('start'=toString(datestart), 'end'=toString(dateend)), parent=dateranges)
	reportSuite <- newXMLNode("reportSuite", reportSuiteId , parent=level3)

	query <- newXMLNode("query", attrs = 	c(	'minGranularity'='hour', 
									'table'=toString(xmlAttrs(tableConfigurationXML)["tableName"]),
									'dateColumn'='date',
									'start'='0',
									'count'=count
								), 
					parent = level2)
	
	columnsXML <- newXMLNode("columns", parent=query)
	colIndex=1;
	for (columnName in columns) {
		internalname <- ifelse(as.character(columnName)=='date', "date", paste(sep="_", "col", colIndex))
		if(as.character(columnName)=='date') {
			column <- newXMLNode("column", attrs = c(	'name'=internalname, 
									'columnName'=toString(columnName),
									'returnAsDate'='true',
									'dateFormat'='day'
								), parent=columnsXML)
		}
		else {
			column <- newXMLNode("column", attrs = c(	'name'=internalname, 
									'columnName'=toString(columnName)
								),parent=columnsXML)
		}
		
		colIndex=colIndex + 1
	}
	sortOrder <- newXMLNode("sortOrder", parent=query)

	return(root)
}
