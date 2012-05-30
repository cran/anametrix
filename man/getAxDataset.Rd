\name{getAxDataset}
\alias{getAxDataset}
\title{
Extracts data from Anametrix API into R data structure
}
\description{
Function getAxDataset extracts data from Anametrix API and returns R data frame
}
\usage{
getAxDataset(axapiUri, authtoken, reportSuiteId, tableConfigurationXML, columns, datestart, 
dateend, count)
}
\arguments{
  \item{axapiUri}{
Anametrix API Endpoint URI.
}
  \item{authtoken}{
Authentication Token returned from Anametrix API to provide secure access. Use getToken(axapiUri, username, password) to get it.
}
  \item{reportSuiteId}{
Data Suite ID. Contact your product manager for assistance. 
}
  \item{tableConfigurationXML}{
Configuration of a table in XML format. Use getTableConfiguration(axapiUri, username, password, reportsuiteId, tableName, authtoken) to get it.
}
  \item{columns}{
Vector of column names for the request. Use printTable(tableConfigurationXML) to see all column names. Example: c("city","country","male_population","female_population")
}
  \item{datestart}{
Start date of the request. Use YYYY-MM-DD format. Example: "2012-03-31"
}
  \item{dateend}{
End date of the request. Use YYYY-MM-DD format. Example: "2012-03-31"
}
  \item{count}{
Number of rows requested. Example: "1000"
}
}
\value{
\item{D}{R Data frame}
}
\format{R Data frame containing number of observations requested from Anametrix API}
\references{
http://www.anametrix.com
}
\author{
Author, Roman Jugai
}

\examples{
#Example: Extracting 3 columns from Browsers table from Anametrix API
axapiUri = "https://demo.anametrix.com/api"
username = "demo.user@demoanametrix.com"
password = "demo.user"
authtoken <- getToken(axapiUri, username, password)
table <- getTableConfiguration(axapiUri, authtoken, username, password, 10385, "Browsers")
printTable(table)
axdata <- getAxDataset(axapiUri, authtoken, 10385, table, c("name","visits","page_views"), 
"2012-03-01", "2012-04-30", '500')
head(axdata)
}
\keyword{connection}
\keyword{database}