\name{getAxDataset}
\alias{getAxDataset}
\title{
Extracts data from Anametrix API into R data structure
}
\description{
Function getAxDataset extracts data from Anametrix API and returns R data frame
}
\usage{
getAxDataset(auth, reportSuiteId, tableConfigurationXML, columns, datestart, 
dateend, count, verbose)
}
\arguments{
 \item{auth}{
Authentication instance. 
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
 \item{verbose}{
Specifies whether to print out notifications etc. Example: FALSE
}
}
\value{
\item{D}{R Data frame containing data}
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

username = "demo.user@demoanametrix.com"
password = "demo.user"
auth <- authenticate("https://demo.anametrix.com/api", username, password)
table <- getTableConfiguration(auth, 10385, "Browsers")
printTable(table)
axdata <- getAxDataset(auth, 10385, table, c("name","visits","page_views"), "2012-03-01", "2012-04-30", 
100, TRUE)
head(axdata)
}
\keyword{connection}
\keyword{database}