\name{getAxDataset}
\alias{getAxDataset}
\alias{authenticate}
\alias{getFullTableListXML}
\alias{getTableConfiguration}
\alias{constructQueryXML}
\alias{queryReader}
\alias{getDBDateFormat}
\alias{randomAlphaNumericUID}
\alias{getColumnTypeId}
\alias{isdefined}
\title{
Extracts data from Anametrix API into R data structure
}
\description{
Function getAxDataset extracts data from Anametrix API and returns R data frame
}
\usage{
getAxDataset(auth, reportSuiteId, tableObject, columns, datestart, 
dateend, count, dateFormat, sortColumn, sortDirection, verbose)
}
\arguments{
 \item{auth}{
Authentication instance. 
}
  \item{reportSuiteId}{
Data Suite ID. Contact your product manager for assistance. 
}
  \item{tableObject}{
Configuration of a table. Use getTableConfiguration(auth, reportsuiteId, tableName) to get it.
}
  \item{columns}{
Array of column names for the request. Example: c("city","country","male_population")
Use printTable(tableObject) to see all column names.
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
  \item{dateFormat}{
Format of the date column. Example: "day", "week", or custom format like "MM-dd-yy" or "w-yyyy"
}
  \item{sortColumn}{
Column to sort on. Use any column provided in the columns array
}
  \item{sortDirection}{
Direcion of the sorting. Example: "asc"
}
 \item{verbose}{
Specifies whether to print out notifications. Example: FALSE
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
axdata <- getAxDataset(auth, 10385, table, c("name","visits","page_views"), "2012-03-01", 
"2012-04-30", 100, "month", "visits", "asc", TRUE)
head(axdata)
}
\keyword{connection}
\keyword{database}