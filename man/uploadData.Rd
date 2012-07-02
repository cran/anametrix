\name{uploadData}
\alias{uploadData}
\alias{rowToXML}
\alias{constructUploadXML}
\title{
Uploads data into Anametrix from R data structure
}
\description{
Function uploadData uploads data from R data.frame into Anametrix table
}
\usage{
uploadData(auth, reportSuiteId, tableConfigurationXML, dataframe, columns, tag, truncation)
}
\arguments{
  \item{auth}{
Authentication instance.
}
  \item{reportSuiteId}{
Data Suite ID. Contact your product manager for assistance.
}
  \item{tableConfigurationXML}{
Configuration of a table in XML format. Use getTableConfiguration(auth, reportsuiteId, tableName) to get it.
}
  \item{dataframe}{
R Data frame containing data that needs to be uploaded
}
  \item{columns}{
Array of column names that need to be uploaded. Example: c("city","country","male_population")
Use printTable(tableConfigurationXML) to see all column names.
}
  \item{tag}{
Tag of the upload.
}
  \item{truncation}{
Truncation type. If truncation type is 'none', it doesn't truncate anything before the upload. If truncation type is 'tag', all data with that tag will be truncated before the upload.
}
}

\references{
http://www.anametrix.com
}
\author{
Author, Roman Jugai
}

\keyword{connection}
\keyword{database}
