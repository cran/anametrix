% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/exported_functions.R
\name{getVariableInfo}
\alias{getVariableInfo}
\title{getVariableInfo}
\usage{
getVariableInfo(table.object, vars = c("id", "tableColumnTypeId",
  "columnName", "name", "summable", "required", "showInUI", "unique",
  "presentationDefault", "aggregationTypeId", "presentationTypeId",
  "decimalPlaces", "description"))
}
\arguments{
\item{table.object}{output from \code{\link{getTableConfiguration}} function}

\item{vars}{info to download (column names of the output dataframe). Default variables are \code{c('id', 'tableColumnTypeId', 'columnName', 'name')}. 
Additional variable names are c('summable', 'required', 'showInUI', 'unique', 'presentationDefault', 'aggregationTypeId', 'presentationTypeId', 'decimalPlaces', 'description')}
}
\value{
R data frame containing one row per variable in the table.
}
\description{
List all columns for a specific table.
}
\examples{
username <- "demo.user@demoanametrix.com"
password <- "password"
if (!authenticationCheck(auth)) 
auth <- authenticate("https://demo.anametrix.com/api", username, password)
browser.table  <- getTableConfiguration(auth, 10385, "Browsers")
variable.df <- getVariableInfo(browser.table)

}
\seealso{
Other metadata functions: \code{\link{getAccountInfo}},
  \code{\link{getDatasuiteInfo}},
  \code{\link{getDetailedInfo}},
  \code{\link{getReplacementNames}},
  \code{\link{getSegmentInfo}}, \code{\link{getTableInfo}}
}

