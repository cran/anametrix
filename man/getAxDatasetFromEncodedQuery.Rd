\name{getAxDatasetFromEncodedQuery}
\alias{getAxDatasetFromEncodedQuery}
\title{
Extracts data from Anametrix API into R data structure
}
\description{
Function getAxDatasetFromEncodedQuery extracts data from Anametrix API using Base64-encoded Anametrix query, and returns R data frame
}
\usage{
getAxDatasetFromEncodedQuery(auth, encodedQuery, startDate, endDate, verbose)
}
\arguments{
\item{auth}{
Authentication instance.
}
\item{encodedQuery}{
Base64-encoded query XML corresponding to Anametrix API requirements. Contact your product manager for assistance. 
}
\item{startDate}{
Specifies start date of the query (format: YYYY-MM-DD).
}
\item{endDate}{
Specifies end date of the query (format: YYYY-MM-DD).
}
\item{verbose}{
Specifies whether to print out notifications. Example: FALSE
}
}
\author{
Author, Roman Jugai
}
\keyword{connection}
\keyword{database}