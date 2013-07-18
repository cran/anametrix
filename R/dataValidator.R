.packageName <- 'anametrix'

library(RCurl)
library(XML)
library(pastecs)

populateSummaryStatisticsTable <- function(auth, reportsuiteId, startDate, endDate, verbose) {
  fullTableListXML <- getFullTableListXML(auth, reportsuiteId);
  tableListXML <- fullTableListXML[[1]];

  for(i in 1:xmlSize(tableListXML)) {

    tableXML <- tableListXML[[i]]
    cols <- tableXML["columns"]
    
    columnNamesmap=c()
    columnvector <- character(0)
    for(j in 1:xmlSize(cols$columns)) {
      col <- cols$columns[[j]]
      columnName <- xmlAttrs(col)["columnName"]
      columnvector <- c(columnvector, columnName)
      columnNamesmap[columnName] = xmlAttrs(col)["name"]
    }
    
    tableName <- xmlAttrs(tableXML)["name"]
    tableobject <- getTableConfiguration(auth, reportsuiteId, tableName)
    axdata <- getAxDataset(auth, reportsuiteId, tableobject, columnvector, startDate, endDate, 10000, "day", "name","asc", FALSE)

    if(is.null(axdata)) {
      if(verbose) {
        cat("skipping because no data in the table:", i, "tableName:", tableName,"\n")
      }
      next
    }
    
    dimcolumns <- axdata[, !sapply(axdata, is.numeric)]
    drops <- c("tag")
    dimcolumns <- dimcolumns[, !(names(dimcolumns) %in% drops)]
    #TODO: dimension columns statistics
    numericcolumns <- axdata[, sapply(axdata, is.numeric)]
    
    if(is.null(ncol(numericcolumns))) {
      if(verbose) {
        cat("skipping because no metric columns for table:", i, "tableName:", tableName,"\n")
      }
      next
    }
    
    for (i in 1:ncol(numericcolumns)) {
      oldName <- colnames(numericcolumns)[i]
      newName <- columnNamesmap[[oldName]]
      colnames(numericcolumns)[colnames(numericcolumns)==oldName] <- newName
    }

    stdesc <- stat.desc(numericcolumns, p=0.9)
    
    Msummarytable <- data.frame(t(stdesc))
    Msummarytable <- data.frame(Msummarytable, 'name'=rownames(Msummarytable))
    
    drops <- c("nbr.val", "nbr.null", "nbr.na", "CI.mean.0.9", "var")
    Msummarytable <- Msummarytable[, !(names(Msummarytable) %in% drops)]
    Msummarytable$tablename <- tableName
    colnames(Msummarytable)[colnames(Msummarytable)=="SE.mean"] <- "se_of_mean"
    colnames(Msummarytable)[colnames(Msummarytable)=="std.dev"] <- "std_dev"
    colnames(Msummarytable)[colnames(Msummarytable)=="coef.var"] <- "coef_var"
    
    tc <- getTableConfiguration(auth, reportsuiteId, "Summary Table (Metric Columns)")
    
    uploadData(auth, reportsuiteId, tc, Msummarytable, colnames(Msummarytable), tableName, "none")
    if(verbose) {
      cat("uploading summary statistics for table", tableName, "\n");
    }
  }
}
