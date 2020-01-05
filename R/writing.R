#' @export
createDatabase <- function(database, sqlitePath){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  DBI::dbDisconnect(db)
}

## add data to existing table
#' @export
saveData <- function(dataToWrite,table,database, sqlitePath){
  ## connect to database
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("INSERT INTO %s (%s) VALUES ('%s')",
                   table,
                   paste(names(dataToWrite), collapse = ", "),
                   paste(dataToWrite, collapse = "', '")
  )
  DBI::dbExecute(db, query)
  DBI::dbDisconnect(db)
}

## bulk import new data
#' @export
appendData <- function(database, dataToWrite, table, sqlitePath, append = TRUE, overwrite = FALSE){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  DBI::dbWriteTable(db, table, dataToWrite, append = append, overwrite = overwrite)
  DBI::dbDisconnect(db)
}

#' @export
appendEmpty <- function(database, table, sqlitePath, append = TRUE, overwrite = FALSE){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("INSERT INTO %s DEFAULT VALUES", table)
  DBI::dbExecute(db, query)
  DBI::dbDisconnect(db)
}

#' @export
uploadData <- function(database, table, dataToWrite, sqlitePath){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  DBI::dbWriteTable(db, table, dataToWrite, append = TRUE)
  DBI::dbDisconnect(db)
}

## delete data from a table
#' @export
nukeData <- function(database, table, sqlitePath){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("DELETE FROM %s", table)
  DBI::dbExecute(db, query)
  DBI::dbDisconnect(db)
}

## overwrite data but minds duplicates based on date
#' @export
changeTargets <- function(database, table, avg, ul, ll, mr_ul, loc, start, end, sqlitePath){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  if(loc == 'All'){
    query1 <- sprintf("UPDATE %s SET AVG = %s, UL = %s, LL = %s, MR_UL = %s WHERE DATE BETWEEN '%s' AND '%s' AND NMS_LOC = 'NMS-CORE'", table, avg, ul, ll, mr_ul, start, end)
    query2 <- sprintf("UPDATE %s SET AVG = %s, UL = %s, LL = %s, MR_UL = %s WHERE DATE BETWEEN '%s' AND '%s' AND NMS_LOC = 'IFS'", table, avg, ul, ll, mr_ul, start, end)
    data <- DBI::dbExecute(db, query1)
    data <- DBI::dbExecute(db, query2)
  }else{
    query <- sprintf("UPDATE %s SET AVG = %s, UL = %s, LL = %s, MR_UL = %s WHERE DATE BETWEEN '%s' AND '%s' AND NMS_LOC = '%s'", table, avg, ul, ll, mr_ul, start, end, loc)
    #query <- sprintf("REPLACE INTO %s (AVG, UL, LL, MR_UL) VALUES (%s, %s, %s, %s)", table, avg, ul, ll, mr_ul)
    print(query)
    data <- DBI::dbExecute(db, query)
  }
  DBI::dbDisconnect(db)
}
