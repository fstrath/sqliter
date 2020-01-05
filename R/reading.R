#' Direct query to database
#' @param database name of the database as a string
#' @param query the query as a string
#' @return the requested data
#' @examples
#' directQuery(database = 'database_name', query = 'SELECT * FROM table')

#' @export
directQuery <- function(database, query){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  dat <- DBI::dbGetQuery(db, query)
  DBI::dbDisconnect(db)
  return(dat)
}

#' @export
directStatement <- function(database, query){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  dat <- DBI::dbExecute(db, query)
  DBI::dbDisconnect(db)
  return(dat)
}

#' @export
getMax <- function(database, table, col){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("SELECT MAX (%s) FROM %s", col, table)
  dat <- DBI::dbGetQuery(db, query)
  DBI::dbDisconnect(db)
  return(dat)
}

#' @export
getMin <- function(database, table, col){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("SELECT MIN (%s) FROM %s", col, table)
  dat <- DBI::dbGetQuery(db, query)
  DBI::dbDisconnect(db)
  return(dat)
}

#' @export
getTables <- function(database){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  list_of_tables <- DBI::dbListTables(db)
  DBI::dbDisconnect(db)
  list_of_tables
}

#' @export
getNames <- function(database, tables, match_what){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  d <- list()
  for(i in tables){
    query <- sprintf("SELECT * FROM %s LIMIT 1", i)
    print(query)
    names_to_match <- names(DBI::dbGetQuery(db, query))
    d[[i]] <- names_to_match[which(grepl(match_what, names_to_match))]
  }
  ## submit
  DBI::dbDisconnect(db)
  d
}

#' @export
getDistinct <- function(database, table, col){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("SELECT DISTINCT %s FROM %s", col, table)
  dat <- DBI::dbGetQuery(db, query)
  DBI::dbDisconnect(db)
  return(dat)
}

## load unique data from existing table columns and retrun in separate lists
#' @export
loadDataSub <- function(database, table, cols){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  d <- list()
  for(i in cols){
    query <- sprintf("SELECT DISTINCT %s FROM %s ORDER BY %s ASC", i, table, i)
    print(query)
    d[[i]] <- DBI::dbGetQuery(db, query)
  }
  ## submit
  DBI::dbDisconnect(db)
  d
}

#' @export
loadDataSubset <- function(database, table, col_return, col_filter, match){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  if(any(match == '*')){
    matchType <- "GLOB"
    match <- "'*'"
  }else{
    matchType <- 'in'
    match <- paste("(", paste("'", match, "'", collapse = ',', sep = ''), ")", sep = "")
  }
  query <- sprintf("SELECT DISTINCT %s FROM %s WHERE %s %s %s ORDER BY %s", col_return, table, col_filter, matchType, match, col_return)
  print(query)
  d <- DBI::dbGetQuery(db, query)
  ## submit
  DBI::dbDisconnect(db)
  d
}

#' @export
loadDataSubsetAll <- function(database, table, col_filter, match){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  if(any(match == '*')){
    matchType <- "GLOB"
    match <- "'*'"
  }else{
    matchType <- 'in'
    match <- paste("(", paste("'", match, "'", collapse = ',', sep = ''), ")", sep = "")
  }
  query <- sprintf("SELECT * FROM %s WHERE %s %s %s", table, col_filter, matchType, match)
  print(query)
  d <- DBI::dbGetQuery(db, query)
  ## submit
  DBI::dbDisconnect(db)
  d
}

#' @export
loadDateRange <- function(database, table, datecol, start, end, compound){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  ## pick the match type for multiples or singles
  if(any(compound == '*')){
    matchType <- "GLOB"
    compound <- "'*'"
  }else{
    matchType <- 'in'
    compound <- paste("(", paste("'", compound, "'", collapse = ',', sep = ''), ")", sep = "")
  }
  query <- sprintf("SELECT * FROM %s WHERE %s BETWEEN '%s' AND '%s' AND Compound %s %s", table, datecol, start, end, matchType, compound)
  print(query)
  ## submit
  data <- DBI::dbGetQuery(db, query)
  DBI::dbDisconnect(db)
  data
}

#' @export
loadTypes <- function(database, table, match1, match2, match3, match4){
  db <- DBI::dbConnect(RSQLite::SQLite(), gettextf('%s/%s', sqlitePath, database))
  query <- sprintf("SELECT Component_Name, Type, Subtype_1, Subtype_2, Subtype_3 FROM %s", table)
  print(query)
  ## submit
  data <- DBI::dbGetQuery(db, query)
  #data <- ''
  if(all(match1 != '*')){
    data <- data %>% filter(Type %in% match1)
  }
  if(all(match2 != '*')){
    data <- data %>% filter(Subtype_1 %in%  match2)
  }
  if(all(match3 != '*')){
    data <- data %>% filter(Subtype_2 %in% match3)
  }
  if(all(match4 != '*')){
    data <- data %>% filter(Subtype_3 %in% match4)
  }
  DBI::dbDisconnect(db)
  return(data)
}
