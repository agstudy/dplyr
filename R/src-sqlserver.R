#' Connect to MS sqlserver
#' 
#' Use \code{src_sqlserver} to connect to an existing MS sqlserver data base
#' and \code{tbl} to connect to tables within that database. 
#' To connect to MS sqlserver database, you should provide either an url connection
#' Or connection parameters (host,user,dbname,password,trusted,...)
#' 
#' @template db-info
#' @param dbname Database name
#' @param host,port Host name and port number of database
#' @param user,password User name and password (if needed)
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}. For the tbl, included for 
#'   compatibility with the generic, but otherwise ignored.
#' @param src a sqlserver src created with \code{src_sqlserver}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # To connect to a database first create a src:
#' # here I am using a trsuted connection (using windows )
#' my_db <- src_sqlserver(host = "localhsot", trsuted = TRUE)
#'  
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' # Here we'll use the Lahman database: to create your own local copy,
#' # create a local database called "lahman", or tell lahman_mysql() how to 
#' # a database that you can write to
#' 
#' if (has_lahman("mysql")) {
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_mysql(), "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = 1.0 * R / AB)
#' 
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten 
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#' 
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#'
#' # MySQL doesn't support windowed functions, which means that only
#' # grouped summaries are really useful:
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(ungroup(stints), stints > 3)
#' summarise(stints, max(stints))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_mysql(), "Master"), playerID, hofID, 
#'   birthYear)
#' hof <- select(filter(tbl(lahman_mysql(), "HallOfFame"), inducted == "Y"),
#'  hofID, votedBy, category)
#' 
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # Find players not in hof
#' anti_join(player_info, hof)
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_mysql(),
#'   sql("SELECT * FROM Batting WHERE YearID = 2008"))
#' batting2008
#' }
src_sqlserver <- function(url=NULL,dbname=NULL, host = "localhost", user = "",trusted=FALSE, 
                      password = "", ...) {
  if (!require("rsqlserver")) {
    stop("rsqlserver package required to connect to Sql Server", call. = FALSE)
  }
  
  con <- dbi_connect(SqlServer(), url=url,dbname = dbname , host = host, 
                     user = user, password = password, trusted=trusted,...)
  info <- db_info(con)
  
  src_sql("sqlserver", con, 
          info = info, disco = db_disconnector(con, "sqlserver"))
}

#' @export
#' @rdname src_rsqlserver
tbl.src_sqlserver <- function(src, from, ...) {
  tbl_sql("sqlserver", src = src, from = from, ...)
}

#' @export
brief_desc.src_sqlserver <- function(x) {
  info <- x$info
  paste0("Sql server ", info$ServerVersion, " [", info$WorkstationId, "@", 
         info$DataSource, ":", info$Database, "/", 
         ifelse(info$State[[1]]=='1','open','closed'), "]")
}
#' @export
translate_env.src_sqlserver <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
                   n = function() sql("count(*)"),
                   sd =  sql_prefix("stddev_samp"),
                   var = sql_prefix("var_samp"),
                   paste = function(x, collapse) build_sql("group_concat(", x, collapse, ")")
    )
  )
}


#' @export
head.tbl_sqlserver <- function(x, n = 6L, ...) {
  assert_that(length(n) == 1, n > 0L)
  build_query(x)$fetch()
}

