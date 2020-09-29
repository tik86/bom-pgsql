# if (!require("remotes")) {
#   install.packages("remotes", repos = "http://cran.rstudio.com/")
#   library("remotes")
# }
# 
# install_github("ropensci/bomrang", build_vignettes = TRUE)

library(DBI)
library(bomrang)
library(RPostgres)
library(readtext)
library(stringr)

con <- dbConnect(RPostgres::Postgres(),dbname = 'bom', 
                 host = '127.0.0.1',
                 port = 5432, # or any other port specified by your DBA
                 user = 'postgres',
                 password = 'xVM15IoA')

get.weather <- function(id, type){
  data <- get_historical(stationid = id, type = type)
  data$date <- ISOdate(data$year, data$month, data$day)
  data[setdiff(colnames(data),c("year","month","day","accum_days_min","quality", "accum_days_max", "product_code","period"))]
}

merge.weather <- function(stn){
  df1 <- get.weather(stn, "max")
  df2 <- get.weather(stn, "min")
  df3 <- get.weather(stn, "rain")

  df4 <- merge(df1, df2, by=c("station_number", "date"))
  df <- merge(df3, df4, by=c("station_number", "date"))
}


get.stations <- function(){
  url <-('http://www.bom.gov.au/climate/data/lists_by_element/alphaAUS_122.txt')
  bom <- read.table(url, sep = '\t',header = FALSE, skip=4, quote="")
  bom$site <- str_trim(substr(bom$V1,1,7))
  bom$location <- str_trim(substr(bom$V1,9,48))
  bom$lat <- str_trim(substr(bom$V1,50,58))
  bom$lon <- str_trim(substr(bom$V1,60,68))
  bom$from_dt <- as.Date(paste('01',str_trim(substr(bom$V1,69,77))),format='%d %b %Y')
  bom$to_dt <- as.Date(paste('01',str_trim(substr(bom$V1,78,86))),format='%d %b %Y')
  bom <- bom[setdiff(colnames(bom), c('V1'))]
  bom <- head(bom, -3)
}
  
bom <- get.stations()
dbWriteTable(con,"stations",data.frame(bom), append = FALSE, row.names = FALSE, overwrite=TRUE)

pgdf <- dbGetQuery(con, 'select * from stations')


for(i in 1:nrow(pgdf)){
print(i)
 tryCatch({
   x <- merge.weather(pgdf[i,1])
   dbWriteTable(con,"observations",data.frame(x), append = TRUE, row.names = FALSE)
   },error=function(e){
            print(e)
          })
}



