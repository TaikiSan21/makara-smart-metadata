# fpod fixo
fpodPath <- '../Data/MakBal/fpod'
fpodFiles <- file.path(fpodPath, 
                       c('SBMNS_Metadata.xlsx', 'Metadata.xlsx'))
# xlFiles <- list.files('../Data/MakBal/fpod/', pattern='xl', full.names=TRUE)
library(readxl)
library(lubridate)
library(dplyr)
readOneFpodMeta <- function(x) {
    sheets <- readxl::excel_sheets(x)
    if(any(grepl('useable|usable', tolower(sheets)))) {
        goodSheet <- sheets[grepl('useable|usable', tolower(sheets))]
        if(length(goodSheet) != 1) {
            warning('More than 1 sheet labeled usable in ', x)
        }
        return(oneFpodSheet(x, sheet=goodSheet))
    }
    bind_rows(lapply(sheets, function(s) {
        oneFpodSheet(x, sheet=s)
    }))
}   

oneFpodSheet <- function(x, sheet) {
    data <- read_excel(x, sheet=sheet, col_types = 'list')
    start <- data[['Cleaned Data Start']]
    end <- data[['Cleaned Data End']]
    if(is.numeric(data[[1]][2])) {
        nameCol <- 1
    } else {
        nameCol <- 2
    }
    deployment <- unlist(data[[nameCol]])
    keepIx <- grepl('NEFSC', deployment)
    start <- fixJankTime(start)
    end <- fixJankTime(end)
    list(deployment=deployment[keepIx],
         start_date=format(start[keepIx], format='%m/%d/%Y'),
         start_time=format(start[keepIx], format='%H:%M:%S'),
         end_date=format(end[keepIx], format='%m/%d/%Y'),
         end_time=format(end[keepIx], format='%H:%M:%S'))
}

fixJankTime <- function(time) {
    sapply(time, function(x) {
        if(inherits(x, 'POSIXct')) {
            day <- day(x)
            month <- month(x)
            day(x) <- month
            month(x) <- day
            return(x)
        } else {
            parse_date_time(
                x,
                tz='UTC', 
                orders=c('%d/%m/%Y %H:%M:%S'),
                truncated=3
            )
        }
    }) %>% 
        as.POSIXct(origin='1970-01-01', tz='UTC')
}

hm <- bind_rows(lapply(fpodFiles, readOneFpodMeta))    

write.csv(hm, file='FPOD_Dates.csv', row.names = FALSE)

