# Project-Specific ----
# Packages httr, rjson, dplyr

readPaDataSmart <- function(secrets=NULL, token, id) {
    if(!is.null(secrets)) {
        secrets <- readSmartSecrets(secrets)
        token <- secrets$smart_key
        id <- secrets$pa_data_id
    }
    header <- add_headers(Authorization = paste0('Bearer ', token))
    base <- 'https://api.smartsheetgov.com/2.0/sheets'
    apiData <- GET(url=paste0(base, '/', id), config=header)
    status <- smartToDf(apiData)
    return(status)
    status <- status[status$Status != 'Done', ]
    status <- list(
        projectName = status[['Project Name']],
        deviceName = status[['Item']],
        deploymentMakara = status[['Status: Deployment data entered into Makara']],
        recoveryMakara = status[['Status: Recovery data entered into Makara']],
        tempUpload = status[['Status: Temperature data uploaded']],
        dataExtracted = status[['Status: Data files extracted on Stellwagen server']],
        TOLComplete = status[['Status: (QAQC #1) TOL completed']],
        useableMakara = status[['Status: (QAQC #2) Usable dates entered in Makara']],
        usableStartTime = status[['Usable Data Timeline - Start Time']],
        usableEndTime = status[['Usable Data Timeline - End Time']],
        usableStartDate = status[['Usable Data Timeline - Start Date']],
        usableEndDate = status[['Usable Data Timeline - End Date']]
    )
    hasTime <- !is.na(status$usableStartTime) &
        !is.na(status$usableEndTime) &
        !is.na(status$usableStartDate) &
        !is.na(status$usableEndDate)
    status$usableStart <- NA
    status$usableEnd <- NA
    status$usableStart[hasTime] <- paste0(status$usableStartDate[hasTime],
                                          'T',
                                          status$usableStartTime[hasTime],
                                          'Z')
    status$usableEnd[hasTime] <- paste0(status$usableEndDate[hasTime],
                                        'T',
                                        status$usableEndTime[hasTime],
                                        'Z')
    status$deviceId <- sapply(status$deviceName, parseDeviceId)
    for(c in c('deploymentMakara', 'recoveryMakara')) {
        # do I need to change certian text to NA? i dont think so
    }
    data.frame(status)
}

readSmartSecrets <- function(x) {
    if(is.list(x) &&
       'smart_key' %in% names(x)) {
        return(x)
    }
    text <- readLines(x)
    text <- strsplit(text, ':')
    text <- lapply(text, function(t) {
        base <- gsub("\\s|'", '', t)
        out <- list(base[2])
        names(out) <- base[1]
        out
    })
    text <- unlist(text, recursive=FALSE)
    text
}

smartToDf <- function(x) {
    data <- fromJSON(rawToChar(x$content))
    cols <- sapply(data$columns, function(x) x$title)
    rows <- lapply(data$rows, function(r) {
        one <- lapply(r$cells, function(c) {
            val <-as.character(c$value)
            # bind_rows doenst like len 0 parts
            if(length(val) == 0) {
                return(NA)
            }
            val
        })
        names(one) <- cols
        one
    })
    result <- bind_rows(rows)
    result
}

readInsTrackSmart <- function(secrets=NULL, token, id) {
    if(!is.null(secrets)) {
        secrets <- readSmartSecrets(secrets)
        token <- secrets$smart_key
        id <- secrets$ins_track_id
    }
    header <- add_headers(Authorization = paste0('Bearer ', token))
    base <- 'https://api.smartsheetgov.com/2.0/sheets'
    apiData <- GET(url=paste0(base, '/', id), config=header)
    recorder <- smartToDf(apiData)
    return(recorder)
    recorder <- recorder[c('Item ID', 'Sensitivity')]
    names(recorder) <- c('deviceId', 'sensitivity')
    recorder <- recorder[!is.na(recorder$sensitivity), ]
    recorder$deviceId <- as.character(round(as.numeric(recorder$deviceId), 0))
    recorder$sensitivity <- as.numeric(recorder$sensitivity)
    recorder$sensitivity <- recorder$sensitivity * -1
    data.frame(distinct(recorder))
}

readStDeploymentSmart <- function(secrets=NULL, token, id) {
    if(!is.null(secrets)) {
        secrets <- readSmartSecrets(secrets)
        token <- secrets$smart_key
        id <- secrets$st_deployment_id
    }
    header <- add_headers(Authorization = paste0('Bearer ', token))
    base <- 'https://api.smartsheetgov.com/2.0/sheets'
    apiData <- GET(url=paste0(base, '/', id), config=header)
    data <- smartToDf(apiData)
    # data <- data %>%
    #     filter(Status == 'Recovered')
    data[['ST600 Serial Number:']] <- gsub('\\*', '', data[['ST600 Serial Number:']])
    data
}

formatGoogleQAQC <- function(x, map) {
    if(ncol(x) <= 2) {
        return(NULL)
    }
    one <- myRenamer(x, map=map)
    one <- one[!is.na(one$deployment_code), ]
    if(!'st_serial_number' %in% names(one)) {
        one$st_serial_number <- NA
    }
    one$st_serial_number <- as.character(one$st_serial_number)
    timeCols <- c(
        'recording_start_datetime',
        'recording_end_datetime',
        'deployment_datetime',
        'recovery_datetime',
        'recording_usable_start_datetime',
        'recording_usable_end_datetime'
    )
    for(t in c(timeCols, grep('compromised_data', names(one), value=TRUE))) {
        one[[t]] <- googsTimeToChar(one[[t]], dropNaChar=TRUE)
    }
    for(i in seq_len(ncol(one))) {
        if(is.list(one[[i]])) {
            one[[i]] <- unlist(one[[i]])
        }
    }
    
    if(one$sheet_name[1] == 'Seamount' &&
       !'recording_quality_code' %in% names(one)) {
        one$recording_quality_code <- NA
    }
    one$recording_device_lost <- one$recording_device_lost == 'Y'
    # special circumstance for lost to still deploy
    lostAndNA <- one$recording_device_lost & is.na(one$pacm_db_status)
    one$pacm_db_status[lostAndNA] <- 'LOST'
    naCharCols <- c('soundfile_type_1', 'soundfile_type_2',
                    grep('compromised_data', names(one), value=TRUE))
    for(n in naCharCols) {
        one[[n]] <- gsub('n/a', '', tolower(one[[n]]))
        one[[n]][one[[n]] == ''] <- NA
    }
    one <- unite(one, 
                 col='compromised_starts', 
                 matches('compromised_data[0-9]_start'),
                 na.rm=TRUE, 
                 sep=';'
    )
    one <- unite(one, 
                 col='compromised_ends', 
                 matches('compromised_data[0-9]_end'),
                 na.rm=TRUE, 
                 sep=';'
    )
    
    one <- combineColumns(one, 
                          into='recording_filetypes', 
                          columns=c('soundfile_type_1', 'soundfile_type_2'),
                          sep=',')
    one$organization_code <- deployCodeToOrg(one$deployment_code)
    one$recording_channel <- NA
    one$recording_channel[one$recording_n_channels == 1] <- 1
    # select(one, all_of(keepCols))
    one
}

parseDeviceId <- function(x) {
    x <- tolower(x)
    if(!grepl('soundtrap', x)) {
        return(NA)
    }
    x <- gsub(' ', '', x)
    x <- strsplit(x, '-')[[1]]
    if(length(x) != 2) {
        return(NA)
    }
    x <- x[2]
    x <- gsub('\\(copy\\)', '', x)
    x
}

# turns columns new_proj and site_code into actual project_code as proj_test
addNefscProjectCode <- function(x) {
    # x$new_proj_code <- NA
    # x <- x %>% 
    #     mutate(project_code = paste0(project_code, '_', gsub('[0-9]', '', site_code))) %>% 
    #     distinct()
    
    nefscOrg <- x$organization_code == 'NEFSC'
    x$project_code[nefscOrg] <- gsub('[0-9]', '', x$deployment_code[nefscOrg])
    x$project_code[nefscOrg] <- gsub('__', '_', x$project_code[nefscOrg])
    x$site_code <- gsub('.*_([0-9A-Z-]*)$', '\\1', x$deployment_code)
    x <- mutate(x,
                project_code = case_when(
                    project_code == 'NEFSC_DE_DB' ~ 'NEFSC_DE',
                    project_code == 'NEFSC_GOM_LUBEC' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_MATINICUS' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_MDR' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_ME' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_MONHEGAN' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_PETITMANAN' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_PORTLAND' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_SEALISLAND' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_YORK' ~ 'NEFSC_GOM_INSHORE',
                    project_code == 'NEFSC_GOM_USTR' ~ 'NEFSC_GOM_OFFSHORE',
                    project_code == 'NEFSC_MA-RI_COX' ~ 'NEFSC_MA-RI_COX-LEDGE',
                    project_code == 'NEFSC_MA-RI_NS' ~ 'NEFSC_MA-RI_NANTUCKET-SHOALS',
                    project_code == 'NEFSC_MA-RI_COX-PWN' ~ 'NEFSC_MA-RI_POWERON',
                    project_code == 'NEFSC_MA-RI_PWN' ~ 'NEFSC_MA-RI_POWERON',
                    project_code == 'NEFSC_MA-RI_PWN-B' ~ 'NEFSC_MA-RI_POWERON',
                    project_code == 'NEFSC_SBNMS_SB' ~ 'NEFSC_SBNMS_ONMS-SOUND',
                    project_code == 'NEFSC_VA_CB' ~ 'NEFSC_VA',
                    project_code == 'NEFSC_VA_ES' ~ 'NEFSC_VA',
                    project_code == 'NEFSC_VA_PWNVA' ~ 'NEFSC_VA_POWERON',
                    project_code == 'NEFSC_MA-RI_MUSK' ~ 'NEFSC_MA-RI_SEAL',
                    project_code == 'NEFSC_MA-RI_MONO' ~ 'NEFSC_MA-RI_SEAL',
                    .default=NA
                )
    )
    tncOrg <- x$organization_code == 'TNC'
    x$project_code[tncOrg] <- gsub('^([A-Z]*_[A-Z]*)_.*', '\\1', x$deployment_code[tncOrg])
    otherOrgNA <- !x$organization_code %in% c('NEFSC', 'TNC') &
        is.na(x$project_code)
    projPattern <- '^([A-Z]*_[A-Z]*_[0-9]*).*'
    for(i in which(otherOrgNA)) {
        if(grepl(projPattern, x$deployment_code[i])) {
            x$project_code[i] <- gsub(projPattern, '\\1', x$deployment_code[i])
        }
    }
    pauOrg <- x$organization_code == 'PARKSAU'
    x$project_code[pauOrg] <- gsub('_[0-9]*$', '', x$project_code[pauOrg])
    x
}

googsTimeToChar <- function(x, dropNaChar=FALSE) {
    outs <- rep(NA_character_, length(x))
    for(i in seq_along(x)) {
        if(inherits(x[[i]], 'POSIXct')) {
            outs[i] <- format(x[[i]], format='%Y-%m-%d %H:%M:%S')
        }
        if(is.character(x[[i]])) {
            if(isTRUE(dropNaChar) &&
               tolower(x[[i]]) == 'n/a') {
                outs[i] <- ''
            } else {
                outs[i] <- x[[i]]
            }
            
        }
        if(is.na(x[[i]])) {
            outs[i] <- ''
        }
    }
    if(anyNA(outs)) {
        warning(sum(is.na(outs)), ' times cant be converted')
    }
    outs
}


deployCodeToOrg <- function(x) {
    x <- gsub('^([A-Z]*)_.*', '\\1', x)
    x[x == 'PARKSAUSTRALIA'] <- 'PARKSAU'
    x
}

formatRecordingIntervals <- function(x) {
    x <- x %>% 
        mutate(compromised_starts=strsplit(compromised_starts, ';'),
               compromised_ends=strsplit(compromised_ends, ';')) %>% 
        unnest(cols=c('compromised_starts', 'compromised_ends')) %>% 
        rename(recording_interval_start_datetime=compromised_starts,
               recording_interval_end_datetime=compromised_ends)
    x
}
