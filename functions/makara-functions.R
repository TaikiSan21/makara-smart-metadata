# Generic Makara ----
# Packages: dplyr, tidyr, lubridate
myRenamer <- function(x, map) {
    if(is.data.frame(x)) {
        names(x) <- myRenamer(names(x), map)
        return(x)
    }
    for(val in names(map)) {
        if(val %in% x) {
            x[x == val] <- map[[val]]
        }
    }
    x
}

combineColumns <- function(x, into, columns, prefix=NULL, sep='; ', warnMissing=TRUE) {
    missing <- !columns %in% names(x)
    if(all(missing)) {
        warning('None of the column(s) ',
                printN(columns),
                ' were present in data, cannot combine,')
        return(x)
    }
    if(sum(missing) > 0 && isTRUE(warnMissing)) {
        warning(sum(missing), ' column(s) to combine were not present in data (',
                printN(columns[missing]), ')')
    }
    columns <- columns[!missing]
    if(!is.null(prefix) &&
       length(prefix) != length(missing)) {
        warning('Must provide equal number of prefixes and columns')
        prefix <- NULL
    }
    if(!is.null(prefix)) {
        prefix <- prefix[!missing]
        for(i in seq_along(columns)) {
            x[[columns[i]]] <- if_else(is.na(x[[columns[i]]]) | x[[columns[i]]] == '', NA_character_, 
                                       paste0(prefix[i], x[[columns[i]]]))
        }
    }
    x <- unite(x, !!into, any_of(c(into, columns)), sep=sep, na.rm=TRUE)
    x
}

printN <- function(x, n=6, collapse=', ') {
    nItems <- length(x)
    if(nItems == 0) {
        return('')
    }
    if(nItems > n) {
        x <- c(x[1:n], paste0('... (', nItems-n, ' more not shown)'))
    }
    paste0(paste(x, collapse=collapse))
}

psxTo8601 <- function(x) {
    if(is.character(x)) {
        return(x)
    }
    format(x, format='%Y-%m-%dT%H:%M:%SZ')
}

addWarning <- function(x, deployment, table, type, message) {
    if('warnings' %in% names(x)) {
        x$warnings <- addWarning(x$warnings, deployment=deployment, table=table,
                                 type=type, message=message)
        return(x)
    }
    bind_rows(x, list(deployment=deployment, table=table, type=type, message=message))
}

checkMakTemplate <- function(x, templates, mandatory, ncei=TRUE) {
    result <- templates[names(x)]
    onlyNotLost <- c('recording_start_datetime',
                     'recording_duration_secs',
                     'recording_interval_secs',
                     'recording_sample_rate_khz')
    warns <- vector('list', length=0)
    for(n in names(x)) {
        thisTemp <- templates[[n]]
        thisMand <- mandatory[[n]]$always
        thisNcei <- mandatory[[n]]$ncei
        thisData <- x[[n]]
        # thisWarn <- vector('list', length=0)
        # checking that i didnt goof mandatory names
        missMand <- !thisMand  %in% names(thisTemp)
        if(any(missMand)) {
            warning(sum(missMand), ' misspelled mandatory names for ', n,
                    printN(thisMand[missMand]))
        }
        if(isTRUE(ncei)) {
            missNcei <- !thisNcei %in% names(thisTemp)
            if(any(missNcei)) {
                warning(sum(missNcei), 'misspelled ncei names for ', n, 
                        printN(thisNcei[missNcei]))
            }
            missNcei <- !thisNcei %in% names(thisData)
            if(any(missNcei)) {
                warns <- addWarning(warns, deployment='All', table=n, type='Missing NCEI Columns', 
                                    message=paste0('Mandatory NCEI columns ', 
                                                   printN(thisNcei[missNcei], Inf), ' are missing'))
            }
        }
        wrongNames <- !names(thisData) %in% names(thisTemp)
        if(sum(wrongNames) > 0) {
            warns <- addWarning(warns, deployment='All', table=n, type='Extra Columns',
                                message=paste0('Extra columns ', printN(names(thisData)[wrongNames], Inf),
                                               ' are present'))
            thisData <- thisData[!wrongNames]
        }
        # check that mandatory columns atually exist
        missMand <- !thisMand  %in% names(thisData)
        if(any(missMand)) {
            warns <- addWarning(warns, deployment='All', table=n, type='Missing Columns', 
                                message=paste0('Mandatory columns ', 
                                               printN(thisMand[missMand], Inf), ' are missing'))
        }
        
        # check that values in mandatory columns are not NA or ''
        for(m in thisMand[!missMand]) {
            if(is.character(thisTemp[[m]])) {
                blankChar <- !is.na(thisData[[m]]) & thisData[[m]] == ''
                if(any(blankChar)) {
                    # warns <- addWarning(warns, deployment=thisData$deployment_code[blankChar],
                    #                     type='Blank Characters in Mandatory Field',
                    #                     table=n,
                    #                     message=paste0("Character values in mandatory column '",
                    #                                    m, "' are empty, will be replaced with NA"))
                }
                thisData[[m]][blankChar] <- NA
            }
            if(m == 'recording_timezone') {
                badTz <- !grepl('^UTC[+-]?[0-9:]{0,5}$', thisData[[m]])
                if(any(badTz)) {
                    warns <- addWarning(warns, deployment=thisData$deployment_code[badTz],
                                        type='Invalid Timezone',
                                        table=n,
                                        message=paste0('Timezone ', thisData[[m]][badTz], ' is invalid'))
                }
            }
            # if(grepl('datetime', m)) {
            #     times <- makeValidTime(thisData[[m]])
            #     goodTime <- !is.na(times)
            #     thisData[[m]][goodTime] <- times[goodTime]
            #     if(any(!goodTime)) {
            #         warns <- addWarning(warns, deployment=thisData$deployment_code[!goodTime],
            #                             type='Invalid Time',
            #                             table=n,
            #                             message=paste0("Time '", thisData[[m]][!goodTime], "' in column '",
            #                             m, "'could not be converted'"))
            #     }
            # }
            
            naVals <- is.na(thisData[[m]])
            # some columns in recordings are only mandatory if not lost
            # and not UNUSABLE
            if(n == 'recordings' &&
               m %in% onlyNotLost) {
                notLost <- !sapply(thisData$recording_device_lost, isTRUE)
                notUnusable <- thisData$recording_quality_code != 'UNUSABLE' | is.na(thisData$recording_quality_code)
                naVals <- naVals & notLost & notUnusable
            }
            if(any(naVals)) {
                warns <- addWarning(warns, deployment=unique(thisData$deployment_code[naVals]),
                                    type='NA in Mandatory Field',
                                    table=n,
                                    message=paste0("Mandatory column '",
                                                   m, "' is NA"))
            }
        }
        # Fix time columns
        timeCols <- grep('datetime', names(thisData), value=TRUE)
        for(t in timeCols) {
            alreadyNa <- is.na(thisData[[t]]) | thisData[[t]] == ''
            times <- makeValidTime(thisData[[t]][!alreadyNa])
            goodTime <- !is.na(times)
            thisData[[t]][!alreadyNa][goodTime] <- times[goodTime]
            if(any(!goodTime)) {
                warns <- addWarning(warns, deployment=thisData$deployment_code[!alreadyNa][!goodTime],
                                    type='Invalid Time',
                                    table=n,
                                    message=paste0("Time '", thisData[[t]][!alreadyNa][!goodTime], "' in column '",
                                                   t, "'could not be converted'"))
            }
            
        }
        result[[n]] <- bind_rows(thisTemp, thisData)
    }
    if(!'warnings' %in% names(result)) {
        result$warnings <- warns
    } else {
        result$warnings <- bind_rows(result$warnings, warns)
    }
    result
}

checkWarnings <- function(x) {
    if(!'warnings' %in% names(x) ||
       nrow(x$warnings) == 0) {
        return(NULL)
    }
    alls <- x$warnings$deployment == 'All'
    lapply(split(x$warnings, alls), function(y) {
        nWarns <- nrow(y)
        if(nWarns == 0) {
            return(NULL)
        }
        types <- unique(y$type)
        nDeps <- length(unique(y$deployment))
        if(y$deployment[1] == 'All') {
            warning(nWarns, ' warnings of ', length(types), ' types (', 
                    printN(types, Inf), ') affecting all deployments.')
        } else {
            warning(nWarns, ' warnings of ', length(types), ' types (',
                    printN(types, Inf), ') affecting ', nDeps, ' different deployments')
        }
    })
}
makeValidTime <- function(x) {
    out <- rep(NA_character_, length(x))
    for(i in seq_along(x)) {
        val <- x[i]
        if(is.na(val) || val == '') {
            next
        }
        datetime <- parse_date_time(
            val,
            orders=c('%Y-%m-%d %H:%M:%S',
                     '%Y/%m/%d %H:%M:%S',
                     '%Y-%m-%dT%H:%M:%SZ'),
            truncated = 3,
            tz='UTC',
            quiet=TRUE,
            exact=TRUE)
        if(is.na(datetime)) {
            next
        }
        out[i] <- psxTo8601(datetime)
    }
    out
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

checkValidTimezone <- function(rec) {
    isBad <- !grepl('^UTC[+-]?[0-9]{0,4}$', rec$recording_timezone)
    if(any(isBad)) {
        warning(sum(isBad), ' deployments (', printN(rec$deployment_code[isBad], Inf),
                ') have timezones that are invalid (', printN(unique(rec$recording_timezone[isBad]), Inf), 
                ')')
    }
    rec
}

checkDbValues <- function(x, db) {
    warns <- vector('list', length=0)
    recDevCheck <- left_join(x$recordings,
                             mutate(db$devices, JOINCHECK=TRUE),
                             by=c('organization_code', 'recording_device_codes'='device_code')
    )
    recDevCheck <- is.na(recDevCheck$JOINCHECK)
    if(any(recDevCheck)) {
        warns <- addWarning(warns, deployment=x$recordings$deployment_code[recDevCheck],
                            table='recordings',
                            type="New 'device_code'",
                            message=paste0('recording_device_code ', x$recordings$recording_device_codes[recDevCheck],
                                           ' is not present in database.devices'))
        # warning(sum(recDevCheck), ' deployments (', printN(x$recordings$deployment_code[recDevCheck], Inf),
        #         ') have recording device codes that are not present in the database')
    }
    projCheck <- left_join(x$deployments,
                           mutate(db$projects, JOINCHECK=TRUE),
                           by=c('project_code', 'organization_code'))
    missProj <- is.na(projCheck$JOINCHECK)
    if(any(missProj)) {
        warns <- addWarning(warns, deployment=x$deployments$deployment_code[missProj],
                            table='deployments',
                            type="New 'project_code'",
                            message=paste0('project_code ', x$deployments$project_code[missProj], 
                                           ' is not present in database.projects'))
        # warning(sum(missProj), ' deployments (', printN(x$deployments$deployment_code[missProj], Inf),
        #         ') have project codes that are not present in the database')
    }
    siteCheck <- left_join(x$deployments,
                           mutate(db$sites, JOINCHECK=TRUE),
                           by=c('site_code', 'organization_code'))
    missSite <- is.na(siteCheck$JOINCHECK)
    if(any(missSite)) {
        warns <- addWarning(warns, deployment=x$deployments$deployment_code[missSite],
                            table='deployments', 
                            type="New 'site_code'",
                            message=paste0('site_code ', x$deployments$site_code[missSite], 
                                           ' is not present in database.sites'))
        # warning(sum(missSite), ' deployments (', printN(x$deployments$deployment_code[missSite], Inf),
        #         ') have site codes that are not present in the database')
    }
    devCheck <- select(
        x$deployments, organization_code, deployment_code, deployment_device_codes
    ) %>% 
        mutate(deployment_device_codes = strsplit(deployment_device_codes, ',')) %>% 
        unnest(deployment_device_codes) %>% 
        left_join(
            mutate(db$devices, JOINCHECK=TRUE),
            by=c('organization_code', 'deployment_device_codes' = 'device_code')
        )
    missDev <- is.na(devCheck$JOINCHECK) & !is.na(devCheck$deployment_device_codes)
    if(any(missDev)) {
        warns <- addWarning(warns, deployment=devCheck$deployment_code[missDev],
                            table='deployments',
                            type="New 'device_code'",
                            message=paste0('device_code ', devCheck$deployment_device_codes[missDev],
                                           ' is not present in database.devices')
        )
    }
    if(!'warnings' %in% names(x)) {
        x$warnings <- warns
    } else {
        x$warnings <- bind_rows(x$warnings, warns)
    }
    x
}

checkAlreadyDb <- function(x, db) {
    # deployment and recording checko
    dep_rec <- left_join(
        rename(db$deployments, deployment_id=id),
        select(db$recordings, recording_code, deployment_id),
        by='deployment_id'
    ) %>% 
        mutate(JOINCHECK=TRUE)
    depCheck <- left_join(
        x$deployments,
        distinct(select(dep_rec, organization_code, deployment_code, JOINCHECK)),
        by=c('organization_code', 'deployment_code')
    )
    newDep <- is.na(depCheck$JOINCHECK)
    x$deployments$new <- newDep
    recCheck <- left_join(
        x$recordings,
        dep_rec,
        by=c('organization_code', 'deployment_code', 'recording_code')
    )
    newRec <- is.na(recCheck$JOINCHECK)
    x$recordings$new <- newRec
    newDep <- sum(x$deployments$new)
    newRec <- sum(x$recordings$new)
    message(newDep, ' out of ', nrow(x$deployments), ' deployments are new (not yet in Makara)')
    message(newRec , ' out of ', nrow(x$recordings), ' recordings are new (not yet in Makara)')
    if('recording_intervals' %in% names(x)) {
        intData <- left_join(
            db$recording_intervals, 
            select(db$recordings, deployment_id, id, recording_code), 
            by=c('recording_id'='id')) %>% 
            left_join(
                select(db$deployments, deployment_id=id, deployment_code),
                by='deployment_id'
            ) %>% 
            select(deployment_code, recording_code, 
                   recording_interval_start_datetime) %>% 
            mutate(JOINCHECK=TRUE,
                   recording_interval_start_datetime=format(recording_interval_start_datetime, 
                                                            format='%Y-%m-%d %H:%M:%S'))
        intCheck <- left_join(
            x$recording_intervals,
            intData,
            by=c('deployment_code', 'recording_code', 'recording_interval_start_datetime'))
        newInt <- is.na(intCheck$JOINCHECK)
        x$recording_intervals$new <- newInt
        newInt <- sum(x$recording_intervals$new)
        message(newInt, ' out of ', nrow(x$recording_intervals), 
                ' recording_intervals are new (not yet in Makara)')
    }
    x
}

dropAlreadyDb <- function(x, drop=FALSE) {
    for(n in names(x)) {
        if('new' %in% names(x[[n]])) {
            if(isTRUE(drop)) {
                isNew <- x[[n]]$new
                x[[n]] <- x[[n]][isNew, ]
            }
            x[[n]]$new <- NULL
        }
    }
    x
}

# Project-Specific ----
# Packages httr, rjson

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
    data
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

formatDatetime <- function(date, time, warn=FALSE, type=c('char', 'posix')) {
    date[is.na(date)] <- ''
    time[is.na(time)] <- ''
    datetime <- paste0(date, ' ', time)
    bothMissing <- datetime == ' '
    if(any(bothMissing) && isTRUE(warn)) {
        warning(sum(bothMissing), ' dates did not have a date or time component')
    }
    datetime[bothMissing] <- NA
    datetime <- parse_date_time(
        datetime,
        orders=c('%Y-%m-%d %H:%M:%S',
                 '%Y/%m/%d %H:%M:%S',
                 '%m/%d/%Y %H:%M:%S'),
        truncated = 3,
        tz='UTC')
    noParse <- is.na(datetime) & !bothMissing
    if(any(noParse) && isTRUE(warn)) {
        warning(sum(noParse), ' dates could not be parsed')
    }
    type <- match.arg(type)
    if(type == 'char') {
        return(psxTo8601(datetime))
    }
    datetime
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
