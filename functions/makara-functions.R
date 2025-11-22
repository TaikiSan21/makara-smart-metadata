# Generic Makara Functions ----
# Packages: dplyr, tidyr, lubridate, makaraValidatr

## Constant values ----
# List of mandatory fields for various tables
# mandatory_fields <- list(
#     'deployments' = list(
#         'always' = c('organization_code', 'deployment_code', 'deployment_platform_type_code', 
#                      'deployment_datetime', 'deployment_latitude', 'deployment_longitude'),
#         'ncei' = c('project_code','site_code', 'recovery_datetime', 'recovery_longitude', #site if stationary
#                    'recovery_latitude') 
#     ),
#     'detections' =  list(
#         'always' = c('deployment_organization_code', 'deployment_code', 'analysis_code',
#                      'analysis_organization_code',
#                      'detection_start_datetime', 'detection_end_datetime' ,
#                      'detection_effort_secs', 'detection_sound_source_code',
#                      'detection_call_type_code', 'detection_result_code'),
#         'ncei' = c()
#     ),
#     'analyses' = list(
#         'always' = c('deployment_organization_code', 'deployment_code', 'analysis_code', 'recording_codes',
#                      'analysis_organization_code',
#                      'analysis_sound_source_codes', 'analysis_granularity_code', 
#                      'analysis_sample_rate_khz', 'analysis_processing_code', 
#                      'analysis_quality_code', 'analysis_protocol_reference',
#                      'analysis_release_data', 'analysis_release_pacm', 'detector_codes'),
#         'ncei' = c('analysis_start_datetime', 'analysis_end_datetime', 'analysis_min_frequency_khz',
#                    'analysis_max_frequency_khz')
#     ),
#     'recordings' = list(
#         'always' = c('organization_code', 'deployment_code', 'recording_code', 
#                      'recording_device_codes', 'recording_start_datetime', 'recording_interval_secs',
#                      'recording_sample_rate_khz', 'recording_duration_secs',
#                      'recording_n_channels', 'recording_timezone'), # many are only if not lsot
#         'ncei' = c('recording_end_datetime', 'recording_bit_depth', 'recording_channel',
#                    'recording_quality_code', 'recording_device_depth_m', 'recording_json')
#     ),
#     'recording_intervals' = list(
#         'always' = c('organization_code', 'deployment_code', 'recording_code',
#                      'recording_interval_quality_code'),
#         'ncei' = c()
#     ),
#     'devices' = list(
#         'always' = c('organization_code', 'device_code', 'device_type_code'),
#         'ncei' = c()
#     ),
#     'projects' = list(
#         'always' = c('organization_code', 'project_code', 'project_contacts'),
#         'ncei' = c()
#     ),
#     'sites' = list(
#         'always' = c('organization_code', 'site_code'),
#         'ncei' = c()
#     ),
#     'track_positions' = list(
#         'always' = c('organization_code', 'deployment_code', 'track_code',
#                      'track_position_datetime',
#                      'track_position_latitude',
#                      'track_position_longitude'),
#         'ncei' = c()
#     ),
#     'tracks' = list(
#         'always' = c('organization_code', 'deployment_code', 'track_code'),
#         'ncei' = c()
#     )
# )

## Functions ----
# map is list of form old=new
# renames either values in a vector or names of dataframe
myRenamer <- function(x, map) {
    if(is.data.frame(x)) {
        names(x) <- myRenamer(names(x), map)
        return(x)
    }
    # check if map is obviously backwards and fix it
    if(!any(x %in% names(map)) &&
       any(x %in% unlist(map))) {
        newMap <- as.list(names(map))
        names(newMap) <- unlist(map)
        map <- newMap
    }
    for(val in names(map)) {
        if(val %in% x) {
            x[x == val] <- map[[val]]
        }
    }
    x
}

# combines a bunch of columns into one, optionally adding a prefix
# to each column entry if it is not NA
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

# pretty printing helper to print number of items in a list
# n is a cutoff of max to show at once
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

# formats a POSIXct object to 8601 format
psxTo8601 <- function(x) {
    if(is.character(x)) {
        return(x)
    }
    if(!inherits(x, 'POSIXct')) {
        warning('Must be POSIXct or character')
        return(x)
    }
    if(tz(x) != 'UTC') {
        warning('Non-UTC timezone not yet supported')
    }
    format(x, format='%Y-%m-%dT%H:%M:%SZ')
}

# create a POSIXct or full datetime character from separate date
# and time character columns
formatDatetime <- function(date, time, warn=TRUE, type=c('char', 'posix')) {
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

# Helper for tracking warnings in various checking functions
addWarning <- function(x, deployment, table, type, message) {
    if('warnings' %in% names(x)) {
        x$warnings <- addWarning(x$warnings, deployment=deployment, table=table,
                                 type=type, message=message)
        return(x)
    }
    bind_rows(x, list(deployment=deployment, table=table, type=type, message=message))
}

# Check data against templates for missing mandatory data and other issues
# x is a list with each table 
# templates comes from `formatBasicTemplates`
# mandatory is constant list 
# ncei flag is whether to check columns that are only mandatory for NCEI
# dropEmpty is flag whether to drop empty non-mandatory columns from output
checkMakTemplate <- function(x, templates, ncei=FALSE, dropEmpty=FALSE) {
    result <- templates[names(x)]
    onlyNotLost <- c('recording_start_datetime',
                     'recording_duration_secs',
                     'recording_interval_secs',
                     'recording_sample_rate_khz',
                     'recording_n_channels',
                     'recording_timezone')
    col_defs <- makaraValidatr::column_definitions
    mandatory <- lapply(col_defs, function(x) {
        list(
            'always' = x$name[x$required],
            'ncei' = x$name[x$required_ncei]
        )
    })
    warns <- vector('list', length=0)
    uniqueConditions <- lapply(col_defs, function(d) {
        vals <- d$unique_by[!is.na(d$unique_by)]
        if(length(vals) == 0) {
            return(NULL)
        }
        vals <- strsplit(vals, ',')[[1]]
        vals <- c(d$name[!is.na(d$unique_by)], vals)
        vals
    })
    for(n in names(x)) {
        thisTemp <- templates[[n]]
        thisMand <- mandatory[[n]]$always
        thisNcei <- mandatory[[n]]$ncei
        thisData <- x[[n]]
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
        # check that codes are unique if they should be
        
        uniqueCols <- uniqueConditions[[n]]
        if(!is.null(uniqueCols)) {
            checkDupeDeps <- thisData %>% 
                summarise(dupe = n() > 1, .by=all_of(uniqueCols))
            dupeDeps <- checkDupeDeps$dupe
            codePrint <- paste0('"', paste0(uniqueCols, collapse='-'), '"')
            if(any(dupeDeps)) {
                warns <- addWarning(warns, 
                                    deployment=checkDupeDeps$deployment_code[dupeDeps],
                                    table=n,
                                    type=paste0('Duplicated ', codePrint),
                                    message=paste0(codePrint, 
                                                   checkDupeDeps$deployment_code[dupeDeps],
                                                   ' has multiple entries in ', n)
                )
            }
        }
        # check that mandatory columns atually exist
        missMand <- !thisMand  %in% names(thisData)
        if(any(missMand)) {
            warns <- addWarning(warns, deployment='All', table=n, type='Missing Columns', 
                                message=paste0('Mandatory columns ', 
                                               printN(thisMand[missMand], Inf), ' are missing'))
        }
        # Fix time columns
        timeCols <- grep('datetime', names(thisData), value=TRUE)
        for(t in timeCols) {
            if(inherits(thisData[[t]], 'POSIXct')) {
                thisData[[t]] <- psxTo8601(thisData[[t]])
            }
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
            
            naVals <- is.na(thisData[[m]])
            if(m == 'recording_timezone') {
                badTz <- !grepl('^UTC[+-]?[0-9:]{0,5}$', thisData[[m]][!naVals])
                if(any(badTz)) {
                    warns <- addWarning(warns, deployment=thisData$deployment_code[!naVals][badTz],
                                        type='Invalid Timezone',
                                        table=n,
                                        message=paste0('Timezone ', thisData[[m]][!naVals][badTz], ' is invalid'))
                }
            }
            # some columns in recordings are only mandatory if not lost
            # and not UNUSABLE
            if(n == 'recordings' &&
               m %in% onlyNotLost) {
                notLost <- !sapply(thisData$recording_device_lost, isTRUE)
                naVals <- naVals & notLost
                if('recording_quality_code' %in% names(thisData)) {
                    notUnusable <- thisData$recording_quality_code != 'UNUSABLE' | is.na(thisData$recording_quality_code)
                    naVals <- naVals & notUnusable
                }
            }
            if(n == 'detections' &&
               m == 'localization_method_code') {
                isLocalized <- !is.na(thisData$localization_latitude)
                naVals <- naVals & isLocalized
            }
            
            if(n == 'detections' &&
               m == 'localization_depth_method_code') {
                isLocalizedDepth <- !is.na(thisData$localization_depth_m)
                naVals <- naVals & isLocalizedDepth
            }
            
            if(any(naVals)) {
                warns <- addWarning(warns, deployment=unique(thisData$deployment_code[naVals]),
                                    type='NA in Mandatory Field',
                                    table=n,
                                    message=paste0("Mandatory column '",
                                                   m, "' is NA"))
            }
        }
        
        # Remove columns that werent in our loaded data and are not mandatory
        if(isTRUE(dropEmpty)) {
            keepNames <- names(thisTemp) %in% unique(c(names(thisData), thisMand))
            thisTemp <- thisTemp[keepNames]
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

# Pretty printing helper for warnings created with `addWarning`
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
# convert date characters to proper 8601 style output
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

# checkValidTimezone <- function(rec) {
#     isBad <- !grepl('^UTC[+-]?[0-9]{0,4}$', rec$recording_timezone)
#     if(any(isBad)) {
#         warning(sum(isBad), ' deployments (', printN(rec$deployment_code[isBad], Inf),
#                 ') have timezones that are invalid (', printN(unique(rec$recording_timezone[isBad]), Inf), 
#                 ')')
#     }
#     rec
# }

# Check if codes being used are actually in database
# Currently checks recording_device_codes, deployment device_codes, project_codes,
# site_codes, using organization_code in the check
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

# Checks if deployments, recordings, and recording_intervals
# are already in makara 
# checkAlreadyDb <- function(x, db) {
#     # deployment and recording checko
#     dep_rec <- left_join(
#         rename(db$deployments, deployment_id=id),
#         select(db$recordings, recording_code, deployment_id),
#         by='deployment_id'
#     ) %>% 
#         mutate(JOINCHECK=TRUE)
#     depCheck <- left_join(
#         x$deployments,
#         distinct(select(dep_rec, organization_code, deployment_code, JOINCHECK)),
#         by=c('organization_code', 'deployment_code')
#     )
#     newDep <- is.na(depCheck$JOINCHECK)
#     x$deployments$new <- newDep
#     recCheck <- left_join(
#         x$recordings,
#         dep_rec,
#         by=c('organization_code', 'deployment_code', 'recording_code')
#     )
#     newRec <- is.na(recCheck$JOINCHECK)
#     x$recordings$new <- newRec
#     newDep <- sum(x$deployments$new)
#     newRec <- sum(x$recordings$new)
#     message(newDep, ' out of ', nrow(x$deployments), ' deployments are new (not yet in Makara)')
#     message(newRec , ' out of ', nrow(x$recordings), ' recordings are new (not yet in Makara)')
#     if('recording_intervals' %in% names(x)) {
#         intData <- left_join(
#             db$recording_intervals, 
#             select(db$recordings, deployment_id, id, recording_code), 
#             by=c('recording_id'='id')) %>% 
#             left_join(
#                 select(db$deployments, deployment_id=id, deployment_code),
#                 by='deployment_id'
#             ) %>% 
#             select(deployment_code, recording_code, 
#                    recording_interval_start_datetime) %>% 
#             mutate(JOINCHECK=TRUE,
#                    recording_interval_start_datetime=format(recording_interval_start_datetime, 
#                                                             format='%Y-%m-%d %H:%M:%S'))
#         intCheck <- left_join(
#             x$recording_intervals,
#             intData,
#             by=c('deployment_code', 'recording_code', 'recording_interval_start_datetime'))
#         newInt <- is.na(intCheck$JOINCHECK)
#         x$recording_intervals$new <- newInt
#         newInt <- sum(x$recording_intervals$new)
#         message(newInt, ' out of ', nrow(x$recording_intervals), 
#                 ' recording_intervals are new (not yet in Makara)')
#     }
#     x
# }
# reworked for BQ database
checkAlreadyDb <- function(x, db) {
    # deployment and recording checko
    dep_rec <- db$recordings %>% 
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
        intData <- db$recording_intervals %>% 
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

# Only works after `checkAlreadyDb` run to create "new" column
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

# checks to drop non UTF-8 chars in character columns
fixUTF8 <- function(x) {
    x |>
        mutate(
            across(where(is.character), ~ iconv(., "UTF-8", "UTF-8", sub = ""))
        )
}

# writes template formatted CSV files to a folder - last step
writeTemplateOutput <- function(data, folder='outputs') {
    if(!dir.exists(folder)) {
        dir.create(folder)
    }
    for(n in names(data)) {
        outFile <- file.path(folder, paste0(n, '.csv'))
        if(length(data[[n]]) == 0) {
            next
        }
        data[[n]] %>% 
            fixUTF8 %>% 
            write.csv(file=outFile, row.names=FALSE, na='')
    }
}

# folder containing template .csv files
# applies column types for enforcing later
formatBasicTemplates <- function() {
    # tempFiles <- list.files(folder, pattern='csv$', full.names=TRUE, recursive=TRUE)
    # result <- lapply(tempFiles, function(x) {
    #     # sometimes incomplete final line warning, ignore it
    #     table <- suppressWarnings(read.csv(x, stringsAsFactors=FALSE))
    #     table <- lapply(table, as.character)
    #     table
    # })
    col_defs <- makaraValidatr::column_definitions
    
    result <- lapply(col_defs, function(x) {
        df <- data.frame(
            matrix(NA, nrow=1, ncol=length(x$name))
        )
        names(df) <- x$name
        df <- df[FALSE, ]
        df <- lapply(df, as.character)
        df
    })
    numCols <- lapply(col_defs, function(x) {
        x$name[x$type %in% c('float', 'integer')]
    })
    boolCols <- lapply(col_defs, function(x) {
        x$name[x$type %in% c('bool')]
    })
    # names(result) <- gsub('\\.csv', '', basename(tempFiles))
    # numCols <- list(
    #     'analyses' = c('analysis_sample_rate_khz',
    #                    'analysis_min_frequency_khz',
    #                    'analysis_max_frequency_khz'),
    #     'detections' = c('detection_effort_secs',
    #                      'detection_n_validated',
    #                      'detection_n_total',
    #                      'detection_latitude',
    #                      'detection_longitude',
    #                      'detection_received_level_db',
    #                      'detection_n_animals',
    #                      'detection_n_animals_min',
    #                      'detection_n_animals_max',
    #                      'localization_latitude',
    #                      'localization_latitude_min',
    #                      'localization_latitude_max',
    #                      'localization_longitude',
    #                      'localization_longitude_min',
    #                      'localization_longitude_max',
    #                      'localization_distance_m',
    #                      'localization_distance_m_min',
    #                      'localization_distance_m_max',
    #                      'localization_bearing',
    #                      'localization_bearing_min',
    #                      'localization_bearing_max',
    #                      'localization_depth_n_signals',
    #                      'localizatoin_depth_m',
    #                      'localization_depth_m_min',
    #                      'localization_depth_m_max'),
    #     'deployments' = c('deployment_water_depth_m', 
    #                       'deployment_latitude', 
    #                       'deployment_longitude',
    #                       'recovery_latitude',
    #                       'recovery_longitude'),
    #     'recordings' = c('recording_duration_secs',
    #                      'recording_interval_secs',
    #                      'recording_sample_rate_khz',
    #                      'recording_bit_depth',
    #                      'recording_channel',
    #                      'recording_n_channels',
    #                      'recording_usable_min_frequency_khz',
    #                      'recording_usable_max_frequency_khz',
    #                      'recording_device_depth_m'),
    #     'recording_intervals' = c('recording_interval_channel',
    #                               'recording_interval_min_frequency_khz',
    #                               'recording_interval_max_frequency_khz'),
    #     'devices' = c(),
    #     'projects' = c(),
    #     'sites' = c('site_latitude', 'site_longitude'),
    #     'track_positions' = c('track_position_latitude',
    #                           'track_position_longitude',
    #                           'track_position_speed_knots',
    #                           'track_position_depth_m'),
    #     'tracks' = c()
    #     
    # )
    # boolCols <- list(
    #     'analyses' = c('analysis_release_data',
    #                    'analysis_release_pacm'),
    #     'recordings' = c('recording_redacted',
    #                      'recording_device_lost'),
    #     'deployments' = c(),
    #     'detections' = c(),
    #     'recording_intervals' = c(),
    #     'devices' = c(),
    #     'projects' = c(),
    #     'sites' = c(),
    #     'track_positions' = c(),
    #     'tracks' = c()
    # )
    for(n in names(result)) {
        for(col in numCols[[n]]) {
            result[[n]][[col]] <- as.numeric(result[[n]][[col]])
        }
        for(col in boolCols[[n]]) {
            result[[n]][[col]] <- as.logical(result[[n]][[col]])
        }
    }
    result
}
