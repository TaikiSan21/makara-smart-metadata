library(targets)
# Set target options:
tar_option_set(
    packages = c("dplyr", 'rjson', 'lubridate', 'httr', 
                 'httpuv', 'bigrquery',
                 'googledrive', 'readxl', 'tidyr', 'yaml',
                 'makaraValidatr')
)

# Run the R scripts in the R/ folder with your custom functions:
# uncomment this chunk to force janky single-file update of my common functions
# updateFunctions <- download.file('https://api.github.com/repos/TaikiSan21/makaraHelpers/contents/R/makara-functions.R',
#                      destfile = 'functions/makara-functions.R',
#                      method='libcurl',
#                      headers=c('Accept'= 'application/vnd.github.v3.raw'),
#                      extra='-O -L')
tar_source('functions/makara-functions.R')
tar_source('functions/nefsc-metadata-functions.R')

# Set TRUE to force re-loading BigQuery database
reload_database <- TRUE

### dont change below ###
if(!tar_exist_objects('db_raw')) {
    reload_database <- TRUE
}
if(isTRUE(reload_database)) {
    reload_database <- 'always'
} else if(isFALSE(reload_database)) {
    reload_database <- 'thorough'
}
####

list(
    # parameters ----
    # Values you can adjust to change how things run
    tar_target(params, {
        list(
            # possible options 'READY', 'PENDING', 'IMPORTED', 'LOST', 'NA'
            'pacm_status_to_export' = c('READY'),
            # 'pacm_status_to_export' = c('PENDING'),
            # 'pacm_status_to_export' = c('READY', 'NA'),
            # 'pacm_status_to_export' = c('READY', 'PENDING', 'IMPORTED'),
            # identify specific deployments to skip, if wanted
            'skip_deployments' = c('NEFSC_TEMP-EXP_202503_AVAST_ST7393',
                                   'NEFSC_TEMP-EXP_202503_AVAST_ST8024',
                                   'NEFSC_TEMP-EXP_202503_AVAST_ST8546'),
            # device types to export temp data for, future vemco/st only
            'temp_sensor_types' = c('FPOD', 'HOBO', 'SOUNDTRAP', 'TEMPERATURE_SENSOR', 'VEMCO'),
            # Whether or not to export data already present in DB TRUE/FALSE,
            'export_already_in_db' = TRUE,
            # Allow replacing non-NA database values with NA - almost always FALSE
            'replace_db_with_na' = FALSE,
            # keep extra columns with output - for testing
            'keep_extra_columns' = FALSE,
            'skip_temperature' = TRUE,
            'load_previous_temp' = TRUE,
            'update_device_orgs' = TRUE,
            # set this to TRUE to remove mandatory fields with NA values
            'drop_mandatory_na' = FALSE
        )
    }),
    # constants ----
    # Put any hard coded values here for transparency
    tar_target(constants, {
        list(
            'platform' = 'BOTTOM_MOUNTED_MOORING',
            'fpod_duration' = 3600,
            'fpod_interval' = 3600,
            'fpod_sr' = 1000,
            'fpod_nchannels' = 1,
            'fpod_bits' = 10,
            'fpod_channel' = 1,
            'fpod_tz' = 'UTC',
            'instrument_type' = 'SOUNDTRAP'
        )
    }),
    # manual values ----
    tar_target(manual, {
        # dplyr::tribble(
        #     ~table, ~condition, ~column, ~value
        #     'deployments', 'deployment_code == "NEFSC_MA-RI_202309_NS03"', 'deployment_latitude', 12
        # )
    }),
    # templates ----
    # tar_target(template_dir, 'templates'),
    tar_target(templates, {
        # now uses makaraValidatr
        formatBasicTemplates()
    }),
    # db tables ----
    # secrets has DB passwords, smartsheets key and IDs
    tar_target(secrets_file, '.secrets/secrets.yml'),
    tar_target(secrets, {
        if(file.exists(secrets_file)) {
            read_yaml(secrets_file)
        } else {
            makeCloudSecrets()
        }
    }),
    tar_target(db_raw, {
        downloadBqMakara()
    }, cue=tar_cue(reload_database)),
    tar_target(db, {
        formatBqMakara(db_raw)
    }),
    # google qaqc ----
    tar_target(qaqc_google_raw, {
        sheetId <- as_id('1lN1mxXJZpOgKppjYn43f5qxO00vuDHoQsy8uq-G6fDs')
        qaqc_file <- tempfile(fileext = '.xlsx')
        gargle::cred_funs_add(credentials_gce = NULL)# force email auth not GCE
        drive_download(file = sheetId, path=qaqc_file, overwrite = TRUE)
        sheetNames <- excel_sheets(qaqc_file)
        result <- lapply(sheetNames, function(x) {
            skip <- ifelse(x == 'Recording_intervals', 0, 5)
            one <- read_excel(qaqc_file, sheet=x, skip=skip, col_types = 'list')
            one$sheet_name <- x
            one <- janitor::clean_names(one)
            one
        })
        result
    }, cue=tar_cue('always')),
    tar_target(rec_int_google, {
        for(i in seq_along(qaqc_google_raw)) {
            if(qaqc_google_raw[[i]]$sheet_name[1] != 'Recording_intervals') {
                next
            }
            result <- qaqc_google_raw[[i]]
        }
        result[['recording_interval_start_datetime']] <- googsTimeToChar(result[['recording_interval_start_datetime']])
        result[['recording_interval_end_datetime']] <- googsTimeToChar(result[['recording_interval_end_datetime']])
        for(i in seq_len(ncol(result))) {
            if(is.list(result[[i]])) {
                result[[i]] <- unlist(result[[i]])
            }
        }
        result$st_serial_number <- as.character(result$st_serial_number)
        result$recording_interval_comments <- as.character(result$recording_interval_comments)
        result
    }),
    tar_target(qaqc_google, {
        googsMap <- list(
            'usable_start_datetime_utc_beginning_of_no_boat_noise' = 'recording_usable_start_datetime', #
            'usable_start_datetime_gmt_beginning_of_no_boat_noise' = 'recording_usable_start_datetime', #
            'usable_end_datetime_utc_start_of_recovery_vessel_noise' = 'recording_usable_end_datetime', #
            'usable_end_datetime_gmt_start_of_recovery_vessel_noise' = 'recording_usable_end_datetime', #
            'recov_datetime_utc_out_of_water' = 'recovery_datetime', #
            'recov_datetime_gmt_out_of_water' = 'recovery_datetime', #
            'recording_start_utc' = 'recording_start_datetime', #
            'recording_start_gmt' = 'recording_start_datetime', #
            'recording_end_utc' = 'recording_end_datetime', #
            'recording_end_gmt' = 'recording_end_datetime',
            'deploy_datetime_utc' = 'deployment_datetime', #
            'deploy_datetime_gmt' = 'deployment_datetime', #
            'sample_rate_khz' = 'recording_sample_rate_khz', #
            'sample_bits' = 'recording_bit_depth', #
            'no_of_channels' = 'recording_n_channels', #
            'recording_quality_type' = 'recording_quality_code', #
            'recording_quality' = 'recording_quality_code', #
            'recorder_lost' = 'recording_device_lost', #
            'project_name' = 'deployment_code', #
            'comments_beg_mid_end_in_raven' = 'recording_comments',
            'soundfile_timezone' = 'recording_timezone',
            'soundfile_timezone_from_log_files' = 'recording_timezone'
        )
        
        result <- bind_rows(lapply(qaqc_google_raw, function(x) {
            formatGoogleQAQC(x, map=googsMap)
        }))
        keepCols <- c(
            'organization_code',
            'deployment_code', #
            'recording_start_datetime', #
            'recording_end_datetime', #
            'recording_duration_secs', #
            'recording_interval_secs', #
            'recording_sample_rate_khz', #
            'recording_bit_depth', #
            'recording_channel',
            'recording_n_channels', #
            'recording_filetypes', #
            'recording_usable_start_datetime', #
            'recording_usable_end_datetime', #
            'recording_quality_code', #
            'recording_device_lost', #
            'recording_comments', 
            'deployment_datetime', #
            'recovery_datetime', #
            'pacm_db_status',
            'compromised_starts',
            'compromised_ends',
            'recording_timezone',
            'st_serial_number'
        )
        select(result , all_of(keepCols))
    }),
    # smart sheets ----
    tar_target(data_upload_raw, {
        readPaDataSmart(secrets)
    }, cue=tar_cue('always')),
    tar_target(data_upload, {
        # Only has instruemnt type or QAQC status we might care about
        dataUpMap <- list(
            'Project Name' = 'deployment_code',
            'Instrument Type' = 'instrument_type',
            'Item' = 'device',
            'Status' = 'qaqc_status'
        )
        upCols <- c('deployment_code', 'qaqc_status', 'instrument_type', 'device')
        myRenamer(data_upload_raw, map=dataUpMap) %>%
            select(all_of(upCols)) %>%
            filter(!is.na(deployment_code)) %>%
            mutate(instrument_type = toupper(instrument_type),
                   recording_code = paste0(instrument_type, '_RECORDING'))
    }, cue=tar_cue('always')),
    tar_target(nrs_deployment, {
        result <- readNRSDeploymentSmart(secrets)
        result
    }),
    # not currently used for anything
    tar_target(instrument_tracking_raw, {
        readInsTrackSmart(secrets)
    }, cue=tar_cue('always')),
    tar_target(instrument_tracking, {
        instrument_tracking_raw
    }),
    tar_target(st_deployment_raw, {
        result <- readStDeploymentSmart(secrets)
        result
    }, cue=tar_cue('always')),
    tar_target(st_deployment, {
        dropIx <- st_deployment_raw$Status == 'Deployed' &
            st_deployment_raw$Name == 'NEFSC_VA_202409_PWNVA01'
        dropIx <- dropIx | is.na(st_deployment_raw$Status)
        st_deployment_map <- list(
            'Name' = 'deployment_code', #
            'Project' = 'project_code', #
            # 'Site ID' = 'site_code', #
            'Bottom Depth (m)' = 'deployment_water_depth_m', #
            'ST Depth Above Bottom (m)' = 'recording_device_depth_m',
            'Vessel Name' = 'deployment_vessel', #
            'ST600 Serial Number:' = 'device_code', #
            'Deployment Date' = 'deploy_date',
            'Recovery Date' = 'recovery_date',
            'Deployment Time (UTC)' = 'deploy_time',
            'Recovery Time (UTC)' = 'recovery_time',
            'Status' = 'deployment_status',
            'Acoustic Release Model' = 'release_model',
            'Acoustic Release Serial Number' = 'release_number',
            'Temp. Logger Model' = 'temp_model',
            'Temp. Logger ID #:' = 'temp_number',
            'Sat. Tracker Model?' = 'satellite_model',
            'Sat. Tracker Serial Number' = 'satellite_number',
            'Recording URI' = 'recording_uri'
        )
        depCols <- c(
            # 'organization_code',
            # 'deployment_platform_type_code',
            'deployment_code',
            'project_code',
            # 'site_code',
            'deployment_water_depth_m',
            'deployment_vessel',
            'deployment_device_codes',
            'device_code',
            'deployment_latitude',
            'deployment_longitude',
            'deployment_comments',
            'deployment_datetime',
            'recovery_datetime',
            'deployment_status',
            'depth_comment',
            'recording_device_depth_m',
            'recording_uri'
        )
        
        deployment <- myRenamer(st_deployment_raw[!dropIx, ],
                                map=st_deployment_map)
        # case if model name not given but ID is, assume tidbit
        tempIdOnly <- is.na(deployment$temp_model) & !is.na(deployment$temp_number)
        deployment$temp_model[tempIdOnly] <- 'TIDBIT'
        # case if model name not given but ID is, assum vemco
        releaseIdOnly <- is.na(deployment$release_model) & !is.na(deployment$release_number)
        deployment$release_model[releaseIdOnly] <- 'VEMCO'
        releaseParks <- grepl('PARKSAUSTRALIA', deployment$deployment_code) & !is.na(deployment$release_model)
        deployment$release_model[releaseParks] <- 'ACOUSTIC_RELEASE'
        needsSO <- grepl('SOLARONE', deployment$satellite_model) & !grepl('^SO', deployment$satellite_number)
        deployment$satellite_number[needsSO] <- paste0('SO-', deployment$satellite_number[needsSO])
        deployment <- mutate(
            deployment,
            temp_model = toupper(temp_model),
            release_model = toupper(release_model),
            satellite_model = toupper(satellite_model),
            release_model = gsub('VR2AR', 'VEMCO', release_model),
            satellite_model = gsub('APOLLO X1', 'SATELLITE_TRACKER', satellite_model),
            satellite_model = gsub('SOLARONE', 'SATELLITE_TRACKER', satellite_model),
            release_number = gsub('\\*\\*', '', release_number))
        deployment <- unite(deployment, 'temp_code', c('temp_model', 'temp_number'),
                            sep='-', na.rm=TRUE)
        deployment$temp_code[deployment$temp_code == ''] <- NA
        deployment <- unite(deployment, 'release_code', c('release_model', 'release_number'),
                            sep='-', na.rm=TRUE)
        deployment$release_code[deployment$release_code == ''] <- NA
        deployment <- unite(deployment, 'satellite_code', c('satellite_model', 'satellite_number'),
                            sep='-', na.rm=TRUE)
        deployment$satellite_code[deployment$satellite_code == ''] <- NA
        deployment <- unite(deployment, 'deployment_device_codes', 
                            c('temp_code', 'release_code', 'satellite_code'),
                            na.rm=TRUE, sep=',')
        deployment$deployment_water_depth_m <- as.numeric(deployment$deployment_water_depth_m)
        deployment$recording_device_depth_m <- as.numeric(deployment$recording_device_depth_m)
        deployment$depth_comment <- NA
        noRecDepth <- is.na(deployment$recording_device_depth_m)
        deployment$recording_device_depth_m[noRecDepth] <- 0
        deployment$depth_comment[noRecDepth] <- 'recording_device_depth is bottom depth'
        deployment$recording_device_depth_m <- deployment$deployment_water_depth_m - deployment$recording_device_depth_m
        deployment <- combineColumns(
            deployment,
            into='deployment_comments',
            columns=c('Vessel Port', 'Captain Name', 'General Comments'),
            prefix=c('Vessel port ', 'Captain ', ''),
            sep=';'
        )
        # sometimes (DD) and sometimes(DDM) have the actual decimal
        deployment[['Latitude (DDM)']] <- suppressWarnings(as.numeric(deployment[['Latitude (DDM)']]))
        deployment[['Latitude (DD)']] <- suppressWarnings(as.numeric(deployment[['Latitude (DD)']]))
        deployment[['Longitude (DDM)']] <- suppressWarnings(as.numeric(deployment[['Longitude (DDM)']]))
        deployment[['Longitude (DD)']] <- suppressWarnings(as.numeric(deployment[['Longitude (DD)']]))
        deployment$deployment_latitude <- coalesce(deployment[['Latitude (DDM)']], deployment[['Latitude (DD)']])
        deployment$deployment_longitude <- coalesce(deployment[['Longitude (DDM)']], deployment[['Longitude (DD)']])
        deployment$deployment_datetime <- formatDatetime(
            deployment$deploy_date,
            deployment$deploy_time,
            warn=FALSE)
        deployment$recovery_datetime <- formatDatetime(
            deployment$recovery_date,
            deployment$recovery_time,
            warn=FALSE)
        
        deployment <- select(deployment, all_of(depCols))
        recCols <- c(
            'deployment_code',
            'recording_device',
            'fpod_device'
        )
        fpod_map <- list(
            'Name' = 'deployment_code', #
            'ST600 Serial Number:' = 'recording_device',
            'FPOD Serial Number' = 'fpod_device',
            'Bottom Depth (m)' = 'deployment_water_depth_m', #
            'FPOD Depth Above Bottom (m)' = 'recording_device_depth_m'
        )
        # ONLY GET FPOD HERE
        fpod <- myRenamer(st_deployment_raw[!dropIx, ],
                          map=fpod_map)
        fpod$recording_device_depth_m <- as.numeric(fpod$recording_device_depth_m)
        fpod$deployment_water_depth_m <- as.numeric(fpod$deployment_water_depth_m)
        fpod$depth_comment <- NA
        noFpodDepth <- is.na(fpod$recording_device_depth_m)
        fpod$recording_device_depth_m[noFpodDepth] <- 0
        fpod$depth_comment[noFpodDepth] <- 'recording_device_depth is bottom depth'
        fpod$recording_device_depth_m <- fpod$deployment_water_depth_m - fpod$recording_device_depth_m
        fpod$fpod_device <- gsub('N/?A', '', fpod$fpod_device)
        fpod$fpod_device[fpod$fpod_device == ''] <- NA
        fpod <- fpod[!is.na(fpod$fpod_device), ]
        fpod$fpod_device <- paste0('FPOD-', fpod$fpod_device)
        result <- list(deployments=deployment, fpod=fpod)
        result
    }),
    # Temp devices ----
    tar_target(temp_devices, {
        # dropIx <- which(st_deployment_raw$Status == 'Deployed' &
        #                     st_deployment_raw$Name == 'NEFSC_VA_202409_PWNVA01')
        # dropIx <- c(dropIx, which(is.na(st_deployment_raw$Status)))
        dropIx <- st_deployment_raw$Status == 'Deployed' &
            st_deployment_raw$Name == 'NEFSC_VA_202409_PWNVA01'
        dropIx <- dropIx | is.na(st_deployment_raw$Status)
        st_deployment_map <- list(
            'Name' = 'deployment_code', #
            # 'Site ID' = 'site_code', #
            # 'ST600 Serial Number:' = 'device_code', #
            'ST600 Serial Number:' = 'recording_device',
            'FPOD Serial Number' = 'fpod_device',
            'Acoustic Release Model' = 'release_model',
            'Acoustic Release Serial Number' = 'release_number',
            'Temp. Logger Model' = 'temp_model',
            'Temp. Logger ID #:' = 'temp_number',
            'Sat. Tracker Model?' = 'satellite_model',
            'Sat. Tracker Serial Number' = 'satellite_number'
        )
        
        deployment <- myRenamer(st_deployment_raw[!dropIx, ],
                                map=st_deployment_map)
        # case if model name not given but ID is, assume tidbit
        tempIdOnly <- is.na(deployment$temp_model) & !is.na(deployment$temp_number)
        deployment$temp_model[tempIdOnly] <- 'TIDBIT'
        # case if model name not given but ID is, assum vemco
        releaseIdOnly <- is.na(deployment$release_model) & !is.na(deployment$release_number)
        deployment$release_model[releaseIdOnly] <- 'VEMCO'
        deployment <- mutate(
            deployment,
            temp_model = toupper(temp_model),
            release_model = toupper(release_model),
            satellite_model = toupper(satellite_model),
            release_model = gsub('VR2AR', 'VEMCO', release_model),
            satellite_model = gsub('APOLLO X1', 'SATELLITE_TRACKER', satellite_model),
            release_number = gsub('\\*\\*', '', release_number))
        deployment$temp_model[is.na(deployment$temp_number)] <- NA
        deployment <- unite(deployment, 'temp_code', c('temp_model', 'temp_number'),
                            sep='-', na.rm=TRUE)
        deployment$temp_code[deployment$temp_code == ''] <- NA
        deployment$release_model[is.na(deployment$release_number)] <- NA
        deployment <- unite(deployment, 'release_code', c('release_model', 'release_number'),
                            sep='-', na.rm=TRUE)
        deployment$release_code[deployment$release_code == ''] <- NA
        deployment$satellite_model[is.na(deployment$satellite_number)] <- NA
        deployment <- unite(deployment, 'satellite_code', c('satellite_model', 'satellite_number'),
                            sep='-', na.rm=TRUE)
        deployment$satellite_code[deployment$satellite_code == ''] <- NA
        
        deployment$recording_device <- paste0('SOUNDTRAP-', deployment$recording_device)
        
        
        deployment$fpod_device <- gsub('N/?A', '', deployment$fpod_device)
        deployment$fpod_device[deployment$fpod_device == ''] <- NA
        deployment$fpod_device[!is.na(deployment$fpod_device)] <- paste0('FPOD-', deployment$fpod_device[!is.na(deployment$fpod_device)])
        deployment <- unite(deployment, deployment_device_codes,
                            c('recording_device', 'temp_code', 'release_code', 'satellite_code', 'fpod_device'),
                            na.rm=TRUE, sep=',')
        deployment <- deployment[c('deployment_code', 'deployment_device_codes')]
        
        temp_devices <- deployment %>% 
            mutate(device_code = strsplit(deployment_device_codes, ',')) %>% 
            unnest(device_code) %>% 
            distinct() %>%
            select(deployment_code, device_code) %>% 
            mutate(type=gsub('^([A-Z_]*)-.*', '\\1', device_code))
        db_devices <- db$deployments[c('deployment_code', 'deployment_device_codes')] %>% 
            mutate(device_code=strsplit(deployment_device_codes,',')) %>% 
            unnest(device_code) %>% 
            select(-deployment_device_codes) %>% 
            mutate(type=gsub('^([A-Z_]*)-.*', '\\1', device_code))
        temp_devices <- doJoinCheck(temp_devices,
                                    db_devices,
                                    by=c('deployment_code', 'device_code'),
                                    verbose=FALSE)
        db_devices$source <- 'makara'
        temp_devices$source <- 'smartsheets'
        dropDeps <- paste0('NEFSC_GOM_', c('202105', '202012', '202103'), '_PETITMANAN')
        temp_devices <- filter(temp_devices, !deployment_code %in% dropDeps)
        all_devices <- bind_rows(db_devices,
                                 select(filter(temp_devices, new), -new)
        )
        all_devices$type[all_devices$type == 'TIDBIT'] <- 'HOBO'
        all_devices$type[grepl('CTD', all_devices$device_code)] <- 'CTD'
        all_devices
    }),
    # Temp files ----
    tar_target(temp_directory, 'Z:/ANCILLARY_DATA/TEMPERATURE_DATA/'),
    tar_target(temp_files, {
        if(isTRUE(params$skip_temperature)) {
            return(NULL)
        }
        if(!dir.exists(temp_directory)) {
            warning('Temperature directory ', temp_directory, ' does not exist')
            return(NULL)
        }
        files <- list.files(temp_directory, recursive=TRUE, full.names=TRUE, pattern='csv$')
        files
    }, cue=tar_cue('always')),
    tar_target(temp_df, {
        if(is.null(temp_files)) {
            return(NULL)
        }
        deps <- basename(dirname(temp_files))
        ix <- grep('temperature', deps, ignore.case=T)
        deps[ix] <- basename(dirname(dirname(temp_files[ix])))
        filedf <- data.frame(deployment_code = deps,
                             full=temp_files,
                             file = basename(temp_files)) %>% 
            mutate(
                filtered=grepl('Filtered', file),
                deployment_code = gsub('MOUNTDESERTROCK', 'MDR', deployment_code),
                type = case_when(
                    grepl('_ST_', file) ~ 'SOUNDTRAP',
                    grepl('_FPOD_', file) ~ 'FPOD',
                    grepl('_VEMCO_', file) ~ 'VEMCO',
                    grepl('^VR2AR', file) ~ 'VEMCO',
                    grepl('_HOBO_', file) ~ 'HOBO',
                    grepl('_CTD_', file) ~ 'CTD',
                    .default=NA
                ))
        # try to find deployments that just need filtering
        # these deps are known problems so ignore
        knownDeps <- c('NEFSC_GOM_202112_SEALISLAND', 
                       'NEFSC_SBNMS_202108_OLE01', 
                       'NEFSC_SBNMS_202209_OLE01')
        needsFilt <- filedf %>% 
            filter(!is.na(type)) %>% 
            group_by(deployment_code, type) %>% 
            summarise(nFilt = sum(filtered),
                      files = paste0(file, collapse=','),
                      fulls = paste0(full, collapse=',')) %>% 
            filter(nFilt == 0,
                   !deployment_code %in% knownDeps) # dropping old mari_site01 folders
        if(nrow(needsFilt) > 0) {
            warning(nrow(needsFilt), ' deployments (', printN(needsFilt$deployment_code), 
                    ') need to have filtering done on temperature files')
        }
        filedf
    }),
    # Sensor dataset ----
    tar_target(sensor_datasets, {
        if(is.null(temp_df)) {
            return(NULL)
        }
        result <- temp_devices
        depsToUse <- st_deployment$deployments$deployment_code[
            st_deployment$deployments$deployment_status != 'Deployed'
        ]
        if(isTRUE(params$load_previous_temp)) {
            depsToUse <- unique(c(depsToUse,
                                  db$deployments_deployment_code
            ))
        }
        result <- filter(result, 
                         deployment_code %in% depsToUse,
                         type %in% params$temp_sensor_types)
        result$full <- NA
        
        filt_temp <- filter(temp_df, filtered)
        result <- bind_rows(lapply(split(result, list(result$deployment_code, result$type)), function(x) {
            if(nrow(x) == 0) {
                return(NULL)
            }
            thisDep <- x$deployment_code[1]
            thisType <- x$type[1]
            thisMatch <- filter(filt_temp, 
                                deployment_code == thisDep,
                                type == thisType)
            
            if(thisDep == 'NEFSC_SBNMS_202207_SB04' &&
               thisType == 'SOUNDTRAP') {
                x$full <- grep('671666216', thisMatch$full, value=TRUE)
                return(x)
            }
            if(thisType == 'TEMPERATURE_SENSOR' &&
               nrow(thisMatch) == 0) {
                checkGeneric <- filter(filt_temp,
                                       deployment_code == thisDep,
                                       type == 'HOBO')
                if(nrow(checkGeneric) == 1) {
                    x$warning <- 'TIDBIT listed in DB as TEMPERATURE_SENSOR-GENERIC'
                    x$full <- checkGeneric$full
                    return(x)
                }
            }
            if(nrow(x) != nrow(thisMatch)) {
                # msg <- paste0('Deployment ', thisDep, ' type ', thisType, 
                # ' has ', nrow(x), ' files and ', nrow(thisMatch), ' devices\n', sep='')
                if(nrow(thisMatch) != 0) {
                    msg <- paste0(nrow(thisMatch), ' files and ', nrow(x), ' devices:',
                                  paste0(x$device_code, collapse=','),
                                  '(',paste0(x$source, collapse=','),')')
                } else {
                    msg <- paste0(nrow(thisMatch), ' files and ', nrow(x), ' devices')
                }
                x$warning <- msg
            } else {
                x$full <- thisMatch$full
                if(nrow(x) != 1) {
                    x$warning <- 'Multiple devices not actually matched yet'
                    # x$device_code <- paste0(x$device_code, '(', thisMatch$source, ')')
                    ids <- gsub('SOUNDTRAP-', '', x$device_code)
                    ids <- gsub('^HF_4_', '', ids)
                    ids <- gsub('^STD5_', '', ids)
                    ids <- gsub('_2018$', '', ids)
                    for(i in seq_along(ids)) {
                        matchIx <- which(grepl(ids[i], thisMatch$file))
                        if(length(matchIx) == 1) {
                            x$full[i] <- thisMatch$full[matchIx]
                            x$warning[i] <- NA
                        }
                    }
                    
                }
                x
            }
            x
        }))
        
        noDevice <- is.na(result$device_code)
        
        result <- select(result,
                         'deployment_code',
                         'sensor_dataset_device_code' = 'device_code',
                         'type',
                         'warning',
                         'filename' = 'full') %>% 
            mutate(sensor_dataset_comments = NA,
                   sensor_dataset_variable_code = 'TEMP_C',
                   organization_code = deployCodeToOrg(deployment_code),
                   sensor_dataset_code = paste0('TEMP_DATA_', type))
        result <- bind_rows(lapply(split(result, list(result$deployment_code, result$sensor_dataset_code)), function(x) {
            if(nrow(x) <= 1) {
                return(x)
            }
            x$sensor_dataset_code <- paste0(x$sensor_dataset_code, seq_len(nrow(x)))
            x
        }))
        ## ST calibration ----
        result$sensor_dataset_comments[result$type == 'SOUNDTRAP'] <- 
            'Calibration applied: Tc = Tm - (-0.060*Tm - 1.26), where Tm is measured temperature'
        result$sensor_dataset_comments[result$type == 'FPOD'] <- 
            'Data have been aggregated to hourly mean values from the original 1-minute resolution. Full resolution data can be found at the sensor_dataset_uri'
        result$value <- lapply(result$filename, function(x) {
            if(is.na(x)) {
                return(NULL)
            }
            if(!file.exists(x)) {
                warning('File ', sensor_datasets$filename[i], ' does not exist')
                return(NULL)
            }
            read.csv(x, stringsAsFactors = FALSE)
        })
        result$sensor_dataset_uri <- gsub('Z:', 'gs://nefsc-1-pab', result$filename)
        
        result
    }),
    # Sensor Values ----
    # get from $values i attached to sd, unnest from there
    #so easy to grab meta-cols
    tar_target(sensor_values, {
        if(is.null(sensor_datasets)) {
            return(NULL)
        }
        result <- sensor_datasets
        # do format
        temp <- vector('list', length=nrow(result))
        for(i in seq_along(temp)) {
            val <- formatSensorValues(
                result$value[[i]], 
                type=tolower(result$type[i]), 
                name=basename(result$filename[i])
            )
            if(is.null(val)) {
                next
            }
            temp[[i]] <- val
        }
        result$value <- temp
        result <- result %>% 
            unnest(value) %>% 
            select(
                organization_code,
                deployment_code,
                sensor_dataset_code,
                sensor_value_datetime,
                sensor_value_value)
        result
    }),
    # FPOD ----
    tar_target(fpod_times, {
        # fpodFile <- 'FPOD_Dates.csv'
        # data <- read.csv(fpodFile, stringsAsFactors = FALSE)
        # data$fpod_start <- formatDatetime(date=data$start_date,
        #                                   time=data$start_time,
        #                                   warn=FALSE)
        # data$fpod_end <- formatDatetime(date=data$end_date,
        #                                 time=data$end_time,
        #                                 warn=FALSE)
        # data <- rename(data, deployment_code=deployment)
        # data$deployment_code <- gsub(' ', '', data$deployment_code)
        data <- readFpodSmart(secrets)
        data
    }, cue=tar_cue('always')),
    # combine sources ----
    tar_target(combined_data, {
        # removing one NA deployment
        dep <- st_deployment$deployments %>% 
            filter(!is.na(deployment_status)) %>% 
            rename(st_deploy_time = deployment_datetime,
                   st_recovery_time = recovery_datetime)
        result <- qaqc_google
        # manual dropping of some special PARKSAUS ones
        dropParks <- c('PARKSAUSTRALIA_JURIEN_202201_JNE',
                       'PARKSAUSTRALIA_JURIEN_202201_JSE')
        dropIx <- result$deployment_code %in% dropParks &
            is.na(result$pacm_db_status)
        result <- result[!dropIx, ]
        result$instrument_type <- constants$instrument_type

        result$recording_code <- paste0(result$instrument_type, '_RECORDING')
        ###
        multiDep <- names(which(table(result$deployment_code) > 1))
        for(d in multiDep) {
            ix <- which(result$deployment_code == d)
            result$recording_code[ix] <- paste0(result$recording_code[ix], '_', seq_along(ix))
        }
        ###
        result$pacm_db_status[is.na(result$pacm_db_status)] <- 'NA'
        # result$pacm_db_status[
        #     result$deployment_code == 'NEFSC_GOM_202412_USTR12' &
        #         result$pacm_db_status == 'NA'] <- 'READY'
        
        # result <- result %>%
        #     filter(pacm_db_status %in% params$pacm_status_to_export)
        
        result$deployment_platform_type_code <- constants$platform
        if(length(params$skip_deployments) > 0) {
            dropIx <- result$deployment_code %in% params$skip_deployments
            if(any(dropIx)) {
                warning('Removed data from ', sum(dropIx), ' deployments',
                        " in 'params$skip_deployments'")
            }
            result <- result[!dropIx, ]
        }
        # result <- left_join(
        #     result,
        #     distinct(select(data_upload, deployment_code, instrument_type)),
        #     by='deployment_code',
        #     relationship='many-to-one')
        
        # Googs do not have device ID unless in comment, here we fix
        # for cases where multiple recordings
        multiRecorderDep <- names(which(table(qaqc_google$deployment_code) > 1))
        
        # multiRecorderDep <- multiRecorderDep[!multiRecorderDep %in% dropParks]
        result$multiRecorder <- result$deployment_code %in% multiRecorderDep
        notMultiDep <- character(0)
        result <- bind_rows(lapply(split(result, result$multiRecorder), function(x) {
            if(isTRUE(x$multiRecorder[1])) {
                x$device_code <- NA
                for(d in multiRecorderDep) {
                    depIds <- dep$device_code[dep$deployment_code == d]
                    thisG <- which(x$deployment_code == d)
                    if(length(thisG) == 1) {
                        notMultiDep <<- c(notMultiDep, d)
                    }
                    # x$recording_code[thisG] <- paste0(x$recording_code[thisG], '_', seq_along(thisG))
                    if(all(!is.na(x$st_serial_number[thisG]))) {
                        x$device_code[thisG] <- x$st_serial_number[thisG]
                        next
                    }
                    for(i in thisG) {
                        for(dev in depIds) {
                            if(grepl(dev, x$recording_comments[i])) {
                                x$device_code[i] <- dev
                            }
                        }
                    }
                }
                noDevice <- is.na(x$device_code)
                if(any(noDevice)) {
                    warning(sum(noDevice), ' deployments (', printN(x$deployment_code[noDevice], Inf),
                            ') have no matching device')
                }
                x <- x %>% 
                    left_join(
                        dep,
                        by=c('deployment_code', 'device_code'),
                        relationship='one-to-one')
            } else {
                x <- left_join(
                    x,
                    dep,
                    by='deployment_code',
                    relationship='one-to-one')
            }
            x$recording_device_codes <- paste0(x$instrument_type, '-', x$device_code)
            x$recording_device_codes[is.na(x$device_code)] <- NA
            # if google had no deploy/recovery time, then use smartsheet time
            noDep <- x$deployment_datetime == '' | is.na(x$deployment_datetime)
            noRec <- x$recovery_datetime == '' | is.na(x$recovery_datetime)
            x$deployment_datetime[noDep] <- x$st_deploy_time[noDep]
            x$recovery_datetime[noRec] <- x$st_recovery_time[noRec]
            x
        }))
        
        if(length(notMultiDep) > 0) {
            warning('Expected multiple recorders but only found 1 entry for ',
                    length(notMultiDep), ' deployment(s) (',
                    printN(notMultiDep), ')')
        }
        # print(table(result$organization_code))
        result <- addNefscProjectCode(result)
        result <- unite(result, 'recording_comments', c('recording_comments', 'depth_comment'), sep=';', na.rm=TRUE)
        dep_out <- result %>% 
            filter(pacm_db_status %in% params$pacm_status_to_export) %>% 
            select(any_of(c(names(templates$deployments), 'pacm_db_status', 'deployment_status')))
        # rec_out <- select(result, any_of(names(templates$recordings)))
        rec_out <- result %>% 
            filter(pacm_db_status %in% params$pacm_status_to_export) %>% 
            filter(deployment_status %in% c('Recovered', 'No acoustic data (recorder lost or damaged)', NA)) %>% 
            select(any_of(names(templates$recordings)))
        # multiRecorderDep <- names(which(table(rec_out$deployment_code) > 1))
        
        # deployments with multiple recorders get 
        multiDrop <- numeric(0)
        for(d in multiRecorderDep) {
            thisIx <- which(dep_out$deployment_code == d)
            if(length(thisIx) <= 1) {
                next
            }
            sumNa <- apply(sapply(dep_out[thisIx, ], is.na), 1, sum)
            # either take the row with least NA vals, or if tied the first
            inNa <- thisIx[which(sumNa == min(sumNa))[1]]
            multiDrop <- c(multiDrop, thisIx[thisIx != inNa])
        }
        if(length(multiDrop) > 0) {
            dep_out <- dep_out[-multiDrop, ]
        }
        fpodCommonCols <- c('organization_code', 
                            'deployment_code', 
                            # 'recording_start_datetime', 
                            # 'recording_end_datetime',
                            'recording_timezone')
        fpod_out <- left_join(
            select(rec_out, all_of(fpodCommonCols)),
            select(st_deployment$fpod, 'deployment_code', 'fpod_device', 'recording_device_depth_m', 
                   'recording_comments'='depth_comment'),
            by='deployment_code'
        ) %>% 
            filter(!is.na(fpod_device)) %>% 
            left_join(
                select(filter(fpod_times, !is.na(deployment_code)),
                       deployment_code, 
                       recording_start_datetime=fpod_start,
                       recording_usable_start_datetime=fpod_start,
                       recording_end_datetime=fpod_end,
                       recording_usable_end_datetime=fpod_end),
                by='deployment_code')
        # FPOD is mostly constants
        fpod_out$recording_device_codes <- fpod_out$fpod_device
        fpod_out$recording_duration_secs <- constants$fpod_duration
        fpod_out$recording_interval_secs <- constants$fpod_interval
        fpod_out$recording_sample_rate_khz <- constants$fpod_sr
        fpod_out$recording_n_channels <- constants$fpod_nchannels
        fpod_out$recording_timezone <- constants$fpod_tz
        fpod_out$recording_code <- 'FPOD_RECORDING'
        fpod_out$recording_bit_depth <- constants$fpod_bits
        fpod_out$recording_filetypes <- NA_character_
        fpod_out$recording_channel <- constants$fpod_channel
        fpod_out$fpod_device <- NULL
        fpod_out <- fpod_out %>% 
            mutate(
                recording_quality_code = case_when(
                    deployment_code == 'NEFSC_GOM_202407_MDR' ~ 'UNUSABLE',
                    deployment_code == 'NEFSC_GOM_202407_USTR11' ~ 'UNUSABLE',
                    deployment_code == 'NEFSC_GOM_202412_USTR11' ~ 'UNUSABLE',
                    .default = 'GOOD'),
                recording_comments = case_when(
                    deployment_code == 'NEFSC_GOM_202407_MDR' ~ 'No data - battery failure',
                    deployment_code == 'NEFSC_GOM_202407_USTR11' ~ 'No data - software malfunction',
                    deployment_code == 'NEFSC_GOM_202412_USTR11' ~ 'SD card was bent and data seeemd to be unrecoverable',
                    .default = recording_comments),
                recording_device_lost = FALSE
            )
        dropFpod <- is.na(fpod_out$recording_start_datetime) & 
            (fpod_out$deployment_code %in% c('NEFSC_SBNMS_202405_SB03', 'NEFSC_SBNMS_202409_SB03'))
        fpod_out <- fpod_out[!dropFpod, ]
        fpod_out <- distinct(fpod_out)
        # print(str(rec_out))
        rec_out <- bind_rows(rec_out, fpod_out)
        rec_int_out <- select(result, 
                              any_of(c(names(templates$recording_intervals), 
                                       'compromised_starts',
                                       'compromised_ends')))
        rec_int_out <- formatRecordingIntervals(rec_int_out)
        # switch to googs rec int
        rec_int_out <- rec_int_google
        rec_int_out$multiRecorder <- rec_int_out$deployment_code %in% multiRecorderDep
        rec_int_out <-  bind_rows(lapply(split(rec_int_out, rec_int_out$multiRecorder), function(x) {
            if(isTRUE(x$multiRecorder)) {
                left_join(x,
                          select(result, deployment_code, st_serial_number, recording_code, organization_code),
                          by=c('deployment_code', 'st_serial_number'),
                          relationship='many-to-one'
                )
            } else {
                left_join(x,
                          select(result, deployment_code, recording_code, organization_code),
                          by=c('deployment_code'),
                          relationship='many-to-one'
                )
            }
        }))
        ## manual fixes ----
        dep_out <- mutate(dep_out,
                          deployment_latitude = case_when(
                              deployment_code == 'NEFSC_MA-RI_202309_NS03' ~ 40.8622,
                              deployment_code == 'NEFSC_MA-RI_202311_MUSK01' ~ 41.352254,
                              deployment_code == 'NEFSC_MA-RI_202411_MUSK01' ~ 41.352254,
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' ~ -31.71245,
                              deployment_code == 'PARKSAUSTRALIA_CEMP_202404_CES' ~ -30.869070,
                              .default = deployment_latitude
                          ),
                          deployment_longitude = case_when(
                              deployment_code == 'NEFSC_MA-RI_202309_NS03' ~ -70.2051,
                              deployment_code == 'NEFSC_MA-RI_202311_MUSK01' ~ -70.324329,
                              deployment_code == 'NEFSC_MA-RI_202411_MUSK01' ~ -70.324329,
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' ~ 115.61445,
                              deployment_code == 'PARKSAUSTRALIA_CEMP_202404_CES' ~ 156.299920,
                              .default = deployment_longitude
                          )
        )
        rec_out <- mutate(rec_out,
                          recording_device_lost = case_when(
                              deployment_code == 'NEFSC_GOM_202301_USTR04' ~ FALSE,
                              deployment_code == 'NEFSC_MA-RI_202311_MUSK01' ~ FALSE,
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' ~ FALSE,
                              .default=recording_device_lost
                          ),
                          recording_quality_code = case_when(
                              deployment_code == 'NEFSC_GOM_202301_USTR04' & recording_code == 'SOUNDTRAP_RECORDING' ~ 'COMPROMISED',
                              deployment_code == 'NEFSC_MA-RI_202311_MUSK01' & recording_code == 'SOUNDTRAP_RECORDING' ~ 'GOOD',
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' & recording_code == 'SOUNDTRAP_RECORDING' ~ 'GOOD',
                              deployment_code == 'NEFSC_VA_202310_ES02' & recording_code == 'SOUNDTRAP_RECORDING' ~ 'COMPROMISED',
                              .default = recording_quality_code
                          ),
                          recording_device_codes = case_when(
                              deployment_code == 'NEFSC_MA-RI_202309_NS03' & recording_device_codes == 'SOUNDTRAP-NA' ~ 'SOUNDTRAP-7414',
                              deployment_code == 'NEFSC_MA-RI_202311_MUSK01' & recording_device_codes == 'SOUNDTRAP-NA' ~ 'SOUNDTRAP-1677778970',
                              deployment_code == 'NEFSC_MA-RI_202411_MUSK01' & recording_device_codes == 'SOUNDTRAP-NA' ~ 'SOUNDTRAP-8924',
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' & recording_device_codes == 'SOUNDTRAP-NA' ~ 'SOUNDTRAP-5458',
                              deployment_code == 'PARKSAUSTRALIA_CEMP_202404_CES' & recording_device_codes == 'SOUNDTRAP-NA' ~ 'SOUNDTRAP-5473',
                              .default = recording_device_codes
                          ),
                          recording_timezone = case_when(
                              deployment_code == 'NEFSC_MA-RI_202402_PWN04' & recording_code == 'SOUNDTRAP_RECORDING' ~ NA_character_,
                              deployment_code == 'PARKSAUSTRALIA_TWOROCKS_202211_TRE' ~ 'UTC',
                              .default = recording_timezone
                          )
        )
        # filling PMEL fields from NRS smartsheet
        rec_out <- fillFromOther(rec_out, 
                                 nrs_deployment,
                      cols=c('recording_code', 'recording_device_codes', 'recording_device_depth_m'),
                      by='deployment_code')
        out <- list(deployments=dep_out,
                    recordings=rec_out)
        
        if(nrow(rec_int_out) > 0) {
            out$recording_intervals <- rec_int_out
        }
        if(!is.null(sensor_datasets)) {
            depsToUpload <- unique(c(out$deployments$deployment_code,
                                     out$recordings$deployment_code))
            sd <- filter(sensor_datasets, deployment_code %in% depsToUpload)
            noFile <- is.na(sd$filename)
            if(any(noFile)) {
                warning(sum(noFile), ' devices are missing temperature data')
                out$warnings <- addWarning(
                    vector('list', length=0), 
                    deployment=sd$deployment_code[noFile],
                    type='Missing Temperature Dataset',
                    table='sensor_datasets',
                    message=paste0("No temperature file found for device '",
                                   sd$sensor_dataset_device_code[noFile], '"'))
            }
            out$sensor_datasets <- sd
            out$sensor_values <- filter(sensor_values, deployment_code %in% depsToUpload)
        }
        out
    }),
    # final checks ----
    tar_target(db_check, {
        out <- checkAlreadyDb(combined_data, db)
        out <- dropAlreadyDb(out, drop=!params$export_already_in_db)
        out <- checkMakTemplate(out,
                                templates=templates,
                                # mandatory=mandatory_fields,
                                ncei=FALSE,
                                dropEmpty = TRUE,
                                dropExtra=!params$keep_extra_columns,
                                dropMandatoryNA=params$drop_mandatory_na)
        out <- checkDbValues(out, db, updateDeviceOrgs=params$update_device_orgs)
        out <- checkDbReplacements(out, db, replaceWithNA = params$replace_db_with_na)
        checkWarnings(out)
        out
    }),
    tar_target(output_dir, 'outputs'),
    tar_target(output, {
        writeTemplateOutput(db_check, folder=output_dir)
        output_dir
    }, format='file'),
    tar_target(validatr, {
        validate_submission(output, 
                            output_file = file.path(output_dir, 'validation_results.csv'),
                            verbose=FALSE)
    })
)

# QUESTIONS ----

# FPODS - default sample rate and freq range 1 million 10 bit samples per second
## frange is 20kHz - 220kHz

# TODO 

# are we going to have non-soundstrap data to run? Currently only place to get instrument
# is from PA Data Upload sheet, but this will not have entries for deployments that
# are lost. So, currently a problem where lost deployments do not get a recorder
# type (soundtrap,haru) - yes but not now

# Will recording_channel ever not be 1? - yes but not now

# NOTES ON STATUS ----

# NEFSC_MA-RI_202411_MUSK01 is not in Smart Deployment
# PARKSAUSTRALIA_CEMP_202404_CES is not in Smart

# All AVASTs are in ??? status
