# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed.

# Set target options:
tar_option_set(
    packages = c("dplyr", 'rjson', 'lubridate', 'httr', 
                 'googledrive', 'readxl', 'tidyr', 'yaml', 'DBI', 'RPostgres')
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source('functions/makara-functions.R')

# Can be set to "always" or "never"
reload_data <- 'thorough'

list(
    # parameters ----
    tar_target(params, {
        list(
            # possible options 'READY', 'PENDING', 'IMPORTED', 'LOST', 'NA'
            'pacm_status_to_export' = c('READY'),
            # Whether or not to export data already present in DB TRUE/FALSE
            'export_already_in_db' = FALSE,
            # identify specific deployments to skip, if wanted
            'skip_deployments' = c() #c('PARKSAUSTRALIA_CEMP_202404_CES')
        )
    }),
    # constants ----
    # any hard coded values here for transparency
    tar_target(constants, {
        list(
            'platform' = 'BOTTOM_MOUNTED_MOORING',
            'fpod_duration' = 3600,
            'fpod_interval' = 0,
            'fpod_sr' = 1000,
            'fpod_nchannels' = 1,
            'fpod_bits' = 10,
            'fpod_channel' = 1,
            'fpod_tz' = 'UTC',
            'instrument_type' = 'SOUNDTRAP'
        )
    }),
    # templates ----
    tar_target(template_dir, 'templates'),
    tar_target(templates, {
        tempFiles <- list.files(template_dir, pattern='csv$', full.names=TRUE, recursive=TRUE)
        result <- lapply(tempFiles, function(x) {
            table <- read.csv(x, stringsAsFactors=FALSE)
            table <- lapply(table, as.character)
            table
        })
        names(result) <- gsub('\\.csv', '', basename(tempFiles))
        numCols <- list(
            'analyses' = c('analysis_sample_rate_khz',
                           'analysis_min_frequency_khz',
                           'analysis_max_frequency_khz'),
            'detections' = c('detection_effort_secs',
                             'detection_n_validated',
                             'detection_n_total',
                             'detection_latitude',
                             'detection_longitude',
                             'detection_received_level_db',
                             'detection_n_animals',
                             'detection_n_animals_min',
                             'detection_n_animals_max',
                             'localization_latitude',
                             'localization_latitude_min',
                             'localization_latitude_max',
                             'localization_longitude',
                             'localization_longitude_min',
                             'localization_longitude_max',
                             'localization_distance_m',
                             'localization_distance_m_min',
                             'localization_distance_m_max',
                             'localization_bearing',
                             'localization_bearing_min',
                             'localization_bearing_max',
                             'localization_depth_n_signals',
                             'localizatoin_depth_m',
                             'localization_depth_m_min',
                             'localization_depth_m_max'),
            'deployments' = c('deployment_water_depth_m', 
                              'deployment_latitude', 
                              'deployment_longitude',
                              'recovery_latitude',
                              'recovery_longitude'),
            'recordings' = c('recording_duration_secs',
                             'recording_interval_secs',
                             'recording_sample_rate_khz',
                             'recording_bit_depth',
                             'recording_channel',
                             'recording_n_channels',
                             'recording_usable_min_frequency_khz',
                             'recording_usable_max_frequency_khz',
                             'recording_device_depth_m'),
            'recording_intervals' = c('recording_interval_channel',
                                      'recording_interval_min_frequency_khz',
                                      'recording_interval_max_frequency_khz'),
            'devices' = c(),
            'projects' = c(),
            'sites' = c('site_latitude', 'site_longitude'),
            'track_positions' = c('track_position_latitude',
                                  'track_position_longitude',
                                  'track_position_speed_knots',
                                  'track_position_depth_m'),
            'tracks' = c()
            
        )
        boolCols <- list(
            'analyses' = c('analysis_release_data',
                           'analysis_release_pacm'),
            'recordings' = c('recording_redacted',
                             'recording_device_lost'),
            'deployments' = c(),
            'detections' = c(),
            'recording_intervals' = c(),
            'devices' = c(),
            'projects' = c(),
            'sites' = c(),
            'track_positions' = c(),
            'tracks' = c()
        )
        for(n in names(result)) {
            for(col in numCols[[n]]) {
                result[[n]][[col]] <- as.numeric(result[[n]][[col]])
            }
            for(col in boolCols[[n]]) {
                result[[n]][[col]] <- as.logical(result[[n]][[col]])
            }
        }
        result
    }),
    tar_target(mandatory_fields, {
        list(
            'deployments' = list(
                'always' = c('organization_code', 'deployment_code', 'deployment_platform_type_code', 
                             'deployment_datetime', 'deployment_latitude', 'deployment_longitude'),
                'ncei' = c('project_code','site_code', 'recovery_datetime', 'recovery_longitude', #site if stationary
                           'recovery_latitude') 
            ),
            'detections' =  list(
                'always' = c('deployment_organization_code', 'deployment_code', 'analysis_code',
                             'analysis_organization_code',
                             'detection_start_datetime', 'detection_end_datetime' ,
                             'detection_effort_secs', 'detection_sound_source_code',
                             'detection_call_type_code', 'detection_result_code'),
                'ncei' = c()
            ),
            'analyses' = list(
                'always' = c('deployment_organization_code', 'deployment_code', 'analysis_code', 'recording_codes',
                             'analysis_organization_code',
                             'analysis_sound_source_codes', 'analysis_granularity_code', 
                             'analysis_sample_rate_khz', 'analysis_processing_code', 
                             'analysis_quality_code', 'analysis_protocol_reference',
                             'analysis_release_data', 'analysis_release_pacm', 'detector_codes'),
                'ncei' = c('analysis_start_datetime', 'analysis_end_datetime', 'analysis_min_frequency_khz',
                           'analysis_max_frequency_khz')
            ),
            'recordings' = list(
                'always' = c('organization_code', 'deployment_code', 'recording_code', 
                             'recording_device_codes', 'recording_start_datetime', 'recording_interval_secs',
                             'recording_sample_rate_khz', 'recording_duration_secs',
                             'recording_n_channels', 'recording_timezone'), # many are only if not lsot
                'ncei' = c('recording_end_datetime', 'recording_bit_depth', 'recording_channel',
                           'recording_quality_code', 'recording_device_depth_m', 'recording_json')
            ),
            'recording_intervals' = list(
                'always' = c('organization_code', 'deployment_code', 'recording_code',
                             'recording_interval_quality_code'),
                'ncei' = c()
            ),
            'devices' = list(
                'always' = c('organization_code', 'device_code', 'device_type_code'),
                'ncei' = c()
            ),
            'projects' = list(
                'always' = c('organization_code', 'project_code', 'project_contacts'),
                'ncei' = c()
            ),
            'sites' = list(
                'always' = c('organization_code', 'site_code'),
                'ncei' = c()
            ),
            'track_positions' = list(
                'always' = c('organization_code', 'deployment_code', 'track_code',
                             'track_position_datetime',
                             'track_position_latitude',
                             'track_position_longitude'),
                'ncei' = c()
            ),
            'tracks' = list(
                'always' = c('organization_code', 'deployment_code', 'track_code'),
                'ncei' = c()
            )
        )
    }),
    # db tables ----
    tar_target(secrets_file, '.secrets/secrets.yml'),
    tar_target(secrets, {
        read_yaml(secrets_file)
    }),
    tar_target(db, {
        con <- try(DBI::dbConnect(
            RPostgres::Postgres(),
            host = secrets$makara_host,
            port = secrets$makara_port,
            dbname =secrets$makara_dbname,
            user = secrets$makara_user,
            password = secrets$makara_pw # replace with actual password
        ), silent=TRUE)
        if(inherits(con, 'try-error')) {
            warning('Could not connect to database to load new Makara data.')
            tar_cancel(TRUE)
        }
        on.exit(DBI::dbDisconnect(con))
        sites <- DBI::dbGetQuery(con, 'select * from sites')
        deployments <- DBI::dbGetQuery(con, 'select * from deployments')
        recordings <- DBI::dbGetQuery(con, 'select * from recordings')
        projects <- DBI::dbGetQuery(con, 'select * from projects')
        devices <- DBI::dbGetQuery(con, 'select * from devices')
        rec_dev <- DBI::dbGetQuery(con, 'select * from recordings_devices')
        rec_int <- DBI::dbGetQuery(con, 'select * from recording_intervals')
        analyses <- DBI::dbGetQuery(con, 'select * from analyses')
        
        list(
            sites=sites,
            deployments=deployments,
            recordings=recordings,
            projects=projects,
            devices=devices,
            recordings_devices=rec_dev,
            recording_intervals=rec_int,
            analyses=analyses
        )
    }, cue=tar_cue(reload_data)),
    
    # google qaqc ----
    tar_target(qaqc_g_file, 'QAQC.xlsx'),
    tar_target(qaqc_google_raw, {
        sheetId <- as_id(secrets$google_qaqc_sheet)
        drive_download(file = sheetId, path=qaqc_g_file, overwrite = TRUE)
        sheetNames <- excel_sheets(qaqc_g_file)
        result <- lapply(sheetNames, function(x) {
            one <- read_excel(qaqc_g_file, sheet=x, skip=5, col_types = 'list')
            one$sheet_name <- x
            one <- janitor::clean_names(one)
            one
        })
        result
    }, cue=tar_cue(reload_data)),
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
        timeCols <- c(
            'recording_start_datetime',
            'recording_end_datetime',
            'deployment_datetime',
            'recovery_datetime',
            'recording_usable_start_datetime',
            'recording_usable_end_datetime'
        )
        result <- bind_rows(lapply(qaqc_google_raw, function(x) {
            if(ncol(x) <= 2) {
                return(NULL)
            }
            one <- myRenamer(x, map=googsMap)
            one <- one[!is.na(one$deployment_code), ]
            if(!'st_serial_number' %in% names(one)) {
                one$st_serial_number <- NA
            }
            one$st_serial_number <- as.character(one$st_serial_number)
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
            select(one, all_of(keepCols))
        }))
        result 
    }),
    # smart sheets ----
    tar_target(data_upload_raw, {
        readPaDataSmart(secrets)
    }, cue=tar_cue(reload_data)),
    tar_target(data_upload, {
        # Only has instruemnt type or QAQC status we might care about
        dataUpMap <- list(
            'Project Name' = 'deployment_code',
            'Instrument Type' = 'instrument_type',
            'Status' = 'qaqc_status'
        )
        upCols <- c('deployment_code', 'qaqc_status', 'instrument_type')
        myRenamer(data_upload_raw, map=dataUpMap) %>% 
            select(all_of(upCols)) %>% 
            filter(!is.na(deployment_code)) %>% 
            mutate(instrument_type = toupper(instrument_type),
                   recording_code = paste0(instrument_type, '_RECORDING'))
    }),
    # not currently used for anything
    tar_target(instrument_tracking_raw, {
        readInsTrackSmart(secrets)
    }, cue=tar_cue(reload_data)),
    tar_target(instrument_tracking, {
        instrument_tracking_raw
    }),
    tar_target(st_deployment_raw, {
        result <- readStDeploymentSmart(secrets)
        result
    }, cue=tar_cue(reload_data)),
    tar_target(st_deployment, {
        dropIx <- which(st_deployment_raw$Status == 'Deployed' &
                            st_deployment_raw$Name == 'NEFSC_VA_202409_PWNVA01')
        dropIx <- c(dropIx, which(is.na(st_deployment_raw$Status)))
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
            'Sat. Tracker Serial Number' = 'satellite_number'
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
            'recording_device_depth_m'
        )
        deployment <- myRenamer(st_deployment_raw[-dropIx, ],
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
            deployment$deploy_time)
        deployment$recovery_datetime <- formatDatetime(
            deployment$recovery_date,
            deployment$recovery_time)
        
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
        fpod <- myRenamer(st_deployment_raw[-dropIx, ],
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
    # combine sources ----
    tar_target(combined_data, {
        # removing one NA deployment
        dep <- st_deployment$deployments %>% 
            filter(!is.na(deployment_status)) %>% 
            rename(st_deploy_time = deployment_datetime,
                   st_recovery_time = recovery_datetime)
        result <- qaqc_google
        result$deployment_platform_type_code <- constants$platform
        if(length(params$skip_deployments) > 0) {
            dropIx <- which(result$deployment_code %in% params$skip_deployments)
            if(length(dropIx) > 0) {
                warning('Removed data from ', length(dropIx), ' deployments', 
                        " in 'params$skip_deployments'")
            }
            result <- result[-dropIx, ]
        }
        # result <- left_join(
        #     result,
        #     distinct(select(data_upload, deployment_code, instrument_type, recording_code)),
        #     by='deployment_code',
        #     relationship='many-to-one')
        result$instrument_type <- constants$instrument_type
        result$recording_code <- paste0(result$instrument_type, '_RECORDING')
        # Googs do not have device ID unless in comment, here we fix
        # for cases where multiple recordings
        multiRecorderDep <- names(which(table(qaqc_google$deployment_code) > 1))
        result$multiRecorder <- result$deployment_code %in% multiRecorderDep
        result <- bind_rows(lapply(split(result, result$multiRecorder), function(x) {
            if(isTRUE(x$multiRecorder[1])) {
                x$device_code <- NA
                for(d in multiRecorderDep) {
                    depIds <- dep$device_code[dep$deployment_code == d]
                    thisG <- which(x$deployment_code == d)
                    if(all(!is.na(x$st_serial_number[thisG]))) {
                        x$device_code[thisG] <- x$st_serial_number[thisG]
                        next
                    }
                    x$recording_code[thisG] <- paste0(x$recording_code[thisG], '_', seq_along(thisG))
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
            # if google had no deploy/recovery time, then use smartsheet time
            noDep <- x$deployment_datetime == '' | is.na(x$deployment_datetime)
            noRec <- x$recovery_datetime == '' | is.na(x$recovery_datetime)
            x$deployment_datetime[noDep] <- x$st_deploy_time[noDep]
            x$recovery_datetime[noRec] <- x$st_recovery_time[noRec]
            x
        }))
        result <- addNefscProjectCode(result)
        result$pacm_db_status[is.na(result$pacm_db_status)] <- 'NA'
        result <- unite(result, 'recording_comments', c('recording_comments', 'depth_comment'), sep=';', na.rm=TRUE)
        result <- result %>% 
            filter(pacm_db_status %in% params$pacm_status_to_export)
        dep_out <- select(result, any_of(c(names(templates$deployments), 'pacm_db_status', 'deployment_status')))
        rec_out <- select(result, any_of(names(templates$recordings)))
        fpodCommonCols <- c('organization_code', 
                            'deployment_code', 
                            'recording_start_datetime', 
                            'recording_end_datetime',
                            'recording_timezone')
        fpod_out <- left_join(
            select(rec_out, all_of(fpodCommonCols)),
            select(st_deployment$fpod, 'deployment_code', 'fpod_device', 'recording_device_depth_m', 
                   'recording_comments'='depth_comment'),
            by='deployment_code'
        ) %>% 
            filter(!is.na(fpod_device))
        # FPOD is mostly constants
        fpod_out$recording_device_codes <- fpod_out$fpod_device
        fpod_out$recording_duration_secs <- constants$fpod_duration
        fpod_out$recording_interval_secs <- constants$fpod_interval
        fpod_out$recording_sample_rate_khz <- constants$fpod_sr
        fpod_out$recording_n_channels <- constants$fpod_nchannels
        fpod_out$recording_timezone <- constants$fpod_tz
        fpod_out$recording_code <- 'FPOD_RECORDING'
        fpod_out$recording_bit_depth <- constants$fpod_bits
        fpod_out$recording_filetypes <- NA
        fpod_out$recording_channel <- constants$fpod_channel
        fpod_out$fpod_device <- NULL
        rec_out <- bind_rows(rec_out, fpod_out)
        rec_int_out <- select(result, 
                              any_of(c(names(templates$recording_intervals), 
                                       'compromised_starts',
                                       'compromised_ends')))
        out <- list(deployments=dep_out,
                    recordings=rec_out)
        rec_int_out <- formatRecordingIntervals(rec_int_out)
        if(nrow(rec_int_out) > 0) {
            out$recording_intervals <- rec_int_out
        }
        out
    }),
    # final checks ----
    tar_target(db_check, {
        out <- checkAlreadyDb(combined_data, db)
        out <- dropAlreadyDb(out, drop=!params$export_already_in_db)
        out <- checkMakTemplate(out,
                                templates=templates,
                                mandatory=mandatory_fields,
                                ncei=FALSE)
        out <- checkDbValues(out, db)
        checkWarnings(out)
        out
    }),
    tar_target(output_files, {
        if(!dir.exists('outputs')) {
            dir.create('outputs')
        }
        for(n in names(db_check)) {
            outFile <- file.path('outputs', paste0(n, '.csv'))
            write.csv(db_check[[n]], file=outFile, row.names=FALSE)
        }
    })
)

# QUESTIONS ----

# FPODS - default sample rate and freq range 1 million 10 bit samples per second
## frange is 20kHz - 220kHz

# TODO fpod stuff Checking comments
# checking for previously lost updates

# are we going to have non-soundstrap data to run? Currently only place to get instrument
# is from PA Data Upload sheet, but this will not have entries for deployments that
# are lost. So, currently a problem where lost deployments do not get a recorder
# type (soundtrap,haru)

# MARI202402 PWN04 and SBNMS 202312 SB03 are UNUSABLE but causing warnings for missing
# currently only affecting the fpod data for these deployments if removing
# stuff already in DB

# Will recording_channel ever not be 1?

# NOTES ON STATUS ----

# NEFSC_MA-RI_202411_MUSK01 is not in Smart Deployment
# PARKSAUSTRALIA_CEMP_202404_CES is not in Smart

# All AVASTs are in ??? status
