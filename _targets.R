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
                 'httpuv', 'bigrquery',
                 'googledrive', 'readxl', 'tidyr', 'yaml',
                 'makaraValidatr')
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source('functions/makara-functions.R')
tar_source('functions/nefsc-metadata-functions.R')

# Can be set to "always" or "never"
reload_database <- 'never'
use_local_database <- FALSE

# don't change this
if(!tar_exist_objects('db_raw')) {
    reload_database <- 'always'
}

list(
    # parameters ----
    # Values you can adjust to change how things run
    tar_target(params, {
        list(
            # possible options 'READY', 'PENDING', 'IMPORTED', 'LOST', 'NA'
            'pacm_status_to_export' = c('READY'),
            # Whether or not to export data already present in DB TRUE/FALSE
            'export_already_in_db' = F,
            # identify specific deployments to skip, if wanted
            'skip_deployments' = c('NEFSC_TEMP-EXP_202503_AVAST_ST7393',
                                   'NEFSC_TEMP-EXP_202503_AVAST_ST8024',
                                   'NEFSC_TEMP-EXP_202503_AVAST_ST8546') #c('PARKSAUSTRALIA_CEMP_202404_CES')
        )
    }),
    # constants ----
    # Put any hard coded values here for transparency
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
    # tar_target(template_dir, 'templates'),
    tar_target(templates, {
        # now uses makaraValidatr
        formatBasicTemplates()
    }),
    # db tables ----
    # secrets has DB passwords, smartsheets key and IDs
    tar_target(secrets_file, '.secrets/secrets.yml'),
    tar_target(secrets, {
        read_yaml(secrets_file)
    }),
    tar_target(db_raw, {
        ds <- bq_dataset(secrets$bq_project, 
                         secrets$bq_dataset)
        tb_ref <- bq_dataset_query(ds, query = "select * from view_reference_codes")
        df_ref <- bq_table_download(tb_ref)
        
        tb_org <- bq_dataset_query(ds, query = "select * from view_organization_codes")
        df_org <- bq_table_download(tb_org)
        
        recint_q <- bq_dataset_query(ds, query = "select 
                             ri.recording_interval_start_datetime,
                             ri.recording_interval_end_datetime,
                             r.recording_code,
                             d.deployment_code
                             from recording_intervals ri 
                             left join 
                             recordings r 
                             on ri.recording_id = r.id
                             left join
                             deployments d
                             on  r.deployment_id = d.id")
        
        recint_df <- bq_table_download(recint_q)
        
        list(db_ref=df_ref,
             db_org=df_org,
             db_rec_int=recint_df)
    }, cue=tar_cue(reload_database)),
    # transform into list of db$table_name
    tar_target(db, {
        result <- split(db_raw$db_org, db_raw$db_org$table)
        result <- lapply(result, function(x) {
            code_prefix <- switch(
                x$table[1],
                'analyses' = 'analysis_code',
                paste0(gsub('s$', '', x$table[1]), '_code')
            )
            names(x)[3] <- code_prefix
            keepCol <- which(sapply(x, function(col) !all(is.na(col))))
            x[keepCol]
        })
        result$recording_intervals <- db_raw$db_rec_int
        result$reference_codes <- db_raw$db_ref
        result
    }),
    # tar_target(db, {
    #     if(isTRUE(use_local_database)) {
    #         db_folder <- 'local_db'
    #         result <- list()
    #         result$deployments <- readRDS(file.path(db_folder, 'deployments.rds'))
    #         result$sites <- readRDS(file.path(db_folder, 'sites.rds'))
    #         result$devices <- readRDS(file.path(db_folder, 'devices.rds'))
    #         result$projects <- readRDS(file.path(db_folder, 'projects.rds'))
    #         result$recordings <- readRDS(file.path(db_folder, 'recordings.rds'))
    #         result$recording_intervals <- readRDS(file.path(db_folder, 'recording_intervals.rds'))
    #         result$recordings_devices <- readRDS(file.path(db_folder, 'recordings_devices.rds'))
    #         return(result)
    #     }
    #     con <- try(DBI::dbConnect(
    #         RPostgres::Postgres(),
    #         host = secrets$makara_host,
    #         port = secrets$makara_port,
    #         dbname =secrets$makara_dbname,
    #         user = secrets$makara_user,
    #         password = secrets$makara_pw
    #     ), silent=TRUE)
    #     if(inherits(con, 'try-error')) {
    #         stop('Could not connect to database to load new Makara data.')
    #         # tar_cancel(TRUE)
    #     }
    #     on.exit(DBI::dbDisconnect(con))
    #     sites <- DBI::dbGetQuery(con, 'select * from sites')
    #     deployments <- DBI::dbGetQuery(con, 'select * from deployments')
    #     recordings <- DBI::dbGetQuery(con, 'select * from recordings')
    #     projects <- DBI::dbGetQuery(con, 'select * from projects')
    #     devices <- DBI::dbGetQuery(con, 'select * from devices')
    #     rec_dev <- DBI::dbGetQuery(con, 'select * from recordings_devices')
    #     rec_int <- DBI::dbGetQuery(con, 'select * from recording_intervals')
    #     analyses <- DBI::dbGetQuery(con, 'select * from analyses')
    #     
    #     list(
    #         sites=sites,
    #         deployments=deployments,
    #         recordings=recordings,
    #         projects=projects,
    #         devices=devices,
    #         recordings_devices=rec_dev,
    #         recording_intervals=rec_int,
    #         analyses=analyses
    #     )
    # }, cue=tar_cue(reload_database)),
    
    # google qaqc ----
    tar_target(qaqc_google_raw, {
        sheetId <- as_id(secrets$google_qaqc_sheet)
        qaqc_file <- tempfile(fileext = '.xlsx')
        drive_download(file = sheetId, path=qaqc_file, overwrite = TRUE)
        sheetNames <- excel_sheets(qaqc_file)
        result <- lapply(sheetNames, function(x) {
            one <- read_excel(qaqc_file, sheet=x, skip=5, col_types = 'list')
            one$sheet_name <- x
            one <- janitor::clean_names(one)
            one
        })
        result
    }, cue=tar_cue('always')),
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
    }, cue=tar_cue('always')),
    tar_target(instrument_tracking, {
        instrument_tracking_raw
    }),
    tar_target(st_deployment_raw, {
        result <- readStDeploymentSmart(secrets)
        result
    }, cue=tar_cue('always')),
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
    # Temperature meta checks ----
    tar_target(temp_devices, {
        dropIx <- which(st_deployment_raw$Status == 'Deployed' &
                            st_deployment_raw$Name == 'NEFSC_VA_202409_PWNVA01')
        dropIx <- c(dropIx, which(is.na(st_deployment_raw$Status)))
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
        # some deploy are here twice bc two STs, combine and distinct
        # multiRecorderDep <- names(which(table(deployment$deployment_code) > 1))
        # # deployments with multiple recorders get 
        # multiDrop <- numeric(0)
        # for(d in multiRecorderDep) {
        #     thisIx <- which(deployment$deployment_code == d)
        #     if(length(thisIx) == 1) {
        #         next
        #     }
        #     sumNa <- apply(sapply(deployment[thisIx, ], is.na), 1, sum)
        #     # either take the row with least NA vals, or if tied the first
        #     inNa <- thisIx[which(sumNa == min(sumNa))[1]]
        #     multiDrop <- c(multiDrop, thisIx[thisIx != inNa])
        # }
        # if(length(multiDrop) > 0) {
        #     deployment <- deployment[-multiDrop, ]
        # }
        deployment %>% 
            mutate(device_code = strsplit(deployment_device_codes, ',')) %>% 
            unnest(device_code) %>% 
            distinct() %>%
            select(deployment_code, device_code)
            # summarise(deployment_device_codes=paste0(deployment_device_codes, collapse=','),
            #           .by=deployment_code)
    }),
    # FPOD ----
    tar_target(fpod_times, {
        fpodFile <- 'FPOD_Dates.csv'
        data <- read.csv(fpodFile, stringsAsFactors = FALSE)
        data$fpod_start <- formatDatetime(date=data$start_date,
                                          time=data$start_time,
                                          warn=FALSE)
        data$fpod_end <- formatDatetime(date=data$end_date,
                                        time=data$end_time,
                                        warn=FALSE)
        data <- rename(data, deployment_code=deployment)
        data$deployment_code <- gsub(' ', '', data$deployment_code)
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
                    x$recording_code[thisG] <- paste0(x$recording_code[thisG], '_', seq_along(thisG))
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
        multiRecorderDep <- names(which(table(rec_out$deployment_code) > 1))
        # deployments with multiple recorders get 
        multiDrop <- numeric(0)
        for(d in multiRecorderDep) {
            thisIx <- which(dep_out$deployment_code == d)
            if(length(thisIx) == 1) {
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
        fpod_out$recording_filetypes <- NA
        fpod_out$recording_channel <- constants$fpod_channel
        fpod_out$fpod_device <- NULL
        fpod_out <- distinct(fpod_out)
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
                                # mandatory=mandatory_fields,
                                ncei=FALSE,
                                dropEmpty = TRUE)
        out <- checkDbValues(out, db)
        checkWarnings(out)
        out
    }),
    tar_target(output, {
        writeTemplateOutput(db_check, folder='outputs')
        'outputs'
    }, format='file'),
    tar_target(validatr, {
        validate_submission(output, 
                            output_file = 'outputs/validation_results.csv',
                            verbose=FALSE)
    })
)

# QUESTIONS ----

# FPODS - default sample rate and freq range 1 million 10 bit samples per second
## frange is 20kHz - 220kHz

# TODO 
# checking for previously lost updates
# Update dbValueChecker to see if x$devices x$projects whatever exists too
# alreadyDbChecker should probably check more/all inputs. Can I make a helper
# so that that isnt tedious...

# are we going to have non-soundstrap data to run? Currently only place to get instrument
# is from PA Data Upload sheet, but this will not have entries for deployments that
# are lost. So, currently a problem where lost deployments do not get a recorder
# type (soundtrap,haru) - yes but not now

# MARI202402 PWN04 and SBNMS 202312 SB03 are UNUSABLE but causing warnings for missing
# currently only affecting the fpod data for these deployments if removing
# stuff already in DB

# Will recording_channel ever not be 1? - yes but not now

# NOTES ON STATUS ----

# NEFSC_MA-RI_202411_MUSK01 is not in Smart Deployment
# PARKSAUSTRALIA_CEMP_202404_CES is not in Smart

# All AVASTs are in ??? status

# MAYBE A PROBLEM - Double check how metadata is working out for deployments that
# have two Soundtraps - these get double entries in the smort shorts so double
# check that outputs are not duplicated. In the temperature devices testing you
# end up with satellite and such device codes with no number - incorrect
