# list deployments
# list temperature directories
# match dirs to deps
# check for vemco and soundtrap where appropriate
# deps need rec codes and vemco codes
# files on server ----
tempDir <- 'Z:/ANCILLARY_DATA/TEMPERATURE_DATA/'
system.time({
  tempFiles <- list.files(tempDir, recursive=TRUE, full.names=TRUE, pattern='csv$') #4684->4689->4706->4715
}) #20, 1.9, 1.9

tempDeps <- basename(dirname(tempFiles))
ix <- grep('temperature', tempDeps, ignore.case=T)
tempDeps[ix] <- basename(dirname(dirname(tempFiles[ix])))
library(dplyr)
library(targets)

filedf <- data.frame(deployment_code = tempDeps,
                     full=tempFiles,
                     file = basename(tempFiles)) %>% 
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
    )) # 5-1 1123->1128->1145->1154 when filter filter
# smartsheets device codes ----
tar_load(temp_devices)
all_devices <- temp_devices
# match device codes to files ----
filtfiles <- dplyr::filter(filedf, filtered)
hm <- bind_rows(lapply(split(filtfiles, list(filtfiles$deployment_code, filtfiles$type)), function(x) {
  if(nrow(x) == 0) {
    return(NULL)
  }
  thisDep <- x$deployment_code[1]
  thisType <- x$type[1]
  if(thisDep == 'NEFSC_SBNMS_202207_SB04' &&
     thisType == 'SOUNDTRAP') {
    x$device_code <- 'SOUNDTRAP-671666216'
    return(x)
  }
  thisMatch <- dplyr::filter(all_devices, 
                      deployment_code == thisDep,
                      type == thisType)
  
  if(thisType == 'HOBO' &&
     nrow(thisMatch) == 0) {
    checkGeneric <- dplyr::filter(all_devices,
                           deployment_code == thisDep,
                           device_code == 'TEMPERATURE_SENSOR-GENERIC')
    if(nrow(checkGeneric) == 1) {
      x$warning <- 'TIDBIT listed in DB as TEMPERATURE_SENSOR-GENERIC'
      x$device_code <- 'TEMPERATURE_SENSOR-GENERIC'
      return(x)
    }
  }
  if(nrow(x) != nrow(thisMatch)) {
    # msg <- paste0('Deployment ', thisDep, ' type ', thisType, 
    # ' has ', nrow(x), ' files and ', nrow(thisMatch), ' devices\n', sep='')
    if(nrow(thisMatch) != 0) {
      msg <- paste0(nrow(x), ' files and ', nrow(thisMatch), ' devices:',
                    paste0(thisMatch$device_code, collapse=','),
                    '(',paste0(thisMatch$source, collapse=','),')')
    } else {
      msg <- paste0(nrow(x), ' files and ', nrow(thisMatch), ' devices')
    }
    x$warning <- msg
  } else {
    x$device_code <- thisMatch$device_code
    if(nrow(x) != 1) {
      x$warning <- 'Multiple devices not actually matched yet'
      x$device_code <- paste0(x$device_code, '(', thisMatch$source, ')')
      ids <- gsub('SOUNDTRAP-', '', thisMatch$device_code)
      ids <- gsub('^HF_4_', '', ids)
      ids <- gsub('^STD5_', '', ids)
      ids <- gsub('_2018$', '', ids)
      for(i in seq_along(ids)) {
        matchIx <- which(grepl(ids[i], x$file))
        if(length(matchIx) == 1) {
          x$device_code[matchIx] <- thisMatch$device_code[i]
          x$warning[matchIx] <- NA
        }
      }
          
    }
    x
  }
  x
}))

View(hm[c('deployment_code', 'warning', 'device_code', 'type', 'file')])
# View(db$sensor_datasets[c('deployment_code', 'sensor_dataset_code')])
# temp_devices$has_dir <- temp_devices$deployment_code %in% tempDeps
# View(temp_devices)

sd <- select(hm,
             'deployment_code',
             'sensor_dataset_device_code' = 'device_code',
             'type',
             'filename' = 'full') %>% 
  # filter(!is.na(sensor_dataset_device_code)) %>% # maybe dont do this and warn
  mutate(sensor_dataset_comments = NA,
         sensor_dataset_variable_code = 'TEMP_C',
         organization_code = gsub('^([A-Z]*)_.*', '\\1', deployment_code),
         sensor_dataset_code = paste0('TEMP_DATA_', type))
sd <- bind_rows(lapply(split(sd, list(sd$deployment_code, sd$sensor_dataset_code)), function(x) {
  if(nrow(x) <= 1) {
    return(x)
  }
  x$sensor_dataset_code <- paste0(x$sensor_dataset_code, seq_len(nrow(x)))
  x
}))
sd$sensor_dataset_comments[sd$type == 'SOUNDTRAP'] <- 
  'Calibration applied: Tc = Tm - (-0.060*Tm - 1.26), where Tm is measured temperature'
table(sd$sensor_dataset_code)
# pipeline uploaders ----
tar_load(combined_data)
View(combined_data$deployments)
left_join(temp_devices,
          select(combined_data$recordings,
                 deployment_code,
                 device_code = recording_device_codes,
                 recording_code,
                 recording_device_lost),
          by=c('deployment_code', 'device_code')) %>% 
  left_join(
    select(combined_data$deployments,
           deployment_code,
           pacm_db_status,
           deployment_status),
    by='deployment_code') %>% 
  mutate(type = gsub('^([A-Z]*)-.*', '\\1', device_code)) %>% 
  View

# i think this currently gets matching files but sooo much is missing
# Many have ST but not filtered, many have not even that
# Need to make option of running qaqc temperature only to fill in 
# or first check whats already Mak'd
select(combined_data$deployments,
       deployment_code,
       pacm_db_status,
       deployment_status) %>% 
  left_join(temp_devices, by='deployment_code') %>% 
  left_join(select(combined_data$recordings,
                   deployment_code,
                   device_code = recording_device_codes,
                   recording_code,
                   recording_device_lost),
            by=c('deployment_code', 'device_code')) %>% 
  mutate(type = gsub('^([A-Z]*)-.*', '\\1', device_code)) %>% 
  left_join(filedf,
            by=c('deployment_code', 'type')) %>% 
  View

library(bigrquery)
ds <- bq_dataset('ggn-nmfs-pacm-dev-1', 'makara')
tb_ref <- bq_dataset_query(ds, query = "select * from devices")
dev <- bq_table_download(tb_ref)  

dbsens <- db$sensor_datasets %>% 
  left_join(dev[c('id', 'device_code')],
            by=c('device_id'='id')) %>% 
  select(deployment_code, device_code, sensor_dataset_code)
str(dbsens)
# check prev uploads covered ----
tar_load(db)
what <- doJoinCheck(db$sensor_datasets, sd, by=c('deployment_code', 'sensor_dataset_code'))
# down to 0!
# View(what)
newups <- doJoinCheck(sd, db$sensor_datasets, by=c('deployment_code', 'sensor_dataset_code'))
# 310 are new 394 are new 424,435, 439

# find deps with no filtered----
needsFilt <- filedf %>% 
  dplyr::filter(!is.na(type)) %>% 
  group_by(deployment_code, type) %>% 
  summarise(nFilt = sum(filtered),
            files = paste0(file, collapse=','),
            fulls = paste0(full, collapse=',')) %>% 
  dplyr::filter(nFilt == 0)
tar_load(db_check)
tar_load(db)
library(lubridate)
dep_times <- bind_rows(
  select(
    db$deployments,
    deployment_code, deployment_datetime, recovery_datetime
  ),
  select(
    db_check$deployments,
    deployment_code, deployment_datetime, recovery_datetime
  ) %>% mutate(deployment_datetime=ymd_hms(deployment_datetime),
               recovery_datetime=ymd_hms(recovery_datetime))
)
needsFiltDeps <- dep_times %>% 
  dplyr::filter(deployment_code %in% needsFilt$deployment_code) %>% 
  left_join(
    select(needsFilt, deployment_code, fulls, type),
    by='deployment_code'
  )
View(needsFiltDeps)
# by type do filtering 
# vemco has readvrcsv
# check hobo tids ----
genericTids <- filter(sd, grepl('TEMPERATURE_SENSOR', sensor_dataset_device_code))
tar_load(st_deployment) # same rows as raw
nrow(st_deployment$deployments)
deps <- st_deployment$deployments
genericTids$deployment_code %in% deps$deployment_code
tar_load(instrument_tracking)
genericTids$deployment_code %in% instrument_tracking[['Project Name']]
filter(instrument_tracking,
       .data[['Project Name']] %in% genericTids$deployment_code) %>% 
  View

allgens <- filter(all_devices, device_code == 'TEMPERATURE_SENSOR-GENERIC')
View(allgens)
# check for hobo files to try and pull devices
# cannot for vast majority - possible to pull from CSV files maybe
# if file.hobo == file.csv theres a janky header i can grab from probably
hoboFiles <- list.files(tempDir, recursive=TRUE, full.names=TRUE, pattern='hobo$')

allgens$has_hobo <- allgens$deployment_code %in% filter(hm, type == 'HOBO')$deployment_code
table(allgens$has_hobo)
View(allgens)
write.csv(allgens, file='generic_temps.csv')

# running new ifltering ----
readVrlCsvBork <- function(x, type='TEMP', name='Temperature_C', valIx=7) {
  result <- read.csv(x, skip=1, header=TRUE, stringsAsFactors = FALSE)
  typeColumn <- which(names(result) == 'RECORD.TYPE')
  if(length(typeColumn) == 0) {
    warning('File ', basename(x), ' does not appear to be a full VRL CSV offload')
    return(NULL)
  }
  timeColumn <- typeColumn + 1
  valueColumn <- typeColumn + valIx
  result <- result[c(typeColumn, timeColumn, valueColumn)] 
  names(result) <- c('Parameter', 'Time_UTC', name)
  result <- dplyr::filter(result, Parameter == type)
  if(nrow(result) == 0) {
    warning('File ', basename(x), ' did not contain any ', type,' data')
    return(NULL)
  }
  result$Parameter <- NULL
  result[[name]] <- as.numeric(result[[name]])
  result <- mutate(result,
                   Time_UTC = ymd_hms(Time_UTC),
                   Date = date(Time_UTC),
                   Month = month(Time_UTC),
                   Year = year(Time_UTC)
  )
  # result$Time_UTC <- ymd_hms(result$Time_UTC)
  vemId <- gsub(' ', '_', basename(x))
  vemId <- strsplit(vemId, '_')[[1]][2]
  result$id <- vemId
  result$file <- basename(x)
  result
}
filterTempData <- function(x, dateCol, format='%Y-%m-%d', start, end) {
  x$DATEFILTER <- as.Date(x[[dateCol]], format=format)
  x <- dplyr::filter(x, DATEFILTER >= start, DATEFILTER <= end)
  x$DATEFILTER <- NULL
  x
}

# 2357 vemco
# skip 2, 357 are temp_stats ix9
ix <- 7
readVrlCsvBork(needsFiltDeps$fulls[ix], 'TEMP', valIx=7)
readVrlCsvBork(needsFiltDeps$fulls[ix], 'TEMP_STATS', valIx=9)
needsFiltDeps[7,]

for(i in 1:nrow(needsFiltDeps)) {
  thisDep <- needsFiltDeps$deployment_code[i]
  if(thisDep %in% c('NEFSC_GOM_202112_SEALISLAND',
                    'NEFSC_SBNMS_202108_OLE01',
                    'NEFSC_SBNMS_202209_OLE01')) {
    next
  }
  thisFile <- needsFiltDeps$fulls[i]
  thisDir <- dirname(thisFile)
  thisType <- needsFiltDeps$type[i]
  tempStats <- c("NEFSC_SBNMS_201606_SB01", "NEFSC_SBNMS_201708_SB01", "NEFSC_SBNMS_201710_SB01")
  deploymentDate <- as.Date(needsFiltDeps$deployment_datetime[i], format = "%Y-%m-%d")
  thirdDay <- deploymentDate + 2 # to allow of logger acclimating
  recoveryDate <-  as.Date(needsFiltDeps$recovery_datetime[i], format = "%Y-%m-%d")
  secondLastDay <- recoveryDate - 1
  switch(thisType,
         'SOUNDTRAP' = {
           data <- read.csv(thisFile, stringsAsFactors = FALSE)
           data$DATETRY <- lubridate::mdy_hms(data$Datetime_UTC)
           if(anyNA(data$DATETRY)) {
             # print(basename(thisFile))
             data$DATETRY <- lubridate::dmy_hms(data$Datetime_UTC)
             if(anyNA(data$DATETRY)) {
               warning('double bruh', thisDep)
               next
             }
             data$Datetime_UTC <- data$DATETRY
           } else {
             data$Datetime_UTC <- data$DATETRY
           }
           data$DATETRY <- NULL
           stFile <- gsub('_ST_Internal_temp', '_Filtered_ST_Temp_data', thisFile)
           data$Datetime_UTC <- format(data$Datetime_UTC, format='%m-%d-%Y_%H:%M:%S')
           data <- filterTempData(data, 
                                  dateCol='Datetime_UTC',
                                  format='%m-%d-%Y',
                                  start=thirdDay, end=secondLastDay)
           if(nrow(data) == 0) {
             warning('no filt data ', thisDep)
             next
           }
           # print(stFile)
           write.csv(data, file=stFile, row.names=FALSE)
         },
         'VEMCO' = {
           if(thisDep %in% tempStats) {
             data <- readVrlCsvBork(thisFile, 'TEMP_STATS', valIx=9)
           } else {
             data <- readVrlCsvBork(thisFile, 'TEMP', valIx=7)
           }
           data <- distinct(data)
           vemId <- data$id[1]
           if(grepl('_SITE0[12]$', thisDir)) {
             thisDir <- gsub('SITE', 'NS', thisDir)
           }
           vemFile <- file.path(thisDir, paste0(thisDep,'_', vemId, '_Filtered_VEMCO_Temp_data.csv'))
           data <- filterTempData(data, dateCol='Time_UTC', start=thirdDay, end=secondLastDay)
           data$Time_UTC <- format(data$Time_UTC, format='%Y-%m-%d %H:%M:%S')
           if(nrow(data) == 0) {
             warning('no filt data ', thisDep)
             next
           }
           print(vemFile)
           # write.csv(data, file=vemFile, row.names=FALSE)
           
         }
  )
}

# checking more runs ----
tar_load(sensor_datasets)
sum(is.na(sensor_datasets$filename)) #291 5-15 pre-run, 245 5-19, 223 5-21 but 8 ore deps
View(sensor_datasets[is.na(sensor_datasets$filename), ])
sd <- sensor_datasets
sd$in_db <- sd$deployment_code %in% db$deployments$deployment_code
table(sd$in_db, is.na(sd$filename))
View(sd[is.na(sd$filename), ])
not_filt$in_db <- not_filt$deployment_code %in% sd$deployment_code[is.na(sd$filename)]
table(not_filt$in_db)

teleDir <- 'Z:/ANCILLARY_DATA/TELEMETRY_DATA/'
system.time({
  teleFiles <- list.files(teleDir, recursive=TRUE, full.names=TRUE, pattern='csv$|vrl$|vdat$') #4684->4689->4706->4715
}) #20, 1.9, 1.9

teleDeps <- basename(dirname(teleFiles))
vem_to_do <- not_filt$deployment_code[
  not_filt$type == 'VEMCO' & 
    not_filt$in_db & 
    !is.na(not_filt$type)]
vem_to_do %in% teleDeps

# plotters qaqc ----
ix <- 10
raw <- read.csv(sd$filename[ix])
val <- formatSensorValues(
    raw, 
    type=tolower(sd$type[ix]), 
    name=basename(sd$filename[ix])
)
plot(x=val$sensor_value_datetime, y=val$sensor_value_value, type='l')

plotSd <- function(x, ix=NULL) {
    if(!is.null(ix)) {
        x <- x[ix, ]
    }
    if('value' %in% names(x)) {
        raw <- x$value[[1]]
    } else {
        raw <- read.csv(x$filename)
    }
    vals <- formatSensorValues(
        raw,
        type=tolower(x$type)
    ) %>% 
        calcTempSpeed() %>% 
        mutate(tooFast = abs(rate) > 3)
    name <- paste0(x$deployment_code,
                   ':',
                   x$sensor_dataset_device_code,
                   '\n',
                   basename(x$filename)
    )
    plot(x=vals$sensor_value_datetime,
         y=vals$sensor_value_value,
         main=name,
         type='l')
    if(any(vals$tooFast, na.rm=TRUE)) {
        cat('\nFast points in red for', ix)
        points(x=vals$sensor_value_datetime[vals$tooFast],
               y=vals$sensor_value_value[vals$tooFast],
               col='red')
    }
}

calcTempSpeed <- function(x) {
    diffs <- as.numeric(difftime(x$sensor_value_datetime[2:nrow(x)],
                                 x$sensor_value_datetime[1:(nrow(x)-1)],
                                 units='hours'))
    rate <- diff(x$sensor_value_value)/diffs
    x$rate <- c(0, rate)
    x
}

png('test.png', width=600, height=400, units='px')
plotSd(sd, 20)
dev.off()

outDir <- 'outputs/temp_plots'
pb <- txtProgressBar(min=0, max=nrow(sd), style=3)
for(i in 1:nrow(sd)) {
# for(i in 358:nrow(sd)) {
    filename <- paste0('TempPlot_', i, '.png')
    filename <- file.path(outDir, filename)
    png(filename, width=600, height=400, units='px')
    plotSd(sd, i)
    dev.off()
    setTxtProgressBar(pb, value=i)
}
for(i in 1:nrow(sd)) {
    oldname <- paste0('TempPlot_', i, '.png')
    newname <- paste0(sd$deployment_code[i], '-', sd$sensor_dataset_device_code[i], '.png')
    file.copy(from=file.path(outDir, oldname), 
              to=file.path(outDir, 'renamed', newname),
              overwrite = TRUE)
}
pb <- txtProgressBar(min=0, max=nrow(sensor_datasets), style=3)
for(i in 1:nrow(sensor_datasets)) {
    if(is.na(sensor_datasets$filename[i])) next
    filename <- paste0('TempPlot_', i, '.png')
    filename <- file.path(outDir, filename)
    # png(filename, width=600, height=400, units='px')
    # plotSd(sensor_datasets, i)
    # dev.off()
    oldname <- paste0('TempPlot_', i, '.png')
    newname <- paste0(sensor_datasets$deployment_code[i], '-', sensor_datasets$sensor_dataset_device_code[i], '.png')
    file.copy(from=file.path(outDir, oldname), 
              to=file.path(outDir, 'renamed', newname),
              overwrite = TRUE)
    setTxtProgressBar(pb, value=i)
}

View(read.csv(sd$filename[358]))


logs <- processSoundtrapLogs('Y:/bottom_mounted/NEFSC_VA/NEFSC_VA_202504_CB03/7401_LOG-and-SUD/')

# check temp speed ----
calcTempSpeed <- function(x) {
    diffs <- as.numeric(difftime(x$sensor_value_datetime[2:nrow(x)],
                                 x$sensor_value_datetime[1:(nrow(x)-1)],
                                 units='hours'))
    rate <- diff(x$sensor_value_value)/diffs
    x$rate <- c(0, rate)
    x
}
normIx <- 315
norms <- formatSensorValues(
    read.csv(sd$filename[normIx]),
    type=tolower(sd$type[normIx])
) %>% calcTempSpeed
par(mfrow=c(2,1))
plot(norms$sensor_value_datetime, norms$sensor_value_value)
plot(norms$sensor_value_datetime, norms$rate)
badIx <- 959
bads <- formatSensorValues(
    read.csv(sd$filename[badIx]),
    type=tolower(sd$type[badIx])
) %>% calcTempSpeed
plot(bads$sensor_value_datetime, bads$sensor_value_value)
plot(bads$sensor_value_datetime, bads$rate)

#
lol <- doJoinCheck(sd, sensor_datasets, by=c('deployment_code', 'sensor_dataset_device_code'))
# upload batch ----
tar_load(sensor_datasets)
tar_load(sensor_values)

# NEFSC_GOM_202006_YORK skip me
# NEFSC_SBNMS_202209_OLE01
# NEFSC_SBNMS_202108_OLE01
skippers <- c('NEFSC_GOM_202006_YORK', #me, soundtrap bad
              'NEFSC_SBNMS_202209_OLE01',
              'NEFSC_SBNMS_202108_OLE01',
              'NEFSC_GOM_202112_SEALISLAND', #vemoc bad
              'MIT_SBNMS_202506_SBV01')
skippers %in% sd_out$deployment_code
sd_out <- sensor_datasets %>% 
    dplyr::filter(!is.na(filename),
                  deployment_code %in% db$deployments$deployment_code) %>% 
    select(organization_code,
           deployment_code,
           sensor_dataset_code,
           sensor_dataset_device_code,
           sensor_dataset_variable_code,
           sensor_dataset_uri,
           sensor_dataset_comments)
dropIx <- sd_out$deployment_code == 'NEFSC_GOM_202510_USTR12' &
    sd_out$sensor_dataset_code == 'TEMP_DATA_SOUNDTRAP2'
sd_out <- sd_out[!dropIx, ]
sensor_values <- dplyr::filter(sensor_values,
                       deployment_code %in% db$deployments$deployment_code) 
dropIx <- sensor_values$deployment_code == 'NEFSC_GOM_202510_USTR12' &
    sensor_values$sensor_dataset_code == 'TEMP_DATA_SOUNDTRAP2'
sensor_values <- sensor_values[!dropIx, ]
what <- doJoinCheck(db$sensor_datasets, sd_out, by=c('deployment_code', 'sensor_dataset_code'))
# down to 0!

newups <- doJoinCheck(sd_out, db$sensor_datasets, by=c('deployment_code', 'sensor_dataset_code'))
# 438

sv_data <- list('sensor_datasets' = sd_out,
                'sensor_values' = sensor_values)
tar_load(db)
sv_data <- checkAlreadyDb(sv_data, db)
sv_data <- dropAlreadyDb(sv_data, drop=FALSE)
sv_data <- checkMakTemplate(sv_data,
                        # templates=templates,
                        # mandatory=mandatory_fields,
                        ncei=FALSE,
                        dropEmpty = TRUE,
                        dropExtra=TRUE,
                        dropMandatoryNA=FALSE)
sv_data <- checkDbValues(sv_data, db, updateOrgs=TRUE)
sv_data <- checkDbReplacements(sv_data, db, replaceWithNA = FALSE)
View(sv_data)
writeTemplateOutput(sv_data, folder='temp_outputs')
makaraValidatr::validate_submission('temp_outputs')

# rerun one ----
rerunOneTemperature <- function(x, ix=NULL, dir=NULL, type=c('soundtrap', 'vemco'), 
                                vdat=file.path('Z:/CODE_LIBRARY/R/QAQC','vdat.exe')) {
    if(nrow(x) > 1) {
        if(is.null(ix)) stop('no')
        if(is.character(ix)) {
            ix <- which(x$projectName == ix)
        }
        x <- x[ix, ]
    }
    type <- match.arg(type)
    deploymentDate <- as.Date(x$deploymentDate, format = "%Y-%m-%d")
    thirdDay <- deploymentDate + 2 # to allow of logger acclimating
    recoveryDate <-  as.Date(x$recoveryDate, format = "%Y-%m-%d")
    secondLastDay <- recoveryDate - 1
    if(is.na(thirdDay) | is.na(secondLastDay)) {
        stop('NA days')
    }
    thisName <- paste0(x$projectName, '_', x$deviceId)
    thisTempDir <- file.path(x$tempBaseDir, x$tempDir)
    if(!is.null(vdat) &&
       file.exists(vdat)) {
        vdat_ok <- suppressWarnings(system2(path.expand(vdat), args = "--version", stdout = FALSE, stderr = FALSE))
        if(vdat_ok != 0) {
            tmpdir <- tempdir()
            newvdat <- file.path(tmpdir, 'vdat.exe')
            file.copy(from=vdat, to=newvdat)
            vdat <- newvdat
            on.exit(unlink(newvdat), add=TRUE)
        }
    }
    switch(type,
           'soundtrap' = {
               if(is.null(dir)) {
                   dir <- file.path(x$projectBaseDir, x$projectDir)
               }
               logFiles <- list.files(dir, pattern='xml$', recursive=TRUE, full.names=TRUE)
               data <- processSoundtrapLogs(logFiles)
               data <- distinct(rename(data, 'UTC' = 'fileTime')[c('UTC', 'temp')])
               names(data) <- c('Datetime_UTC', 'Internal_temp_C')
               data$Datetime_UTC <- format(data$Datetime_UTC, format='%m-%d-%Y_%H:%M:%S')
               stTempFile <- file.path(thisTempDir, paste0(thisName, '_ST_Internal_temp.csv'))
               write.csv(data, file=stTempFile, row.names=FALSE)
               filtTempFile <- file.path(thisTempDir, paste0(thisName, '_Filtered_ST_Temp_data.csv'))
               data <- filterTempData(data, 
                                      dateCol='Datetime_UTC',
                                      format='%m-%d-%Y',
                                      start=thirdDay, end=secondLastDay)
               write.csv(data, file=filtTempFile, row.names=FALSE)
           },
           'vemco' = {
               if(is.null(dir)) {
                   dir <- file.path(x$teleBaseDir, x$teleDir)
               }
               data <- readVemcoFolder(dir, vdat_exe=vdat)
               if(is.null(data)) {
                   stop('no vemco data')
               }
               vemId <- data$id[1]
               data$id <- NULL
               vemName <- gsub(x$deviceId, vemId, thisName)
               vemTempFile <- file.path(thisTempDir, paste0(vemName, '_Filtered_VEMCO_Temp_data.csv'))
               data <- distinct(data)
               data <- filterTempData(data, dateCol='Time_UTC', 
                                      start=thirdDay, end=secondLastDay)
               data$Time_UTC <- format(data$Time_UTC, format='%Y-%m-%d %H:%M:%S')
               write.csv(data, file=vemTempFile, row.names=FALSE)
           }
    )
    data
}
functionFile <- 'Z:/CODE_LIBRARY/R/QAQC/qaqcFunctions.R'
source(functionFile)
library(tidyr)
library(targets)
tar_load(db)
tar_load(combined_data)

newDep <- mutate(combined_data$deployments,
                 deployment_datetime=ymd_hms(deployment_datetime),
                 recovery_datetime=ymd_hms(recovery_datetime)
)
deps <- bind_rows(db$deployments, 
                  newDep)
fakeLog <- select(deps, 
                  projectName = deployment_code,
                  deviceName = deployment_device_codes,
                  deploymentDate = deployment_datetime,
                  recoveryDate = recovery_datetime,
                  usableStart = deployment_datetime,
                  usableEnd = recovery_datetime) %>% 
    mutate(deviceName = strsplit(deviceName, ',')) %>% 
    tidyr::unnest(deviceName) %>% 
    dplyr::filter(grepl('SOUNDTRAP', deviceName)) %>% 
    distinct() %>% 
    mutate(deviceId = gsub('SOUNDTRAP-', '', deviceName),
           sensitivity = as.character(-170),
           calibration=NA,
           projectBaseDir=NA,
           projectDir=NA,
           qaqcStatus='NoQAQC',
           qaqcBaseDir=NA,
           qaqcDir=NA,
           tempBaseDir=NA,
           tempDir=NA,
           teleBaseDir=NA,
           teleDir=NA,
           deploymentDate=psxTo8601(deploymentDate),
           recoveryDate=psxTo8601(recoveryDate),
           usableStart=psxTo8601(usableStart),
           usableEnd=psxTo8601(usableEnd)
    )
fakeLog <- addNefscDirs(
    fakeLog, 
    recBase = c('Y:/BOTTOM_MOUNTED/', 
                'Y:/drifting_recorder/'),
    qaqcBase = 'Z:/POST-DEPLOYMENT_METADATA/BOTTOM_MOUNTED/',
    tempBase = c('Z:/ANCILLARY_DATA/TEMPERATURE_DATA/BOTTOM_MOUNTED/',
                 'Z:/ANCILLARY_DATA/TEMPERATURE_DATA/DRIFTING_RECORDER/'),
    teleBase = c('Z:/ANCILLARY_DATA/TELEMETRY_DATA/BOTTOM_MOUNTED/'))
fakeLog <- addNefscDirs(
    fakeLog, 
    teleBase = c('Z:/ANCILLARY_DATA/TELEMETRY_DATA/BOTTOM_MOUNTED/NEFSC_MID-ATL/')
)

vp <- file.path('Z:/CODE_LIBRARY/R/QAQC','vdat.exe')
hm <- rerunOneTemperature(fakeLog, 'TNC_VA_202502_CVOW25', type='vemco',
                          vdat=vp)
str(hm)
wtf <- suppressWarnings(system2(path.expand(vp), args = "--version", stdout = FALSE, stderr = TRUE))
