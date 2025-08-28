# Install "renv" if you do not have it
if(!require('renv', quietly = TRUE)) {
    install.packages('renv')
}
# run this once only to install packages required - may take some time
# Selection option 2 when prompted
renv::restore()

# Should only have to run this once - we need Google credentials for download
googledrive::drive_auth()

# Make sure you have secrets.yml in your .secrets folder
# This runs the workflow!
targets::tar_make()

# Look at outputs/warnings.csv, fix issues then run until you are happy!


# options you can change are at top of _targets.R file