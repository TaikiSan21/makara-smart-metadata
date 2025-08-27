#
library(renv)
# run this once only to install packages
renv::restore()


# run this once - should store
googledrive::drive_auth()
library(targets)

# This runs the workflow
targets::tar_make()

# Look at outputs/warnings.csv


# options you can change are at top of _targets.R file