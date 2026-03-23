#' Read and Process Paradata Files
#'
#' This function reads all `paradata.tab` files from a specified directory,
#' optionally removes passive events, and parses the parameters into a readable format.
#'
#' @param path Character. The directory path containing the `paradata.tab` files.
#' @param remove.passive Logical. If `TRUE`, passive events will be removed from the data. Default is `TRUE`.
#' @param parse Logical. If `TRUE`, the `parameters` column will be split into separate columns for easier readability. Default is `TRUE`.
#' @return A data.table containing the combined and processed paradata from all `paradata.tab` files in the specified directory.
#' @details
#' This function performs the following steps:
#' \itemize{
#'   \item Lists all `paradata.tab` files in the specified directory.
#'   \item Reads each file into a data.table with specified column classes.
#'   \item Ensures all default columns are present in the data.
#'   \item Optionally removes rows with passive events.
#'   \item Optionally parses the `parameters` column into separate columns for better readability.
#' }
#' The passive events removed by default are: `VariableDisabled`, `VariableEnabled`, `VariableSet`, `QuestionDeclaredInvalid`, `QuestionDeclaredValid`.
#' @import data.table
#' @importFrom assertthat assert_that
#' @importFrom glue glue glue_collapse
#' @export
#' @examples
#' \dontrun{
#' paradata <- read_many_para(path = "path/to/directory")
#' }
read_many_para <- function(path = NULL, remove.passive = TRUE, parse = TRUE) {
  ## LIST ALL FILES
  files <- list.files(path = path, pattern = "paradata.tab", recursive = TRUE)

  # PASSIVE EVENTS - FOR NOW ALSO 'QuestionDeclaredInvalid'
  passive_events <- c(
    "VariableDisabled", "VariableEnabled", "VariableSet",
    "QuestionDeclaredInvalid", "QuestionDeclaredValid"
  )


  import_list <- lapply(file.path(path, files), function(x) {
    dt <- fread(file = x, encoding = "UTF-8", colClasses = list(
      character = c("event", "responsible", "tz_offset", "parameters"),
      integer = c("order", "role"),
      POSIXct = c("timestamp_utc")
    ))


    ## CHECK IF ALL DEFAULT SUSO COLUMNS ARE THERE
    col_names <- names(dt)
    col_suso <- c("interview__id", "order", "event", "responsible", "role", "timestamp_utc", "tz_offset", "parameters")
    assertthat::assert_that(
      all(col_suso %chin% col_names),
      msg = glue::glue(
        "Attention! There are columns missing\n",
        "Columns per paradata file format: ", glue::glue_collapse(col_suso, sep = ", "), "\n",
        "Columns in file: ", glue::glue_collapse(col_names, sep = ", ")
      )
    )

    # SET KEY
    setkeyv(dt, c("event"))

    ## IF WE SHALL REMOVE THE PASSIVE EVENTS
    if (remove.passive) dt <- dt[!.(passive_events)]

    # IF SHALL BE PARSED
    if (parse & nrow(dt) > 0) {
      ignore_events <- c(
        "ClosedBySupervisor", "Deleted", "InterviewCreated", "InterviewModeChanged",
        "InterviewerAssigned", "TranslationSwitched", "Completed", "KeyAssigned",
        "OpenedBySupervisor", "Paused", "ReceivedByInterviewer", "ReceivedBySupervisor",
        "Restarted", "Restored", "Resumed", "SupervisorAssigned"
      )

      ## SPLIT PARAMETERS IN READABLE FORMAT
      # only for those which actually have parameters - should result in performance increase

      # First cleanup SuSo Export issue if there is any three "|||" within
      # the parameters string
      dt[
        grepl("\\|\\|\\|", parameters),
        parameters := gsub("\\|\\|\\|", "\\|\\|", parameters)
      ]

      # Check if any Roster
      if (nrow(dt[!.(c(ignore_events))][!grepl("\\|\\|$", parameters)]) == 0) {
        dt[
          !.(c(ignore_events)),
          `:=`(
            c("variable", "value_entered"),
            tstrsplit(parameters, "\\|\\|")
          )
        ]
        dt[, "roster_variable" := NA_character_]
      } else if (nrow(dt[!.(c(ignore_events))][!grepl("\\|\\|$", parameters)]) > 0) {
        dt[
          !.(c(ignore_events)),
          `:=`(
            c("variable", "value_entered", "roster_variable"),
            tstrsplit(parameters, "\\|\\|")
          )
        ]
      }
    }

    dt
  })

  import_list <- Filter(function(x) nrow(x) > 0, import_list)
  return(rbindlist(import_list, fill = TRUE))
}




#' Analyze Paradata
#'
#' This function analyzes paradata from a data.table, identifying answer changes and computing event durations.
#'
#' @param dt A data.table object containing the paradata to be analyzed.
#' @param answ.changes Logical. If `TRUE`, identifies changes made to answers after the first entry. Default is `TRUE`.
#' @param duration Logical. If `TRUE`, computes the duration of events in seconds. Default is `TRUE`.
#' @return A data.table with analyzed paradata, including identified answer changes and computed durations.
#' @details
#' The function performs the following:
#' \itemize{
#'   \item Checks if required columns are present.
#'   \item Identifies changes to answers (if `answ.changes` is `TRUE`).
#'   \item Computes durations of events (if `duration` is `TRUE`).
#' }
#' @import data.table
#' @importFrom assertthat assert_that
#' @importFrom glue glue glue_collapse
#' @importFrom stringr str_detect
#' @export
#' @examples
#' \dontrun{
#' dt <- read_para("path/to/paradata")
#' analyzed_dt <- analyze_para(dt)
#' }
analyze_para <- function(dt = NULL, answ.changes = TRUE, duration = TRUE) {
  ## CHECK IF ALL SUSO COLUMNS NEEDEDF ARE THERE
  col_names <- names(dt)
  col_needed <- c("interview__id", "order", "event", "responsible", "roster_variable", "variable", "value_entered", "timestamp_utc", "tz_offset")

  assertthat::assert_that(
    all(col_needed %chin% col_names),
    msg = glue::glue(
      "Attention! There are columns missing. Did you run 'read_para' yet?\n",
      "Columns found in dt: ", glue::glue_collapse(col_needed, sep = ", "), "\n",
      "Columns in file: ", glue::glue_collapse(col_names, sep = ", ")
    )
  )


  ## CHANGES MADE TO ANSWERS
  if (answ.changes) {
    ## Identify questions for which answer was changed after first entry
    setkeyv(dt, c("interview__id", "order"))
    setorder(dt, interview__id, variable, roster_variable, order)

    dt[
      event %chin% "AnswerSet" & value_entered != shift(value_entered) &
        variable == shift(variable) &
        roster_variable == shift(roster_variable) &
        str_detect(value_entered, ",") == FALSE ## IGNORE MULTI SELECT
      & str_detect(value_entered, "\\|") == FALSE &
        interview__id == shift(interview__id),
      event := "AnswerChanged"
    ]

    ## Identify answer changed for multi-select questions
    ## Simply by vector of length for now
    ### FIRST CREATE HELP VARIABLE
    dt[event == "AnswerSet" &
      nchar(value_entered) > nchar(shift(value_entered, type = "lead")) &
      variable == shift(variable) &
      roster_variable == shift(roster_variable) &
      str_detect(value_entered, ",") == TRUE &
      str_detect(value_entered, "\\|") == FALSE &
      interview__id == shift(interview__id), help := "AnswerChanged"]

    dt[!is.na(shift(help)) &
      event == "AnswerSet" &
      variable == shift(variable) &
      roster_variable == shift(roster_variable) &
      str_detect(value_entered, ",") == TRUE &
      str_detect(value_entered, "\\|") == FALSE &
      interview__id == shift(interview__id), event := "AnswerChanged"][, help := NULL]
  }


  if (duration) {
    # COMPUTE TIME EVENT HAPPENED
    # TODO: Actually needed? If so: Doesn't work with negative UTC
    # dt[,time_event:=as.ITime(tz_offset)+timestamp_utc]

    # IGNORE PASSIVE EVENTS
    excluded_events <- c(
      "ApproveByHeadquarter",
      "ApproveBySupervisor",
      "ClosedBySupervisor",
      "KeyAssigned",
      "OpenedBySupervisor",
      "QuestionDeclaredInvalid",
      "QuestionDeclaredValid",
      "ReceivedByInterviewer",
      "ReceivedBySupervisor",
      "RejectedByHeadquarter",
      "RejectedBySupervisor",
      "SupervisorAssigned",
      "TranslationSwitched",
      "UnapproveByHeadquarters",
      "VariableDisabled",
      "VariableSet"
    )

    # Compute duration in seconds
    setkey(dt, "interview__id")
    setorder(dt, "interview__id", "order")
    # only for subset of rows
    dt[interview__id == shift(interview__id, type = "lead"), elapsed_seconds := as.numeric(difftime(timestamp_utc, shift(timestamp_utc), units = "secs")), by = "interview__id"] # [,time_event:=NULL]

    # clean up duration
    # TODO: Might want to make this more granular / cleaned up
    dt[elapsed_seconds < 0, elapsed_seconds := NA]
  }

  return(dt)
}
