get_geoserver_info <- function() {
  # Get Geoserver info
  rc_url <-"https://geoserver.apps.aims.gov.au/reefcloud/ows"

  # Configure GDAL HTTP options to pass browser-like headers
  # This ensures headers are used for all underlying HTTP requests made by GDAL/OGR
  header_file <- tempfile(fileext = ".txt")
  writeLines(c(
    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type: application/xml",
    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language: en-US,en;q=0.9",
    "Accept-Encoding: gzip, deflate, br",
    "Connection: keep-alive"
  ), header_file)

  # Set GDAL environment variables for HTTP requests
  Sys.setenv(GDAL_HTTP_HEADER_FILE = header_file)
  Sys.setenv(GDAL_HTTP_USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
  Sys.setenv(GDAL_HTTP_UNSAFESSL = "YES")  # Allow HTTPS connections

  rc_client <- ows4R::WFSClient$new(
   url = rc_url,
   serviceVersion = "1.0.0",
   logger = "INFO",
   headers = c(
     "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
     "Content-Type" = "application/xml",
     "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
     "Accept-Language" = "en-US,en;q=0.9",
     "Accept-Encoding" = "gzip, deflate, br",
     "Connection" = "keep-alive",
     "Upgrade-Insecure-Requests" = "1",
     "Sec-Fetch-Dest" = "document",
     "Sec-Fetch-Mode" = "navigate",
     "Sec-Fetch-Site" = "none",
     "Sec-Fetch-User" = "?1"
   )
  )
  assign("rc_client", rc_client, env =  .GlobalEnv)
  invisible(NULL)
}

# Variables to change if needed - depending of the size of bbox and number of replicated years
GEOSERVER_DEFAULT_TIMEOUT <- 60 # in seconds
GEOSERVER_DEFAULT_CHUNK_SIZE <- 10000 # initial chunk size
GEOSERVER_BACKOFF_BASE <- 2 # exponential backoff
GEOSERVER_MAX_RETRIES <- 3
DEBUG_MODE <- FALSE

get_geoserver_data <- function(cov_name = NULL,
                                sites,
                                rc_client,
                                timeout_seconds = GEOSERVER_DEFAULT_TIMEOUT,
                                initial_chunk_size = GEOSERVER_DEFAULT_CHUNK_SIZE) {

bbox <- sf::st_bbox(st_buffer(sites, dist = 0.1))  %>% 
      as.character()%>%
      paste(.,collapse = ',')

  # Adaptive chunking variables (Suggestion 6)
  chunk_size <- initial_chunk_size
  start_index <- 0
  all_features <- list()
  fetch_more <- TRUE
  timeout_count <- 0
  max_timeouts <- 3
  min_chunk_size <- 1000

  # Safety valve: Maximum chunks to prevent infinite loops
  max_chunks <- 200

  # Duplicate detection: Track last chunk hash to detect pagination failures
  last_chunk_hash <- NULL

  # Progress tracking 
  total_fetched <- 0
  chunk_number <- 0

  if (!DEBUG_MODE) {
    cli::cli_alert_info(paste0("Downloading ", cov_name, " (chunk size: ", chunk_size, ")"))
  }

  while(fetch_more && chunk_number < max_chunks) {
    chunk_number <- chunk_number + 1
    retry_count <- 0
    chunk_success <- FALSE

    # Retry loop with exponential backoff (Suggestion 8)
    while (retry_count <= GEOSERVER_MAX_RETRIES && !chunk_success) {
      tryCatch({
        # Set timeout for this request (Suggestion 7)
        chunk_data <- R.utils::withTimeout({
          # Suppress verbose output by capturing it and discarding
          invisible(capture.output({
            chunk_data <- rc_client$getFeatures(
              cov_name,
              srsName = "EPSG:4326",
              bbox = bbox,
              count = chunk_size,
              startIndex = start_index
            )
          }))
          # Return the actual data (assigned inside capture.output)
          chunk_data
        }, timeout = timeout_seconds, onTimeout = "error")

        # Success - process the chunk
        # Defensive check: ensure chunk_data is valid and has rows
        chunk_valid <- !is.null(chunk_data) &&
                      is.data.frame(chunk_data) &&
                      !is.na(nrow(chunk_data)) &&
                      nrow(chunk_data) > 0

        if (isTRUE(chunk_valid)) {
          # Duplicate detection: Check if we're getting the same data repeatedly
          # For sf objects, use attribute columns only (not geometry)
          # Create fingerprint: nrow + first/last row attribute values

          # Initialize to avoid "missing value where TRUE/FALSE needed" error
          current_chunk_hash <- NULL

          tryCatch({
            # Get non-geometry columns for sf objects
            if (inherits(chunk_data, "sf")) {
              attr_data <- sf::st_drop_geometry(chunk_data)
              attr_cols <- names(attr_data)[1:min(3, ncol(attr_data))]
            } else {
              attr_data <- chunk_data
              attr_cols <- names(chunk_data)[1:min(3, ncol(chunk_data))]
            }

            current_chunk_hash <- paste0(
              nrow(chunk_data), "_",
              paste(as.character(attr_data[1, attr_cols]), collapse="_"), "_",
              paste(as.character(attr_data[nrow(attr_data), attr_cols]), collapse="_")
            )
          }, error = function(e) {
            # Fallback: use simple row count hash
            current_chunk_hash <<- paste0("fallback_", nrow(chunk_data))
          })

          if (!is.null(last_chunk_hash) && !is.null(current_chunk_hash) && current_chunk_hash == last_chunk_hash) {
            # Same data returned twice - pagination not working
            if (!DEBUG_MODE) {
              cli::cli_alert_warning(paste0(
                "Duplicate data detected at chunk ", chunk_number, ". ",
                "Geoserver pagination may not be working. Stopping download."
              ))
            }
            fetch_more <- FALSE
            chunk_success <- TRUE
          } else {
            # New unique data
            all_features[[length(all_features) + 1]] <- chunk_data
            total_fetched <- total_fetched + nrow(chunk_data)
            last_chunk_hash <- current_chunk_hash

            # Progress indication
            if (!DEBUG_MODE) {
              cli::cli_alert_success(paste0(
                "Chunk ", chunk_number, ": ", nrow(chunk_data), " features ",
                "(total: ", total_fetched, ")"
              ))
            }

            # Reset timeout counter on success
            timeout_count <- 0

            # Check if we're done
            n_rows <- nrow(chunk_data)
            if (!is.na(n_rows) && n_rows < chunk_size) {
              fetch_more <- FALSE
            } else {
              start_index <- start_index + chunk_size
            }
            chunk_success <- TRUE
          }

        } else {
          # No more data or invalid data
          if (!DEBUG_MODE) {
            if (is.null(chunk_data)) {
              cli::cli_alert_warning("Chunk returned NULL")
            } else if (!is.data.frame(chunk_data)) {
              # Print actual content to see error message
              char_preview <- if (is.character(chunk_data)) {
                substr(paste(chunk_data, collapse=" "), 1, 500)
              } else {
                "Not character type"
              }
              cli::cli_alert_warning(paste0(
                "Chunk is not a data.frame, got: ", class(chunk_data)[1]
              ))
              cli::cli_alert_info(paste0("Response content (first 500 chars): ", char_preview))
            } else if (is.na(nrow(chunk_data))) {
              cli::cli_alert_warning("Chunk nrow is NA")
            } else {
              cli::cli_alert_info(paste0("Chunk has ", nrow(chunk_data), " rows (stopping)"))
            }
          }
          fetch_more <- FALSE
          chunk_success <- TRUE
        }

      }, TimeoutException = function(e) {
        # Timeout occurred 
        retry_count <- retry_count + 1
        timeout_count <- timeout_count + 1

        if (retry_count <= GEOSERVER_MAX_RETRIES) {
          # Exponential backoff 
          wait_time <- GEOSERVER_BACKOFF_BASE ^ retry_count

          if (!DEBUG_MODE) {
            cli::cli_alert_warning(paste0(
              "Timeout on chunk ", chunk_number, " (attempt ", retry_count, "/", GEOSERVER_MAX_RETRIES, "). ",
              "Waiting ", wait_time, "s before retry..."
            ))
          }

          Sys.sleep(wait_time)

          # Adaptive chunk sizing: reduce chunk size after timeouts 
          if (timeout_count >= 2 && chunk_size > min_chunk_size) {
            old_chunk_size <- chunk_size
            chunk_size <- max(min_chunk_size, as.integer(chunk_size / 2))

            if (!DEBUG_MODE) {
              cli::cli_alert_info(paste0(
                "Reducing chunk size: ", old_chunk_size, " -> ", chunk_size
              ))
            }
          }
        } else {
          # Max retries exceeded
          if (!DEBUG_MODE) {
            cli::cli_alert_danger(paste0(
              "Failed after ", GEOSERVER_MAX_RETRIES, " retries on chunk ", chunk_number
            ))
          }
          fetch_more <- FALSE
          chunk_success <- TRUE  # Exit retry loop
        }

      }, error = function(e) {
        # Other errors
        retry_count <- retry_count + 1

        if (retry_count <= GEOSERVER_MAX_RETRIES) {
          # Exponential backoff
          wait_time <- GEOSERVER_BACKOFF_BASE ^ retry_count

          if (!DEBUG_MODE) {
            cli::cli_alert_warning(paste0(
              "Error on chunk ", chunk_number, " (attempt ", retry_count, "/", GEOSERVER_MAX_RETRIES, "): ",
              conditionMessage(e), ". Retrying in ", wait_time, "s..."
            ))
          }

          Sys.sleep(wait_time)
        } else {
          # Fallback: try fetching all at once if we haven't fetched anything yet
          if (start_index == 0 && length(all_features) == 0) {
            if (!DEBUG_MODE) {
              cli::cli_alert_warning("Attempting to fetch all features at once...")
            }

            tryCatch({
              # Suppress verbose output
              invisible(capture.output({
                chunk_data <- rc_client$getFeatures(cov_name, srsName = "EPSG:4326", bbox= bbox)
              }))
              if (!is.null(chunk_data)) {
                all_features[[1]] <- chunk_data
                total_fetched <- nrow(chunk_data)

                if (!DEBUG_MODE) {
                  cli::cli_alert_success(paste0("Fetched all ", total_fetched, " features"))
                }
              }
            }, error = function(e2) {
              if (!DEBUG_MODE) {
                cli::cli_alert_danger(paste0("Fallback also failed: ", conditionMessage(e2)))
              }
            })
          }

          fetch_more <- FALSE
          chunk_success <- TRUE
        }
      })
    }
  }

  # Check if maximum chunks limit was reached (safety valve)
  if (chunk_number >= max_chunks) {
    if (!DEBUG_MODE) {
      cli::cli_alert_warning(paste0(
        "Maximum chunk limit reached (", max_chunks, " chunks). ",
        "This may indicate a pagination issue with the geoserver. ",
        "Downloaded ", total_fetched, " features so far."
      ))
    }
  }

  # Combine all chunks efficiently (Suggestion 10)
  if (length(all_features) > 0) {
    if (!DEBUG_MODE) {
      cli::cli_alert_info(paste0("Combining ", length(all_features), " chunks..."))
    }

    # Use data.table::rbindlist if available for better performance
    if (requireNamespace("data.table", quietly = TRUE)) {
      cov_data <- data.table::rbindlist(all_features, use.names = TRUE, fill = TRUE)
      cov_data <- sf::st_as_sf(cov_data)
    } else {
      cov_data <- do.call(rbind, all_features)
    }

    if (!DEBUG_MODE) {
      cli::cli_alert_success(paste0(
        "Successfully downloaded ", nrow(cov_data), " features for ", cov_name
      ))
    }
  } else {
    cov_data <- NULL
    if (!DEBUG_MODE) {
      cli::cli_alert_warning(paste0("No data retrieved for ", cov_name))
    }
  }

  # Return the data
  return(cov_data)
}

add_cov_to_data <- function(data, cov, cov_name) {
data %>%
sf::st_drop_geometry() %>%
      dplyr::group_by(tier_id, name, year, date) %>% 
      dplyr::summarise(
          !!paste0("max_", cov_name) := max(!!sym(paste0("max_", cov_name)), na.rm = TRUE),
          end_date = max(end_date, na.rm = TRUE),
         .groups = "drop"
  ) %>%
      dplyr::ungroup() %>%
    ## replace NA's for severity_* and max_*
    dplyr::mutate(across(paste0("max_", cov_name),
                  ~ replace_na(.x, replace = 0))) %>% 
    ## arrange according to NAME, fYEAR
    dplyr::group_by(name) %>% 
    dplyr::arrange(name,date) %>%     ## determine whether end_date is after date, then use previous row
    dplyr::rowwise() %>% 
    dplyr::mutate(across(paste0("max_", cov_name),
                  list(~adjust_cov_for_after_surveys(date, end_date, cov, tier_id, cur_column())),
                  .names = "{.col}")) %>%
    ## add lags
    dplyr::mutate(across(paste0("max_", cov_name),
                  list(lag1 =  ~ get_lag_cov(year-1, cov, tier_id, cur_column())),
                  .names = "{.col}.{.fn}")) %>% 
    dplyr::mutate(across(paste0("max_", cov_name),
                  list(lag2 =  ~ get_lag_cov(year-2, cov, tier_id, cur_column())),
                  .names = "{.col}.{.fn}")) %>%
    dplyr::select(-end_date) %>%
    dplyr::ungroup()
}   

adjust_cov_for_after_surveys <- function(dt, end_date, cov, tier_id, colname) {
  if (is.na(end_date)) return(0)
  yr <- lubridate::year(dt)
  if (end_date > dt) yr <- yr - 1
  return(get_lag_cov(yr, cov, tier_id, colname))
}

lag_covariates <- function(cov, year_range, full_cov_lookup, colname) {
  cov %>%
    dplyr::filter(year >= year_range[1] & year <= year_range[2]) %>% 
    dplyr::select(-end_date) %>% 
    dplyr::full_join(full_cov_lookup) %>%
    dplyr::arrange(tier_id, year) %>%
    dplyr::mutate(across(paste0("max_", cov_name),
                  ~ replace_na(colname, replace = 0))) %>% 
    dplyr::group_by(tier_id) %>%
    dplyr::mutate(across(paste0("max_", cov_name),
                  list(lag1 = ~ lag(colname) ))) %>% 
    dplyr::mutate(across(paste0("max_", cov_name),
                  list(lag2 = ~ lag(colname, n = 2) ))) %>%
    dplyr::ungroup() 
}

get_lag_cov <- function(yr, cov, tier_id, colname) {
  if (!colname %in% names(cov)) return(0)  # safety check
  val <- cov %>% dplyr::filter(tier_id == !!tier_id, year == !!yr) %>% dplyr::pull(!!sym(colname))
  return(ifelse(length(val) == 0, 0, max(val)))
}
