get_geoserver_data <- function(cov_name = NULL, sites){
rc_url <-"https://geoserver.apps.aims.gov.au/reefcloud/ows"

rc_client <- WFSClient$new(
  url = rc_url,
  serviceVersion = "1.0.0",
  logger = "INFO",
  headers = c("User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
)

bbox <- st_bbox(st_buffer(sites, dist = 0.1))  %>% 
      as.character()%>%
      paste(.,collapse = ',')

invisible(
  capture.output({
    cov_data <-  rc_client$getFeatures(cov_name, srsName = "EPSG:4326", bbox= bbox)
  })
)

return(cov_data)
}

add_cov_to_data <- function(data, cov, cov_name) {
data %>%
st_drop_geometry() %>%
      group_by(tier_id, name, year, date) %>% 
      summarise(
          !!paste0("max_", cov_name) := max(!!sym(paste0("max_", cov_name)), na.rm = TRUE),
          end_date = max(end_date, na.rm = TRUE),
         .groups = "drop"
  ) %>%
      ungroup() %>%
    ## replace NA's for severity_* and max_*
    mutate(across(paste0("max_", cov_name),
                  ~ replace_na(.x, replace = 0))) %>% 
    ## arrange according to NAME, fYEAR
    group_by(name) %>% 
    arrange(name,date) %>%     ## determine whether end_date is after date, then use previous row
    rowwise() %>% 
    mutate(across(paste0("max_", cov_name),
                  list(~adjust_cov_for_after_surveys(date, end_date, cov, tier_id, cur_column())),
                  .names = "{.col}")) %>%
    ## add lags
    mutate(across(paste0("max_", cov_name),
                  list(lag1 =  ~ get_lag_cov(year-1, cov, tier_id, cur_column())),
                  .names = "{.col}.{.fn}")) %>% 
    mutate(across(paste0("max_", cov_name),
                  list(lag2 =  ~ get_lag_cov(year-2, cov, tier_id, cur_column())),
                  .names = "{.col}.{.fn}")) %>%
    dplyr::select(-end_date) %>%
    ungroup()
}   

adjust_cov_for_after_surveys <- function(dt, end_date, cov, tier_id, colname) {
  if (is.na(end_date)) return(0)
  yr <- year(dt)
  if (end_date > dt) yr <- yr - 1
  return(get_lag_cov(yr, cov, tier_id, colname))
}

lag_covariates <- function(cov, year_range, full_cov_lookup, colname) {
  cov %>%
    filter(year >= year_range[1] & year <= year_range[2]) %>% 
    dplyr::select(-end_date) %>% 
    full_join(full_cov_lookup) %>%
    arrange(tier_id, year) %>%
    mutate(across(paste0("max_", cov_name),
                  ~ replace_na(colname, replace = 0))) %>% 
    group_by(tier_id) %>%
    mutate(across(paste0("max_", cov_name),
                  list(lag1 = ~ lag(colname) ))) %>% 
    mutate(across(paste0("max_", cov_name),
                  list(lag2 = ~ lag(colname, n = 2) ))) %>%
    ungroup() 
}

get_lag_cov <- function(yr, cov, tier_id, colname) {
  if (!colname %in% names(cov)) return(0)  # safety check
  val <- cov %>% filter(tier_id == !!tier_id, year == !!yr) %>% pull(!!sym(colname))
  return(ifelse(length(val) == 0, 0, max(val)))
}
