get_geoserver_info <- function() {
rc_geo<-"https://geoserver.apps.aims.gov.au/reefcloud/ows"
rc_client <- WFSClient$new(rc_geo, 
                               serviceVersion = "2.0.0")
rc_client$getFeatureTypes(pretty = TRUE)
rc_lyrs<-rc_client$getFeatureTypes() %>%
      map_chr(function(x){x$getName()})
url <- parse_url(rc_geo)
geo_info <- list(rc_lyrs = rc_lyrs, url = url)
assign("geo_info", geo_info, env =  .GlobalEnv)
}

get_geoserver_data <- function(cov_name = NULL, sites){
wch <- str_which(geo_info$rc_lyrs, cov_name)

# create a bounding box for a spatial query.
bbox <- st_bbox(st_buffer(sites, dist = 0.1))  %>% 
      as.character()%>%
      paste(.,collapse = ',')

url <- geo_info$url
url$query <- list(service = "WFS",
      version = "1.0.0",
      request = "GetFeature",
      typename = geo_info$rc_lyrs[wch], 
      bbox = bbox,
      srs="EPSG%3A4326",
      styles='',
      format="application/openlayers")

request <- build_url(url)
# if not working use temp_file instead of request in L88.
#temp_file <- tempfile()
cov_data <- read_sf(request) %>% #temp_file
        st_set_crs(4326) 
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
