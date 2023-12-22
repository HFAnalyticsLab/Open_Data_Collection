# Capacity data sources ---------------------------------------------------

## Hospital beds
# Overnight
url <- "https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/bed-data-overnight/"

excel_urls <- obtain_links(url)

excel_urls <- excel_urls[grepl("xls$|xlsx$", excel_urls, ignore.case = TRUE)]

# download files
files <- purrr::map_chr(
  excel_urls,
  ~ download_url_to_directory(
    url = .x,
    new_directory = "Bed Availability and Occupancy Data – Overnight"
  )
)

# clean filenames and remove unnecessary files
purrr::walk(
  files,
  rename_hospital_beds_xls
)

# tidy xlsx spreadsheets
# this function ignores the xls files
quarterly_overnight_beds <- list.files(
  "data-raw/Bed Availability and Occupancy Data – Overnight/",
  full.names = TRUE
) %>% 
  purrr::map_dfr(
    ~ reformat_bed_availability_data(
      filepath = .x,
      bed_type = "overnight"
    )
  )

orgs <- quarterly_overnight_beds %>% 
  pull(org) %>% 
  unique()

org_lkp <- attach_icb_to_org(orgs) %>% 
  mutate(
    divisor = n(),
    .by = health_org_code
  )

quarterly_overnight_beds <- quarterly_overnight_beds %>% 
  left_join(
    org_lkp,
    by = join_by(
      org == health_org_code
    ),
    relationship = "many-to-many"
  ) %>% 
  summarise(
    across(
      c(numerator, denominator),
      ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
    ),
    .by = c(year, quarter, icb_code, metric, frequency)
  ) %>% 
  rename(
    org = icb_code
  ) %>% 
  mutate(overnight_occupancy_rate = numerator / denominator)

quarterly_overnight_beds <- quarterly_overnight_beds %>% 
  filter(metric=='Bed availability - overnight - General & Acute') %>% 
  rename(overnight_bed_occupancy = numerator, overnight_bed_capacity = denominator) %>% 
  select(-metric,-frequency)

# Day only
url <- "https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/bed-data-day-only/"
excel_urls <- obtain_links(url)
excel_urls <- excel_urls[grepl("xls$|xlsx$", excel_urls, ignore.case = TRUE)]

# download files
files <- purrr::map_chr(
  excel_urls,
  ~ download_url_to_directory(
    url = .x,
    new_directory = "Bed Availability and Occupancy Data – Day"
  )
)


# clean and move file names
purrr::walk(
  files,
  rename_hospital_beds_xls
)

# tidy xlsx spreadsheets
# this function ignores the xls files
quarterly_day_beds <- list.files(
  "data-raw/Bed Availability and Occupancy Data – Day/",
  full.names = TRUE
) %>% 
  purrr::map_dfr(
    ~ reformat_bed_availability_data(
      filepath = .x,
      bed_type = "day"
    )
  )

orgs <- quarterly_day_beds %>% 
  pull(org) %>% 
  unique()

org_lkp <- attach_icb_to_org(orgs) %>% 
  mutate(
    divisor = n(),
    .by = health_org_code
  )

quarterly_day_beds <- quarterly_day_beds %>% 
  left_join(
    org_lkp,
    by = join_by(
      org == health_org_code
    ),
    relationship = "many-to-many"
  ) %>% 
  summarise(
    across(
      c(numerator, denominator),
      ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
    ),
    .by = c(year, quarter, icb_code, metric, frequency)
  ) %>% 
  rename(
    org = icb_code
  ) %>% 
  mutate(day_occupancy_rate = numerator / denominator)

quarterly_day_beds <- quarterly_day_beds %>% 
  filter(metric=='Bed availability - day - General & Acute') %>% 
  rename(day_bed_occupancy = numerator, day_bed_capacity = denominator) %>% 
  select(-metric,-frequency)




bed_output <- left_join(quarterly_overnight_beds,quarterly_day_beds, by =c('year', 'quarter', 'org'))

##################################################
##################################################

# sickness absence
# April 2009 to March 2022 file
url <- "https://files.digital.nhs.uk/A6/67E0C1/ESR_ABSENCE_CSV_NHSE.csv"

monthly_sickness_absence <- read.csv(url) %>% 
  mutate(
    year = as.integer(
      substr(
        Date, 1, 4
      )
    ),
    month = match(
      tolower(
        gsub(".*-", "", Date)
      ),
      tolower(month.abb)
    ),
    metric = paste0(
      "ESR absence (",
      tolower(Org.Type),
      " organisation type)"
    ),
    frequency = "monthly"
  ) %>% 
  select(
    "year",
    "month",
    "metric",
    org = "Org.Code",
    org_name = "Org.Name",
    numerator = "FTE.Days.Sick",
    denominator = "FTE.Days.Available",
    value = "SA.Rate....",
    "frequency"
  )

# monthly files April 2022 onwards
url <- "https://digital.nhs.uk/data-and-information/publications/statistical/nhs-sickness-absence-rates"

links <- obtain_links(url) %>% 
  (function(x) x[grepl(tolower(paste(month.name, collapse = "|")), x)]) %>% 
  (function(x) x[!grepl(tolower(paste(2014:2021, collapse = "|")), x)]) %>% 
  (function(x) x[!grepl(tolower(paste(c("january", "february"), 2022, 
                                      sep = "-", 
                                      collapse = "|")), x)])

csv_links <- purrr::map(
  .x = paste0(
    "https://digital.nhs.uk/",
    links
  ),
  .f = obtain_links
) %>% 
  unlist() %>% 
  (function(x) x[grepl("csv$", x)]) %>% 
  (function(x) x[!grepl("benchmarking|reason|COVID|ESR_ABSENCE_CSV_NHSE", x, ignore.case = TRUE)])

monthly_sickness_monthly_files <- purrr::map_dfr(
  csv_links,
  read.csv
) %>% 
  mutate(
    year = as.integer(
      substr(
        DATE, nchar(DATE) - 3, nchar(DATE)
      )
    ),
    month = as.integer(
      substr(
        DATE, nchar(DATE) - 6, nchar(DATE) - 5
      )
    ),
    metric = paste0(
      "ESR absence (",
      tolower(ORG_TYPE),
      " organisation type)"
    ),
    frequency = "monthly"
  ) %>% 
  select(
    "year",
    "month",
    "metric",
    org = "ORG_CODE",
    org_name = "ORG_NAME",
    numerator = "FTE_DAYS_LOST",
    denominator = "FTE_DAYS_AVAILABLE",
    value = "SICKNESS_ABSENCE_RATE_PERCENT",
    "frequency"
  )

monthly_sickness_absence <- bind_rows(
  monthly_sickness_absence, 
  monthly_sickness_monthly_files
)

org_icb_lkp <- monthly_sickness_absence %>% 
  distinct(org) %>% 
  mutate(
    role = map_chr(
      .x = org,
      .f = health_org_role
    )
  ) %>% 
  filter(
    role %in% c(
      # "RO98" CCG 200
      "RO107", # care trust 11
      # "RO108" # care trust sites 0
      # "RO116" Executive agency programme 3
      "RO157", # Independent providers 4
      # "RO162" Executive agency programme 3
      "RO172", # Independent Sector Healthcare providers 32
      # "RO189" Special Health Authorities 13
      "RO197" # NHS trust 259
      # "RO198", # NHS trust sites 0
      # "RO213" Commissioning Support Units 32
      # "RO261" Executive Agency Programme 42
    )
  ) %>% 
  pull(org) %>% 
  attach_icb_to_org() %>% 
  mutate(
    divisor = n(),
    .by = health_org_code
  )

monthly_sickness_absence <- monthly_sickness_absence %>% 
  inner_join(
    org_icb_lkp,
    by = join_by(
      org == health_org_code
    ),
    relationship = "many-to-many"
  ) %>% 
  summarise(
    across(
      c(numerator, denominator),
      ~ sum(.x / divisor, na.rm = TRUE)
    ),
    .by = c(
      year, month, metric, icb_code, frequency
    )
  ) %>% 
  mutate(
    value = numerator / denominator
  ) %>% 
  rename(
    org = icb_code
  )

quarterly_sickness_absence <- monthly_to_quarterly_mean(
  monthly_sickness_absence
)

##################################################

# NHS workforce per population
# collecting the numerator
url <- "https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics"

links <- obtain_links(url) %>% 
  (function(x) x[grepl(paste(month.abb, collapse = "|"), x, ignore.case = TRUE)]) %>% 
  (function(x) x[grepl("[0-9]{4}$", x)]) %>% 
  head(1) %>% 
  (function(x) paste0("https://digital.nhs.uk/", x)) %>% 
  obtain_links %>% 
  (function(x) x[grepl("zip$", x)]) %>% 
  (function(x) x[grepl("csv", x)])


file <- download_url_to_directory(
  url = links,
  new_directory = "Clinical workforce",
  filename = "Latest workforce statistics.zip"
)

annual_workforce_fte <- unzip_file(
  zip_filepath = file,
  filename_pattern = "ICS"
) %>% 
  filter(
    Data.Type == "FTE"
  ) %>% 
  mutate(
    Date = as.Date(Date),
    month = lubridate::month(Date),
    year = lubridate::year(Date),
    months_from_july = abs(7 - month)
  ) %>% 
  filter(
    months_from_july == min(months_from_july),
    .by = year
  ) %>%
  mutate(
    month = 7
  ) %>%
  select(
    "year",
    "month",
    org = "ICS.code",
    "Staff.Group",
    numerator = "Total"
  )

# collecting the denominator (populations by health areas)
url <- "https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice"

links <- obtain_links_and_text(url) %>% 
  (function(x) x[grepl("[a-z]{,9}-[0-9]{4}", basename(x))]) %>% 
  (function(x) rlang::set_names(paste0("https://digital.nhs.uk", x),
                                nm = names(x))) %>% 
  (function(x) x[grepl("january|april|july|october", basename(x), ignore.case = TRUE)]) %>% 
  purrr::lmap(
    ~ list(
      obtain_links_and_text(
        .x
      )
    )
  ) %>% 
  unlist() %>% 
  (function(x) rlang::set_names(x, nm = sub("^.*?([A-Z])", "\\1", names(x)))) %>% 
  (function(x) rlang::set_names(x, nm = gsub("[\n].*$", "", names(x)))) %>%
  (function(x) x[grepl("zip$|csv$", x)]) %>% 
  (function(x) x[!grepl("lsoa|males|tall", basename(x))])%>% 
  (function(x) x[grepl("ICB|CCG", names(x))]) %>% 
  # (\(x) x[grepl("all|ccg", x)])() %>% 
  tibble::enframe() %>% 
  mutate(
    mnth = stringr::str_extract(
      name,
      pattern = paste(c(month.name, month.abb), collapse = "|")
    ),
    mnth = substr(mnth, 1, 3),
    year = stringr::str_extract(
      name,
      pattern = "[0-9]{4}"
    )
  ) %>% 
  mutate(
    n = n(),
    .by = c(mnth, year)
  ) %>% 
  mutate(
    include = case_when(
      n == 1 ~ TRUE,
      .default = grepl("Single", name)
    )
  ) %>% 
  filter(
    include == TRUE
  ) %>% 
  mutate(
    name = paste(
      mnth,
      year,
      sep = "-"
    )
  ) %>% 
  select(c("name", "value")) %>% 
  tibble::deframe()

files <- purrr::lmap(
  links,
  ~ as.list(
    check_and_download(
      filepath = paste0(
        "data-raw/Health populations/", 
        names(.x),
        " ",
        basename(.x)),
      url = .x
    )
  )
)

health_pop_denominators <- purrr::map_df(
  files,
  summarise_health_pop_files
) %>% 
  filter(health_org_code != "UNKNOWN")


org_lkp <- unique(health_pop_denominators$health_org_code) %>% 
  attach_icb_to_org()

quarterly_health_pop_denominators <- health_pop_denominators %>% 
  left_join(
    org_lkp,
    by = join_by(
      health_org_code
    )
  ) %>% 
  summarise(
    denominator = sum(denominator),
    .by = c(
      icb_code, year, month
    )
  ) %>% 
  rename(
    org = "icb_code"
  )

workforce_metrics <- annual_workforce_fte %>% 
  complete(
    year, month, org, Staff.Group
  ) %>% 
  inner_join(
    quarterly_health_pop_denominators,
    by = join_by(
      org,
      year, 
      month
    )
  ) %>% 
  mutate(
    numerator = replace_na(numerator, 0),
    metric = paste0(
      "Workforce FTEs per 10,000 population (",
      Staff.Group,
      ")"
    ),
    value = numerator / (denominator / 1e4),
    frequency = "annual calendar"
  ) %>% 
  select(!c("Staff.Group", "month"))
