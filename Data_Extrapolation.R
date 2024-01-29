# Elective Recovery Project
# Data Extrapolation
# December 2023



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


########################################################################
# Sickness Absence  ---------------------------------------------------
########################################################################

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
) %>% 
  rename(fte_sick_days=numerator,fte_available_days=denominator,sickness_rate=value) %>% 
  filter(metric=='ESR absence (acute organisation type)') %>% 
  select(-frequency,-metric)

# annual_sickness_absence <- monthly_to_annual_mean(
#   monthly_sickness_absence,
#   year_type = "financial"
# )
# 
# sickness_absence <- bind_rows(
#   monthly_sickness_absence,
#   quarterly_sickness_absence,
#   annual_sickness_absence
# ) 

########################################################################
# Covid Occupancy  ---------------------------------------------------
########################################################################

occ_beds <- read_excel('/home/andrew.mooney@tier0/Covid_Occupancy.xlsx', sheet='Occupied_Beds_Quarter') %>% 
  select(-'NHS England Region',-Name)
covid_beds <- read_excel('/home/andrew.mooney@tier0/Covid_Occupancy.xlsx', sheet='Covid_Beds_Quarter') %>% 
  select(-'NHS England Region',-Name)


occ_beds <- occ_beds %>% 
  pivot_longer(cols=-Code, names_to = 'Quarter', values_to = 'occupied_beds')

covid_beds <- covid_beds %>% 
  pivot_longer(cols=-Code, names_to = 'Quarter', values_to = 'covid_beds')

covid_occupancy <- left_join(occ_beds,covid_beds,by=c('Quarter','Code')) 

covid_occupancy <- covid_occupancy %>%
  rename(org = Code) %>% 
  mutate(year=as.numeric(substr(Quarter,4,8)),
         quarter=as.numeric(substr(Quarter,2,2))) %>% 
  select(-Quarter)

orgs <- covid_occupancy %>% 
  pull(org) %>% 
  unique()

org_lkp <- attach_icb_to_org(orgs) %>% 
  mutate(
    divisor = n(),
    .by = health_org_code
  )

covid_occupancy <- covid_occupancy %>% 
  left_join(
    org_lkp,
    by = join_by(
      org == health_org_code
    ),
    relationship = "many-to-many"
  ) %>% 
  summarise(
    across(
      c(covid_beds, occupied_beds),
      ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
    ),
    .by = c(year, quarter, icb_code)
  ) %>% 
  rename(
    org = icb_code
  ) %>% 
  mutate(covid_occupancy_rate = covid_beds / occupied_beds)




########################################################################
# Referral to Treatment  --------------------------------------------
########################################################################

# referral to treatment
url <- "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/"
annual_urls <- obtain_links(url) %>% 
  (function(x) x[grepl("^https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/", x)]) %>% 
  (function(x) x[grep("[0-9]{4}-[0-9]{2}", x)]) %>% 
  unique() %>% 
  (function(x) x[!grepl("2011-12|2012-13", x)])

xl_files <- purrr::map(
  annual_urls,
  obtain_links
) %>% 
  unlist() %>% 
  (function(x) x[grepl("xls$|xlsx$", x)]) %>% 
  (function(x) x[grepl("Commissioner", x)]) %>% 
  (function(x) x[grepl("Admitted", x)])

files <- purrr::map_chr(
  xl_files,
  ~ download_url_to_directory(
    url = .x,
    new_directory = "Referral to Treatment"
  )
) %>% 
  purrr::map_chr(
    rename_rtt_files
  )

# replace unadjusted files with adjusted ones where they exist
for (file in files) {
  if (grepl("adjusted", file)) {
    unadjusted_file <- gsub("adjusted", "admitted", file)
    if (file.exists(unadjusted_file)) {
      file.remove(
        unadjusted_file
      )
      
      file.rename(
        from = file,
        to = unadjusted_file
      )
    }
    
  }
}

# obtain new list of files in the folder
files <- list.files(
  "data-raw/Referral to Treatment",
  full.names = TRUE
)


# tidy the data in the files
monthly_rtt <- files %>% 
  purrr::map_dfr(
    tidy_rtt
  )

org_icb_lkp <- monthly_rtt %>% 
  distinct(
    org
  ) %>% 
  filter(
    org != "-"
  ) %>% 
  pull() %>% 
  attach_icb_to_org()

monthly_rtt <- monthly_rtt %>% 
  inner_join(
    org_icb_lkp,
    by = join_by(
      org == health_org_code
    )
  ) %>% 
  summarise(
    across(
      c(numerator, denominator),
      ~ sum(.x, na.rm = TRUE)
    ),
    .by = c(
      year, month, icb_code, metric, frequency
    )
  ) %>% 
  rename(
    org = "icb_code"
  ) %>% 
  mutate(
    value = numerator / denominator
  )

quarterly_rtt <- monthly_to_quarterly_sum(monthly_rtt)


quarterly_rtt <- quarterly_rtt %>% 
  pivot_wider(names_from=metric, values_from=value) %>% 
  rename(admitted_18plus_weeks = 'Proportion of completed pathways greater than 18 weeks from referral (admitted)',
         non_admitted_18plus_weeks = 'Proportion of completed pathways greater than 18 weeks from referral (not admitted)') %>% 
  select(-frequency,-numerator,-denominator) %>% 
  group_by(year, quarter, org) %>% 
  summarise(admitted_18plus_weeks=max(admitted_18plus_weeks, na.rm=T),
            non_admitted_18plus_weeks=max(non_admitted_18plus_weeks, na.rm=T)) %>% 
  ungroup()



########################################################################
# Join Datasets ----------------------------------------------------
########################################################################


output <- left_join(bed_output,quarterly_sickness_absence,by=c('year','quarter','org'))
output <- left_join(output, covid_occupancy, by=c('year', 'quarter', 'org'))
output <- left_join(output, quarterly_rtt, by=c('year', 'quarter', 'org'))


##########
#Test correlations for 2022.

data_2022 <- output %>% 
  filter(year==2022)

test3 <- data_2022 %>% 
  select(overnight_occupancy_rate,day_occupancy_rate,sickness_rate,covid_occupancy_rate,admitted_18plus_weeks,non_admitted_18plus_weeks,
         workforce_per_1000_pop,doctors_per_1000_pop,nurses_per_1000_pop,ambulance_staff_per_1000_pop,
         technical_staff_per_1000_pop,senior_doctors_proportion_of_doctors,managers_proportion_of_total_staff)

library(corrplot)
correlation_matrix <- cor(test3)
corrplot(correlation_matrix,method='color', addCoef.col = 'black')

cor(test3$managers_proportion_of_total_staff,test3$sickness_rate)

ggplot(test3, aes(x = managers_proportion_of_total_staff, y = sickness_rate)) +
  geom_point(color = "blue")

########################################################################
# NHS Workforce per population  -------------------------------------
########################################################################

# monthly files April 2022 onwards
# workforce monthly files 
url <- "https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics"

links <- obtain_links(url)  %>%
  (function(x) x[grepl(tolower(paste(month.name, collapse = "|")), x)])

xlsx_links <- purrr::map(
  .x = paste0(
    "https://digital.nhs.uk/",
    links
  ),
  .f = obtain_links
) %>% 
  unlist() %>% 
  (function(x) x[grepl("xlsx$", x)]) %>% 
  (function(x) x[grepl("England%20and%20Organisation", x, ignore.case = TRUE)])

##

files <- purrr::map_chr(
  xlsx_links,
  ~ download_url_to_directory(
    url = .x,
    new_directory = "workforce"
  )
)


# Function to read Excel file with 'Total' in the header
read_excel_with_total_header <- function(file_path, max_rows_to_check = 50) {
  # Read in a few rows to find the header row
  preview <- read_excel(file_path, sheet = 4, n_max = max_rows_to_check)
  # Find the row number containing 'Total'
  total_col_index <- which(apply(preview, 2, function(x) any(grepl("Total", x, ignore.case = TRUE))))
  # Check if header_row is found
  if (length(total_col_index) == 0) {
    stop("Column with 'Total' not found in the first ", max_rows_to_check, " rows")
  } else if (total_col_index == 1) {
    stop("No column found before the 'Total' column.")
  }
  ##find month
  # Search for a cell containing a date in the format 'Month Year Monthly Data'
  # Initialize variables
  found_date <- NA
  found <- FALSE
  # Search for a cell containing a date in the format 'Month Year monthly data'
  month_year_pattern <- paste0("(?i)(", paste(month.name, collapse = "|"), ")\\s+\\d{4}\\s+monthly data")
  for (row in 1:nrow(preview)) {
    if (found) break  # Break the outer loop if date is found
    for (col in 1:ncol(preview)) {
      if (grepl(month_year_pattern, preview[row, col], perl = TRUE)) {
        found_date <- preview[row, col]
        found <- TRUE  # Set the flag to true and break the inner loop
        break
      }
    }
  }
  if (is.na(found_date)) {
    stop("No date in the format 'Month Year monthly data' found in the first ", max_rows_to_check, " rows")
  }
  # Extract just the Month and Year part
  found_date <- sub(" monthly data", "", found_date, ignore.case = TRUE)
  # Read the entire file with the specified header row
  data <- read_excel(file_path, sheet = 4, skip = header_row, range = cell_cols((total_col_index - 1):ncol(preview)))
  # Remove empty columns from the specified range
  data <- data[, colSums(is.na(data)) != nrow(data)]
  data$Date <- found_date
  return(data)
}
read_fte_figures_sheet <- function(file_path) {  read_excel_with_total_header(file_path)  }
#%>%    filter(!is.na(`Organisation code`)) }
combined_data <- purrr::map_df(files, read_fte_figures_sheet)
new_combined_data <- combined_data 
names(new_combined_data) <- as.character(unlist(new_combined_data[1,]))
new_combined_data <- new_combined_data[-1, ]
names(new_combined_data)[29] <- "date"
new_combined_data <- new_combined_data %>%
  filter(!is.na(`Organisation code`)) 

new_combined_data <- new_combined_data %>%
  mutate(month = month(as.Date(paste("01", date), format="%d %B %Y")),
         year = year(as.Date(paste("01", date), format="%d %B %Y")))


workforce_monthly_data <- new_combined_data %>%
  select(-date) %>%
  rename(org=`Organisation code`) %>% 
  select(year, month, org, everything()) %>% 
  filter(org!='Organisation code') %>% 
  mutate_at(vars("Total":"Other staff or those with unknown classification"), as.numeric)


workforce_quarterly_data <- monthly_to_quarterly_mean(
  workforce_monthly_data 
) %>% 
  rename(fte_sick_days=numerator,fte_available_days=denominator,sickness_rate=value) %>% 
  filter(metric=='ESR absence (acute organisation type)') %>% 
  select(-frequency,-metric)


orgs <- workforce_monthly_data %>% 
  pull(org) %>%
  unique()

org_lkp <- attach_icb_to_org(orgs) %>%
  mutate(
    divisor = n(),
    .by = health_org_code
  )
 
workforce_monthly_data_2 <- workforce_monthly_data %>%
   left_join(
     org_lkp,
     by = join_by(
       org == health_org_code
     ),
    relationship = "many-to-many"
   ) %>%
   summarise(
     across(
       c(`Total`, `Professionally qualified clinical staff`, `HCHS Doctors`, `Consultant`, `Associate Specialist`,`Specialty Doctor`, `Staff Grade`,
          `Specialty Registrar`, `Core Training`, `Foundation Doctor Year 2`, `Foundation Doctor Year 1`, `Hospital Practitioner / Clinical Assistant`,
          `Other and Local HCHS Doctor Grades`, `Nurses & health visitors`, `Midwives`, `Ambulance staff`, `Scientific, therapeutic & technical staff`,
          `Support to clinical staff`, `Support to doctors, nurses & midwives`, `Support to ambulance staff`, `Support to ST&T staff`,
          `NHS infrastructure support`, `Central functions`, `Hotel, property & estates`, `Senior managers`, `Managers`,`Other staff or those with unknown classification`),
       ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
     ),
     .by = c(year, month, icb_code)
   ) %>%
   rename(
     org = icb_code
   )

# %>% 
#   mutate(covid_occupancy_rate = covid_beds / occupied_beds)

workforce_monthly_data_2 <- workforce_monthly_data_2 %>%
  pivot_longer(cols = c("Total", "Professionally qualified clinical staff", "HCHS Doctors", "Consultant", "Associate Specialist","Specialty Doctor", "Staff Grade",
                        "Specialty Registrar", "Core Training", "Foundation Doctor Year 2", "Foundation Doctor Year 1", "Hospital Practitioner / Clinical Assistant",
                        "Other and Local HCHS Doctor Grades", "Nurses & health visitors", "Midwives", "Ambulance staff", "Scientific, therapeutic & technical staff",
                        "Support to clinical staff", "Support to doctors, nurses & midwives", "Support to ambulance staff", "Support to ST&T staff",
                        "NHS infrastructure support", "Central functions", "Hotel, property & estates", "Senior managers", "Managers","Other staff or those with unknown classification"),
               names_to = "metric",
               values_to = "numerator") %>%
  group_by(year, month, org, metric) %>%
  summarize(numerator = sum(numerator, na.rm = TRUE)) %>% 
  ungroup() %>% 
  mutate(denominator=100)


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

health_pop_denominators <- health_pop_denominators %>% 
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
  ) %>% 
  mutate(quarter=ifelse(month==1,4,
                        ifelse(month==4,2,
                        ifelse(month==7,3,
                        1)))) %>% 
  select(-month)

quarterly_workforce_data <- monthly_to_quarterly_mean(
  workforce_monthly_data_2 
) %>% 
  select(-denominator,-value) %>% 
  inner_join(
    health_pop_denominators,
    by = join_by(
      org,
      year, 
      quarter
    )
  )

quarterly_workforce_stats <- quarterly_workforce_data %>%
  pivot_wider(names_from = metric, values_from = numerator) %>% 
  rename(population=denominator) %>% 
  mutate(workforce_per_1000_pop=(`Total`/population)*1000,
         doctors_per_1000_pop=((`HCHS Doctors`)/population)*1000,
         nurses_per_1000_pop=((`Nurses & health visitors`)/population)*1000,
         ambulance_staff_per_1000_pop=((`Ambulance staff`)/population)*1000,
         technical_staff_per_1000_pop=((`Scientific, therapeutic & technical staff`)/population)*1000,
         senior_doctors_proportion_of_doctors=(`Consultant`+`Associate Specialist`+`Staff Grade`+`Specialty Doctor`)/`HCHS Doctors`,
         managers_proportion_of_total_staff=(`Managers`+`Senior managers`)/`Total`) %>% 
  select(year,quarter,org,workforce_per_1000_pop,doctors_per_1000_pop,nurses_per_1000_pop,ambulance_staff_per_1000_pop,
         technical_staff_per_1000_pop,senior_doctors_proportion_of_doctors,managers_proportion_of_total_staff)


output <- left_join(output, quarterly_workforce_stats, by=c('year', 'quarter', 'org'))





#RTT Data.

rtt_data_month <- s3read_using(fread,
                               object = 'RTT waiting times data/RTT_allmonths_new.csv', # File to open
                               bucket = 'thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp') # Bucket name defined above




rtt_dataset <- rtt_data_month %>% 
  filter(Treatment.Function.Name=='Total') %>% 
  mutate(weeks_0_18 = rowSums(select(., starts_with("Gt.00.to.01.Weeks.SUM.1"):starts_with("Gt.17.to.18.Weeks.SUM.1"))),
         weeks_18_52 = rowSums(select(., starts_with("Gt.18.to.19.Weeks.SUM.1"):starts_with("Gt.51.to.52.Weeks.SUM.1"))),
         weeks_52_104 = rowSums(select(., starts_with("Gt.52.to.53.Weeks.SUM.1"):starts_with("Gt.103.to.104.Weeks.SUM.1"))),
         weeks_104_plus = Gt.104.Weeks.SUM.1) %>% 
  select(Period,monthyr,Provider.Org.Code,RTT.Part.Description,weeks_0_18,weeks_18_52,weeks_52_104,weeks_104_plus, Total, Total.All) %>% 
  mutate(year = as.numeric(substr(monthyr, 4, 5)) + 2000,
         month = substr(monthyr,1,3)) %>% 
  mutate(quarter = case_when(
    month %in% c("Jan", "Feb", "Mar") ~ 4,
    month %in% c("Apr", "May", "Jun") ~ 1,
    month %in% c("Jul", "Aug", "Sep") ~ 2,
    month %in% c("Oct", "Nov", "Dec") ~ 3,
    TRUE ~ NA_integer_
  )) %>% 
  mutate(year = ifelse(quarter==4,year-1,year))


quarterly_rtt_dataset <- rtt_dataset %>% 
  group_by(year,quarter,Provider.Org.Code,RTT.Part.Description) %>% 
  summarise(weeks_0_18 = sum(weeks_0_18, na.rm = TRUE),
            weeks_18_52 = sum(weeks_18_52, na.rm = TRUE),
            weeks_52_104 = sum(weeks_52_104, na.rm = TRUE),
            weeks_104_plus = sum(weeks_104_plus, na.rm = TRUE), 
            Total = sum(Total, na.rm = TRUE), 
            Total.All = sum(Total.All, na.rm = TRUE)) %>% 
  ungroup() %>% 
  rename(org=Provider.Org.Code)

orgs <- quarterly_rtt_dataset %>% 
  pull(org) %>% 
  unique()

org_lkp <- attach_icb_to_org(orgs) %>% 
  mutate(
    divisor = n(),
    .by = health_org_code
  )

quarterly_rtt_dataset <- quarterly_rtt_dataset %>% 
  left_join(
    org_lkp,
    by = join_by(
      org == health_org_code
    ),
    relationship = "many-to-many"
  ) %>% 
  summarise(
    across(
      c(weeks_0_18, weeks_18_52, weeks_52_104, weeks_104_plus, Total, Total.All),
      ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
    ),
    .by = c(year, quarter, icb_code, RTT.Part.Description)
  ) %>% 
  rename(
    org = icb_code
  )

quarterly_rtt_dataset  <- quarterly_rtt_dataset %>% 
  filter(RTT.Part.Description %in% c('Completed Pathways For Non-Admitted Patients',
                                     'Completed Pathways For Admitted Patients')) %>% 
  select(year, quarter, org, RTT.Part.Description, Total)

quarterly_rtt_dataset_pivot <- quarterly_rtt_dataset %>% 
  pivot_wider(
    id_cols = c(year, quarter, org),
    names_from = RTT.Part.Description,
    values_from = Total,
    names_sep = "_"
  )

quarterly_rtt_dataset_pivot <- quarterly_rtt_dataset_pivot %>% 
  rename(completed_admitted = "Completed Pathways For Admitted Patients",
         completed_non_admitted = "Completed Pathways For Non-Admitted Patients") %>% 
  mutate(completed_all = completed_admitted + completed_non_admitted)

baseline_rtt <- quarterly_rtt_dataset_pivot %>% 
  filter(year==2018 & quarter==1) %>% 
  rename(completed_admitted_baseline = completed_admitted,
         completed_non_admitted_baseline = completed_non_admitted,
         completed_all_baseline = completed_all)


quarterly_rtt_dataset_pivot <- left_join(quarterly_rtt_dataset_pivot,baseline_rtt, by = 'org') %>% 
    mutate(completed_admitted_vs_baseline = completed_admitted / completed_admitted_baseline,
         completed_non_admitted_vs_baseline = completed_non_admitted / completed_non_admitted_baseline,
         completed_all_vs_baseline = completed_all / completed_all_baseline) %>% 
  select(-year.y,-quarter.y,-completed_admitted_baseline,-completed_non_admitted_baseline,-completed_all_baseline) %>%   
   rename(year=year.x,
         quarter=quarter.x)


output <- left_join(output, quarterly_rtt_dataset_pivot, by=c('year', 'quarter', 'org')) %>% 
  select(-admitted_18plus_weeks,-non_admitted_18plus_weeks)





# last_month_in_quarter <- c('Mar','Jun','Sep','Dec')
# 
# quarterly_rtt_dataset_last_month <- rtt_dataset %>% 
#   mutate(weeks_0_18 = ifelse(!(month %in% last_month_in_quarter),NA,weeks_0_18),
#          weeks_18_52 = ifelse(!(month %in% last_month_in_quarter),NA,weeks_18_52),
#          weeks_52_104 = ifelse(!(month %in% last_month_in_quarter),NA,weeks_52_104),
#          weeks_104_plus = ifelse(!(month %in% last_month_in_quarter),NA,weeks_104_plus),
#          Total = ifelse(!(month %in% last_month_in_quarter),NA,Total),
#          Total.All = ifelse(!(month %in% last_month_in_quarter),NA,Total.All),
#   ) %>% 
#   group_by(year,quarter,Provider.Org.Code,RTT.Part.Description) %>% 
#   summarise(weeks_0_18 = sum(weeks_0_18, na.rm = TRUE),
#             weeks_18_52 = sum(weeks_18_52, na.rm = TRUE),
#             weeks_52_104 = sum(weeks_52_104, na.rm = TRUE),
#             weeks_104_plus = sum(weeks_104_plus, na.rm = TRUE), 
#             Total = sum(Total, na.rm = TRUE), 
#             Total.All = sum(Total.All, na.rm = TRUE)) %>% 
#   ungroup() %>% 
#   rename(org=Provider.Org.Code)
# 
# orgs <- quarterly_rtt_dataset_last_month %>% 
#   pull(org) %>% 
#   unique()
# 
# org_lkp <- attach_icb_to_org(orgs) %>% 
#   mutate(
#     divisor = n(),
#     .by = health_org_code
#   )
# 
# quarterly_rtt_dataset_v2 <- quarterly_rtt_dataset_last_month %>% 
#   left_join(
#     org_lkp,
#     by = join_by(
#       org == health_org_code
#     ),
#     relationship = "many-to-many"
#   ) %>% 
#   summarise(
#     across(
#       c(weeks_0_18, weeks_18_52, weeks_52_104, weeks_104_plus, Total, Total.All),
#       ~ sum(.x / divisor, na.rm = TRUE) # some health_orgs attributed to multiple icbs, so these are split equally between the icbs
#     ),
#     .by = c(year, quarter, icb_code, RTT.Part.Description)
#   ) %>% 
#   rename(
#     org = icb_code
#   )
# 
# 
# quarterly_rtt_dataset_v2_pivot <- quarterly_rtt_dataset_v2 %>% 
#   pivot_wider(
#     id_cols = c(year, quarter, org),
#     names_from = RTT.Part.Description,
#     values_from = c(weeks_0_18, weeks_18_52, weeks_52_104, weeks_104_plus, Total, Total.All),
#     names_sep = "_"
#   )
# 
# 
# output_v2 <- left_join(output, quarterly_rtt_dataset_v2_pivot, by=c('year', 'quarter', 'org'))
# 
# 
