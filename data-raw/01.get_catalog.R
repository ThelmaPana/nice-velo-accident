#--------------------------------------------------------------------------#
# Project: nice-velo-accident
# Script purpose: Récupérer la liste des RID des fichiers du dataset des accidents corporels
# Date: 13/02/2026
# Author: Thelma Panaïotis
#--------------------------------------------------------------------------#


## Set up ----
#--------------------------------------------------------------------------#
library(tidyverse)
library(httr2)
library(here)

catalog_rid <- "4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d" # RID du catalogue
base_url <- "https://tabular-api.data.gouv.fr/api/resources" # url de l’API
dataset_id <- "53698f4ca3a729239d2036df" # ID du dataset accidents


## Use API to get catalogue and filter with dataset_id  ----
#--------------------------------------------------------------------------#
# Catalogue is HUGE, need to filter before request

all_data <- list()
page <- 1
has_more <- TRUE

while (has_more) {
  url <- paste0(
    base_url,
    "/",
    catalog_rid,
    "/data/",
    "?dataset.id__exact=",
    dataset_id,
    "&page=",
    page,
    "&page_size=200"
  )

  print(paste("Page", page))
  response <- request(url) %>% req_perform()
  result <- resp_body_json(response)

  if (length(result$data) == 0) {
    break
  }

  all_data[[page]] <- map_dfr(result$data, ~ as_tibble(compact(.x)))

  has_more <- !is.null(result$links$`next`)
  page <- page + 1
}

# Assemble together
catalog_accidents <- bind_rows(all_data)


## Clean and save ----
#--------------------------------------------------------------------------#
# Keep only relevant columns
catalog <- catalog_accidents %>%
  select(resource_id = id, title, url) %>%
  distinct(resource_id, .keep_all = TRUE)

# Keep only relevant files, i.e. files of type
# - caracteristiques
# - lieux
# - vehicules
# - usagers
catalog <- catalog %>%
  mutate(
    annee = str_extract(title, "\\d{4}"),
    type = case_when(
      str_detect(tolower(title), "caract|carc") ~ "caracteristiques",
      str_detect(tolower(title), "lieux") ~ "lieux",
      str_detect(tolower(title), "vehic") ~ "vehicules",
      str_detect(tolower(title), "usagers") ~ "usagers",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(type), !is.na(annee), !str_detect(tolower(title), "immatricule"))

# Save
write_csv(catalog, here("data-raw", "01.catalog.csv"))

