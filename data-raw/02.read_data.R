#--------------------------------------------------------------------------#
# Project: nice-velo-accident
# Script purpose: Lire les datasets d’accidents à partir des RID
# Date: 13/02/2026
# Author: Thelma Panaïotis
#--------------------------------------------------------------------------#


## test ----
#--------------------------------------------------------------------------#
library(tidyverse)
library(httr2)
library(here)

# Configuration
base_url <- "https://tabular-api.data.gouv.fr/api/resources"
nice_code <- "06088"

# Lire le catalogue
catalog <- read_csv(here("data-raw", "01.catalog.csv"), show_col_types = FALSE)

# Filtrer pour les années voulues
annees <- 2023:2024
catalog_a_telecharger <- catalog %>%
  filter(annee %in% annees)

# Fonction pour caractéristiques (filtre avant)
get_caracteristiques_nice <- function(resource_id, annee) {
  print(paste(annee, "caracteristiques ..."))

  url <- paste0(
    base_url, "/", resource_id, "/data/",
    "?com__exact=", nice_code
  )

  response <- request(url) %>% req_perform()
  result <- resp_body_json(response)

  total <- result$meta$total
  print(paste("  Total:", total))

  url_full <- paste0(url, "&page_size=", total)
  response_full <- request(url_full) %>% req_perform()
  result_full <- resp_body_json(response_full)

  data <- map_dfr(result_full$data, as_tibble)
  return(data)
}

# Fonction pour autres tables (récupère tout)
get_all_data <- function(resource_id, annee, type) {
  print(paste(annee, type, "..."))

  all_data <- list()
  next_url <- paste0(base_url, "/", resource_id, "/data/")
  page <- 1

  while (!is.null(next_url)) {
    print(paste("  Page", page))
    response <- request(next_url) %>% req_perform()
    result <- resp_body_json(response)

    all_data[[page]] <- map_dfr(result$data, ~ as_tibble(compact(.x)))

    next_url <- result$links$`next`
    page <- page + 1
  }

  data <- bind_rows(all_data)
  print(paste("  Total:", nrow(data), "lignes"))

  return(data)
}

# Utilisation
annee_test <- 2023
caract_rid <- catalog_a_telecharger %>%
  filter(annee == annee_test, type == "caracteristiques") %>%
  pull(resource_id)

caract <- get_caracteristiques_nice(caract_rid, annee_test)
num_acc_nice <- caract$Num_Acc

lieux_rid <- catalog_a_telecharger %>%
  filter(annee == annee_test, type == "lieux") %>%
  pull(resource_id)


get_data_by_num_acc <- function(resource_id, annee, type, num_acc_list) {
  print(paste(annee, type, "..."))

  num_acc_string <- paste(as.character(num_acc_list), collapse = ",")

  all_data <- list()
  next_url <- paste0(
    base_url, "/", resource_id, "/data/",
    "?Num_Acc__in=", num_acc_string
  )

  page <- 1

  while (!is.null(next_url)) {
    print(paste("  Page", page))
    response <- request(next_url) %>% req_perform()
    result <- resp_body_json(response)

    all_data[[page]] <- map_dfr(result$data, ~ as_tibble(compact(.x)))

    next_url <- result$links$`next`
    page <- page + 1
  }

  data <- bind_rows(all_data)
  print(paste("  Total:", nrow(data)))

  return(data)
}

lieux <- get_data_by_num_acc(lieux_rid, annee_test, "lieux", num_acc_nice)

# Véhicules
vehicules_rid <- catalog_a_telecharger %>%
  filter(annee == annee_test, type == "vehicules") %>%
  pull(resource_id)

vehicules <- get_data_by_num_acc(vehicules_rid, annee_test, "vehicules", num_acc_nice)

# Usagers
usagers_rid <- catalog_a_telecharger %>%
  filter(annee == annee_test, type == "usagers") %>%
  pull(resource_id)

usagers <- get_data_by_num_acc(usagers_rid, annee_test, "usagers", num_acc_nice)

# Vérifier
print(glimpse(vehicules))
print(glimpse(usagers))


## join ----
#--------------------------------------------------------------------------#
caract |> filter(Num_Acc == 202300000184)
vehicules |> filter(Num_Acc == 202300000184)
usagers |> filter(Num_Acc == 202300000184)
lieux |> filter(Num_Acc == 202300000184)
