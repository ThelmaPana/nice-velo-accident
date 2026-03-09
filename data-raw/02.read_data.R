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
annees <- 2005:2024
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

  data <- map_dfr(result_full$data, as_tibble) |>
    mutate(mois = as.integer(mois)) |>
    select(-`__id`)

  # Renommer Accident_Id en Num_Acc si nécessaire
  if ("Accident_Id" %in% names(data)) {
    data <- data %>% rename(Num_Acc = Accident_Id)
  }

  return(data)
}

get_data_by_num_acc <- function(resource_id, annee, type, num_acc_list) {
  print(paste(annee, type, "..."))

  tryCatch({
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

    data <- bind_rows(all_data) |>
      select(-`__id`)
    print(paste("  Total:", nrow(data)))

    return(data)

  }, error = function(e) {
    print(paste("  ERREUR:", conditionMessage(e)))
    return(NULL)
  })
}

get_data_by_url <- function(url) {

  tryCatch({
    temp_file <- tempfile(fileext = ".csv")
    download.file(url, temp_file, mode = "wb", quiet = TRUE)

    data <- read_delim(temp_file, delim = ",", locale = locale(encoding = "latin1"), show_col_types = FALSE)

    unlink(temp_file)

    print(paste("  Total:", nrow(data)))
    return(data)

  }, error = function(e) {
    print(paste("  ERREUR:", conditionMessage(e)))
    return(NULL)
  })
}

foo <- get_data_by_url(toto) |> filter(com == "088")

## All years ----
#--------------------------------------------------------------------------#
# Initialiser les listes pour stocker toutes les données
all_caract <- list()
all_lieux <- list()
all_vehicules <- list()
all_usagers <- list()

# Initialiser le tracker d'erreurs
erreurs <- tibble(annee = integer(), type = character(), resource_id = character(), erreur = character())


# Boucle sur les années
for (annee in annees) {
  print(paste("=== Année", annee, "==="))

  # 1. Caractéristiques
  caract_rid <- catalog_a_telecharger %>%
    filter(annee == !!annee, type == "caracteristiques") %>%
    pull(resource_id)

  if (length(caract_rid) > 0) {
    caract <- get_caracteristiques_nice(caract_rid, annee)

    if (is.null(caract)) {
      erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "caracteristiques", resource_id = caract_rid, erreur = "Échec récupération"))
      next
    }

    num_acc_nice <- caract$Num_Acc
    all_caract[[as.character(annee)]] <- caract

    # 2. Lieux
    lieux_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "lieux") %>%
      pull(resource_id)

    if (length(lieux_rid) > 0) {
      lieux <- get_data_by_num_acc(lieux_rid, annee, "lieux", num_acc_nice) |> mutate(nbv = as.integer(nbv))
      if (is.null(lieux)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "lieux", resource_id = lieux_rid, erreur = "Échec récupération"))
      } else {
        all_lieux[[as.character(annee)]] <- lieux
      }
    }

    # 3. Véhicules
    vehicules_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "vehicules") %>%
      pull(resource_id)

    if (length(vehicules_rid) > 0) {
      vehicules <- get_data_by_num_acc(vehicules_rid, annee, "vehicules", num_acc_nice)
      if (is.null(vehicules)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "vehicules", resource_id = vehicules_rid, erreur = "Échec récupération"))
      } else {
        all_vehicules[[as.character(annee)]] <- vehicules
      }
    }

    # 4. Usagers
    usagers_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "usagers") %>%
      pull(resource_id)

    if (length(usagers_rid) > 0) {
      usagers <- get_data_by_num_acc(usagers_rid, annee, "usagers", num_acc_nice)
      if (is.null(usagers)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "usagers", resource_id = usagers_rid, erreur = "Échec récupération"))
      } else {
        all_usagers[[as.character(annee)]] <- usagers
      }
    }
  }
}

# Combiner toutes les années
caract_final <- bind_rows(all_caract)
lieux_final <- bind_rows(all_lieux)
vehicules_final <- bind_rows(all_vehicules)
usagers_final <- bind_rows(all_usagers)

ggplot(caract_final) +
  geom_point(aes(x = long, y = lat, colour = an)) +
  coord_quickmap()

caract_final |> count(an)
lieux_final |> left_join(caract_final) |> count(an)
vehicules_final |> left_join(caract_final) |> count(an)
usagers_final |> left_join(caract_final) |> count(an)

erreurs
# 2021 et 2024 ne peuvent pas être lus


# Accidents impliquant des vélos
vehicules_velo <- vehicules_final |> filter(catv == 1)
acc_velo <- vehicules_velo |> pull(Num_Acc)

acc_velo

caract_velo <- caract_final |> filter(Num_Acc %in% acc_velo)
lieux_velo <- lieux_final |> filter(Num_Acc %in% acc_velo)
usagers_velo <- usagers_final |> filter(Num_Acc %in% acc_velo)

#


## Test code commune ----
#--------------------------------------------------------------------------#
get_data_by_url <- function(url) {
  tryCatch({
    temp_file <- tempfile(fileext = ".csv")
    download.file(url, temp_file, mode = "wb", quiet = TRUE)

    data <- read_delim(temp_file, delim = ",", locale = locale(encoding = "latin1"), show_col_types = FALSE)

    unlink(temp_file)

    print(paste("  Total:", nrow(data)))
    return(data)

  }, error = function(e) {
    print(paste("  ERREUR:", conditionMessage(e)))
    return(NULL)
  })
}

# caract 2009 est un tsv

test <- catalog_a_telecharger %>% filter(annee == 2019, type == "caracteristiques")
data <- get_data_by_url(test$url)
unique(data$com)[1:20]  # Voir les codes

## all years ----
#--------------------------------------------------------------------------#
library(tidyverse)
library(httr2)
library(here)

# Configuration
base_url <- "https://tabular-api.data.gouv.fr/api/resources"

# Lire le catalogue
catalog <- read_csv("catalog_accidents_final.csv")

# Filtrer pour les années voulues
annees <- 2005:2024
catalog_a_telecharger <- catalog %>%
  filter(annee %in% annees)

# ==============================================================================
# FONCTIONS
# ==============================================================================

# Fonction pour caractéristiques (filtre avant via API)
get_caracteristiques_nice <- function(resource_id, annee, code_commune, url_fallback = NULL) {
  print(paste(annee, "caracteristiques ..."))

  url <- paste0(
    base_url, "/", resource_id, "/data/",
    "?com__exact=", code_commune
  )

  tryCatch({
    response <- request(url) %>% req_perform()
    result <- resp_body_json(response)

    total <- result$meta$total
    print(paste("  Total:", total))

    url_full <- paste0(url, "&page_size=", total)
    response_full <- request(url_full) %>% req_perform()
    result_full <- resp_body_json(response_full)

    data <- map_dfr(result_full$data, as_tibble) %>%
      mutate(mois = as.integer(mois))

    # Renommer Accident_Id en Num_Acc si nécessaire
    if ("Accident_Id" %in% names(data)) {
      data <- data %>% rename(Num_Acc = Accident_Id)
    }

    return(data)

  }, error = function(e) {
    print(paste("  ERREUR API:", conditionMessage(e)))

    # Fallback : téléchargement direct
    if (!is.null(url_fallback)) {
      print("  Tentative téléchargement direct...")
      data_all <- get_data_by_url(url_fallback)

      # DEBUG
      print("Colonnes disponibles:")
      print(names(data_all))
      print("Code commune recherché:")
      print(code_commune)

      # Vérifier si la colonne existe
      if (!"com" %in% names(data_all)) {
        print("ATTENTION: colonne 'com' absente!")
        return(NULL)
      }

      data <- data_all %>%
        filter(com == code_commune) %>%
        mutate(mois = as.integer(mois))

      if ("Accident_Id" %in% names(data)) {
        data <- data %>% rename(Num_Acc = Accident_Id)
      }

      return(data)
    }

    return(NULL)
  })
}

# Fonction pour autres tables (filtre après via Num_Acc)
get_data_by_num_acc <- function(resource_id, annee, type, num_acc_list) {
  print(paste(annee, type, "..."))

  tryCatch({
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

  }, error = function(e) {
    print(paste("  ERREUR:", conditionMessage(e)))
    return(NULL)
  })
}

# Fonction pour téléchargement direct par URL
get_data_by_url <- function(url) {
  tryCatch({
    temp_file <- tempfile(fileext = ".csv")
    download.file(url, temp_file, mode = "wb", quiet = TRUE)

    #data <- read_delim(temp_file, delim = ",", locale = locale(encoding = "latin1"), show_col_types = FALSE)
    data <- read_delim(temp_file, locale = locale(encoding = "latin1"), show_col_types = FALSE)

    unlink(temp_file)

    print(paste("  Total:", nrow(data)))
    return(data)

  }, error = function(e) {
    print(paste("  ERREUR:", conditionMessage(e)))
    return(NULL)
  })
}

# ==============================================================================
# BOUCLE
# ==============================================================================

# Initialiser les listes
all_caract <- list()
all_lieux <- list()
all_vehicules <- list()
all_usagers <- list()
erreurs <- tibble(annee = integer(), type = character(), resource_id = character(), erreur = character())

# Boucle sur les années

for (annee in annees) {
  print(paste("=== Année", annee, "==="))

  # Code commune selon l'année
  code_commune <- if (annee < 2019) "088" else "06088"

  # 1. Caractéristiques
  caract_rid <- catalog_a_telecharger %>%
    filter(annee == !!annee, type == "caracteristiques") %>%
    pull(resource_id)

  caract_url <- catalog_a_telecharger %>%
    filter(annee == !!annee, type == "caracteristiques") %>%
    pull(url)

  if (length(caract_rid) > 0) {
    caract <- get_caracteristiques_nice(caract_rid, annee, code_commune, caract_url)

    if (is.null(caract)) {
      erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "caracteristiques", resource_id = caract_rid, erreur = "Échec récupération"))
      next
    }

    num_acc_nice <- caract$Num_Acc
    all_caract[[as.character(annee)]] <- caract

    # 2. Lieux
    lieux_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "lieux") %>%
      pull(resource_id)

    lieux_url <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "lieux") %>%
      pull(url)

    if (length(lieux_rid) > 0) {
      lieux <- get_data_by_num_acc(lieux_rid, annee, "lieux", num_acc_nice)

      # Fallback si échec API
      if (is.null(lieux) && length(lieux_url) > 0) {
        print("  Tentative téléchargement direct...")
        lieux_all <- get_data_by_url(lieux_url)

        # Renommer si nécessaire
        if ("Accident_Id" %in% names(lieux_all)) {
          lieux_all <- lieux_all %>% rename(Num_Acc = Accident_Id)
        }

        lieux <- lieux_all %>%
          filter(Num_Acc %in% num_acc_nice) %>%
          mutate(nbv = as.integer(nbv))
      }

      if (is.null(lieux)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "lieux", resource_id = lieux_rid, erreur = "Échec récupération"))
      } else {
        all_lieux[[as.character(annee)]] <- lieux
      }
    }

    # 3. Véhicules
    vehicules_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "vehicules") %>%
      pull(resource_id)

    vehicules_url <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "vehicules") %>%
      pull(url)

    if (length(vehicules_rid) > 0) {
      vehicules <- get_data_by_num_acc(vehicules_rid, annee, "vehicules", num_acc_nice)

      # Fallback si échec API
      if (is.null(vehicules) && length(vehicules_url) > 0) {
        print("  Tentative téléchargement direct...")
        vehicules_all <- get_data_by_url(vehicules_url)

        if ("Accident_Id" %in% names(vehicules_all)) {
          vehicules_all <- vehicules_all %>% rename(Num_Acc = Accident_Id)
        }

        vehicules <- vehicules_all %>%
          filter(Num_Acc %in% num_acc_nice)
      }

      if (is.null(vehicules)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "vehicules", resource_id = vehicules_rid, erreur = "Échec récupération"))
      } else {
        all_vehicules[[as.character(annee)]] <- vehicules
      }
    }

    # 4. Usagers
    usagers_rid <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "usagers") %>%
      pull(resource_id)

    usagers_url <- catalog_a_telecharger %>%
      filter(annee == !!annee, type == "usagers") %>%
      pull(url)

    if (length(usagers_rid) > 0) {
      usagers <- get_data_by_num_acc(usagers_rid, annee, "usagers", num_acc_nice)

      # Fallback si échec API
      if (is.null(usagers) && length(usagers_url) > 0) {
        print("  Tentative téléchargement direct...")
        usagers_all <- get_data_by_url(usagers_url)

        if ("Accident_Id" %in% names(usagers_all)) {
          usagers_all <- usagers_all %>% rename(Num_Acc = Accident_Id)
        }

        usagers <- usagers_all %>%
          filter(Num_Acc %in% num_acc_nice)
      }

      if (is.null(usagers)) {
        erreurs <- bind_rows(erreurs, tibble(annee = annee, type = "usagers", resource_id = usagers_rid, erreur = "Échec récupération"))
      } else {
        all_usagers[[as.character(annee)]] <- usagers
      }
    }
  }
}

# ==============================================================================
# CONSOLIDATION ET SAUVEGARDE
# ==============================================================================

# Combiner toutes les années
caract_final <- bind_rows(all_caract)
lieux_final <- bind_rows(all_lieux)
vehicules_final <- bind_rows(all_vehicules)
usagers_final <- bind_rows(all_usagers)



## Test URL ----
#--------------------------------------------------------------------------#
test_annee <- function(annee_test) {
  print(paste("=== TEST", annee_test, "==="))

  code_commune <- if (annee_test < 2019) "088" else "06088"

  caract_url <- catalog_a_telecharger %>%
    filter(annee == annee_test, type == "caracteristiques") %>%
    pull(url)

  temp_file <- tempfile(fileext = ".csv")
  download.file(caract_url, temp_file, mode = "wb", quiet = TRUE)

  # Essayer avec fread
  library(data.table)
  data_all <- fread(temp_file, encoding = "UTF-8") %>% as_tibble()

  unlink(temp_file)

  print(paste("Lignes totales:", nrow(data_all)))
  print("Colonnes:")
  print(names(data_all))

  if ("com" %in% names(data_all)) {
    print("20 premiers codes commune:")
    print(head(unique(data_all$com), 20))

    # Filtre qui marche pour les deux types
    if (is.numeric(data_all$com)) {
      data_nice <- data_all %>% filter(com == as.integer(code_commune))
    } else {
      data_nice <- data_all %>% filter(com == code_commune)
    }
    print(paste("Lignes Nice:", nrow(data_nice)))
  }

  return(data_all)
}

test_2006 <- test_annee(2012)
