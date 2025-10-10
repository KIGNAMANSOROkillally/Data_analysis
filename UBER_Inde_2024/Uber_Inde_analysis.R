# ******************************************************************************
# Analyse des données Uber 2024
# Script principal 
# ******************************************************************************

# --------------------------
# 1. CHARGEMENT DES LIBRARIES
# --------------------------

library(tidyverse)
library(lubridate)
library(janitor)
library(skimr)
library(ggthemes)
library(viridis)
library(patchwork)
library(tidymodels)
library(xgboost)
library(DALEXtra)
library(ggrepel)
library(scales)

# Définir le thème ggplot2 par défaut
theme_set(theme_minimal(base_size = 12) +
              theme(plot.title = element_text(face = "bold", size = 16),
                    plot.subtitle = element_text(color = "gray50"),
                    panel.grid.minor = element_blank()))

# --------------------------
# 2. CONFIGURATION
# --------------------------
# Création des répertoires si nécessaire
dirs_to_create <- c("data/raw", "data/processed", "figs", "models", "reports")
for(dir in dirs_to_create) {
    if(!dir.exists(dir)) dir.create(dir, recursive = TRUE)
}

# --------------------------
# 3. FONCTIONS PERSONNALISÉES
# --------------------------

# Fonction de winsorisation pour traiter les valeurs extrêmes
winsorize <- function(x, probs = c(0.01, 0.99)) {
    if(!is.numeric(x)) return(x)
    
    quantiles <- quantile(x, probs, na.rm = TRUE)
    x[x < quantiles[1]] <- quantiles[1]
    x[x > quantiles[2]] <- quantiles[2]
    x
}

# Fonction pour standardiser les types de véhicules
standardize_vehicle_type <- function(vehicle_type) {
    vehicle_type <- tolower(vehicle_type)
    case_when(
        str_detect(vehicle_type, "bike|ebike") ~ "Bike/eBike",
        str_detect(vehicle_type, "sedan") ~ "Sedan",
        str_detect(vehicle_type, "mini") ~ "Mini",
        str_detect(vehicle_type, "auto") ~ "Auto",
        str_detect(vehicle_type, "xl") ~ "XL",
        TRUE ~ as.character(vehicle_type)
    )
}

# --------------------------
# 4. IMPORT ET NETTOYAGE DES DONNÉES
# --------------------------

# Lecture des données 
df <- read_csv("data/raw/ncr_ride_bookings.csv", 
               na = c("", "NA", "null", "NULL"))

# Nettoyage initial
rides_clean <- df %>%
    # Nettoyer les noms de colonnes
    clean_names() %>%
    # Supprimer les guillemets excessifs dans les IDs
    mutate(booking_id = str_remove_all(booking_id, '"'),
           customer_id = str_remove_all(customer_id, '"')) %>%
    # Créer un datetime unique pour la réservation
    mutate(booking_datetime = ymd_hms(paste(date, time))) %>%
    # Standardiser le type de véhicule
    mutate(vehicle_type = standardize_vehicle_type(vehicle_type)) %>%
    # Nettoyer les textes
    mutate(across(c(pickup_location, drop_location, payment_method), 
                  ~str_squish(str_to_title(.)))) %>%
    # Gérer les valeurs manquantes dans les flags d'annulation
    mutate(across(c(cancelled_rides_by_customer, cancelled_rides_by_driver, incomplete_rides),
                  ~if_else(is.na(.), 0, .))) %>%
    # Corriger les incohérences de statut
    mutate(booking_status = case_when(
        cancelled_rides_by_customer == 1 ~ "Cancelled by Customer",
        cancelled_rides_by_driver == 1 ~ "Cancelled by Driver",
        incomplete_rides == 1 ~ "Incomplete",
        is.na(booking_value) & is.na(ride_distance) ~ "No Driver Found",
        TRUE ~ booking_status
    )) %>%
    # Réordonner les colonnes
    select(booking_datetime, date, time, booking_id, customer_id, everything())

# Winsoriser les valeurs numériques extrêmes
rides_clean <- rides_clean %>%
    mutate(across(c(booking_value, ride_distance, avg_vtat, avg_ctat), winsorize))

# Vérification des doublons
duplicate_count <- sum(duplicated(rides_clean$booking_id))
if(duplicate_count > 0) {
    message("Suppression de ", duplicate_count, " doublons")
    rides_clean <- rides_clean %>%
        distinct(booking_id, .keep_all = TRUE)
}

# Validation des règles de cohérence
rides_clean <- rides_clean %>%
    mutate(
        # Si le statut est Completed, les valeurs doivent être présentes
        booking_value = if_else(booking_status == "Completed" & is.na(booking_value), 
                                median(booking_value, na.rm = TRUE), booking_value),
        ride_distance = if_else(booking_status == "Completed" & is.na(ride_distance), 
                                median(ride_distance, na.rm = TRUE), ride_distance),
        # Assurer que les ratings sont dans [1,5]
        driver_ratings = if_else(!is.na(driver_ratings) & (driver_ratings < 1 | driver_ratings > 5), 
                                 NA_real_, driver_ratings),
        customer_rating = if_else(!is.na(customer_rating) & (customer_rating < 1 | customer_rating > 5), 
                                  NA_real_, customer_rating)
    )

# --------------------------
# 5. FEATURE ENGINEERING
# --------------------------

# Features temporelles
rides_clean <- rides_clean %>%
    mutate(
        hour = hour(booking_datetime),
        dow = wday(booking_datetime, label = TRUE, abbr = FALSE),
        month = month(booking_datetime, label = TRUE, abbr = FALSE),
        week = week(booking_datetime),
        is_peak = hour %in% c(7:9, 17:19),
        time_of_day = case_when(
            hour >= 5 & hour < 12 ~ "Morning",
            hour >= 12 & hour < 17 ~ "Afternoon",
            hour >= 17 & hour < 21 ~ "Evening",
            TRUE ~ "Night"
        )
    )

# Features de route
rides_clean <- rides_clean %>%
    mutate(
        route_id = paste(pickup_location, drop_location, sep = " -> "),
        value_per_km = booking_value / ride_distance
    )

# Flags explicites pour l'analyse
rides_clean <- rides_clean %>%
    mutate(
        is_completed = booking_status == "Completed",
        is_cancelled_by_customer = booking_status == "Cancelled by Customer",
        is_cancelled_by_driver = booking_status == "Cancelled by Driver",
        is_incomplete = booking_status == "Incomplete",
        is_no_driver = booking_status == "No Driver Found",
        has_rating = !is.na(customer_rating) & !is.na(driver_ratings)
    )

# --------------------------
# 6. CRÉATION DES TABLES DÉRIVÉES
# --------------------------

# Table clients
customers <- rides_clean %>%
    group_by(customer_id) %>%
    summarise(
        first_ride = min(booking_datetime, na.rm = TRUE),
        last_ride = max(booking_datetime, na.rm = TRUE),
        total_bookings = n(),
        completed_rides = sum(is_completed, na.rm = TRUE),
        cancelled_by_customer = sum(is_cancelled_by_customer, na.rm = TRUE),
        cancelled_by_driver = sum(is_cancelled_by_driver, na.rm = TRUE),
        avg_booking_value = mean(booking_value, na.rm = TRUE),
        avg_rating_given = mean(customer_rating, na.rm = TRUE),
        avg_rating_received = mean(driver_ratings, na.rm = TRUE),
        favorite_vehicle = ifelse(length(na.omit(vehicle_type)) > 0, 
                                  names(which.max(table(vehicle_type))), 
                                  NA_character_),
        favorite_payment = ifelse(length(na.omit(payment_method)) > 0, 
                                  names(which.max(table(payment_method))), 
                                  NA_character_)
    ) %>%
    mutate(
        cancellation_rate = (cancelled_by_customer + cancelled_by_driver) / total_bookings,
        completion_rate = completed_rides / total_bookings,
        customer_tenure = as.numeric(difftime(last_ride, first_ride, units = "days"))
    )

# Table routes
routes <- rides_clean %>%
    filter(!is.na(pickup_location) & !is.na(drop_location)) %>%
    group_by(route_id, pickup_location, drop_location) %>%
    summarise(
        total_volume = n(),
        completion_rate = sum(is_completed, na.rm = TRUE) / n(),
        avg_distance = mean(ride_distance, na.rm = TRUE),
        avg_value = mean(booking_value, na.rm = TRUE),
        avg_value_per_km = mean(value_per_km, na.rm = TRUE),
        avg_vtat = mean(avg_vtat, na.rm = TRUE),
        avg_ctat = mean(avg_ctat, na.rm = TRUE),
        cancellation_rate = sum(is_cancelled_by_customer | is_cancelled_by_driver, na.rm = TRUE) / n()
    ) %>%
    ungroup()

# --------------------------
# 7. ANALYSE EXPLORATOIRE ET VISUALISATIONS
# --------------------------

# Fonction sécurisée pour sauvegarder les graphiques
safe_ggsave <- function(filename, plot, ...) {
    tryCatch({
        ggsave(filename, plot, ...)
        message("Graphique sauvegardé: ", filename)
    }, error = function(e) {
        message("Erreur lors de la sauvegarde de ", filename, ": ", e$message)
    })
}

# 7.1 Distribution des statuts de réservation
status_plot <- rides_clean %>%
    count(booking_status) %>%
    mutate(percent = n / sum(n) * 100,
           booking_status = fct_reorder(booking_status, n)) %>%
    ggplot(aes(x = booking_status, y = n, fill = booking_status)) +
    geom_col(show.legend = FALSE) +
    geom_text(aes(label = paste0(round(percent, 1), "%")), 
              hjust = -0.1, size = 3.5) +
    coord_flip() +
    labs(title = "Distribution des statuts de réservation",
         x = NULL, y = "Nombre de réservations") +
    scale_fill_viridis(discrete = TRUE)

# 7.2 Volume de réservations par heure et jour de la semaine
time_heatmap <- rides_clean %>%
    filter(!is.na(hour) & !is.na(dow)) %>%
    count(hour, dow) %>%
    ggplot(aes(x = hour, y = dow, fill = n)) +
    geom_tile() +
    scale_fill_viridis(name = "Nombre de\nreservations", option = "magma") +
    scale_x_continuous(breaks = seq(0, 23, by = 3)) +
    labs(title = "Volume de réservations par heure et jour de la semaine",
         x = "Heure de la journée", y = "Jour de la semaine") +
    theme(legend.position = "bottom")

# 7.3 Performance par type de véhicule
vehicle_summary <- rides_clean %>%
    filter(!is.na(vehicle_type)) %>%
    group_by(vehicle_type) %>%
    summarise(
        total_rides = n(),
        completion_rate = sum(is_completed, na.rm = TRUE) / n(),
        avg_value = mean(booking_value, na.rm = TRUE),
        avg_distance = mean(ride_distance, na.rm = TRUE),
        avg_rating = mean(customer_rating, na.rm = TRUE)
    ) %>%
    mutate(vehicle_type = fct_reorder(vehicle_type, total_rides))

vehicle_volume_plot <- ggplot(vehicle_summary, aes(x = vehicle_type, y = total_rides, fill = vehicle_type)) +
    geom_col(show.legend = FALSE) +
    coord_flip() +
    labs(title = "Volume de réservations par type de véhicule", 
         x = NULL, y = "Nombre de réservations") +
    scale_fill_viridis(discrete = TRUE)

vehicle_value_plot <- ggplot(vehicle_summary, aes(x = vehicle_type, y = avg_value, fill = vehicle_type)) +
    geom_col(show.legend = FALSE) +
    coord_flip() +
    labs(title = "Valeur moyenne par course", 
         x = NULL, y = "Valeur moyenne (₹)") +
    scale_fill_viridis(discrete = TRUE)

# 7.4 Top routes par revenu
top_routes <- routes %>%
    filter(total_volume >= 5) %>%
    slice_max(order_by = avg_value * total_volume, n = 10) %>%
    mutate(route_label = str_wrap(route_id, width = 30),
           route_label = fct_reorder(route_label, avg_value * total_volume))

route_revenue_plot <- ggplot(top_routes, aes(x = route_label, y = avg_value * total_volume, fill = avg_value)) +
    geom_col() +
    coord_flip() +
    labs(title = "Top 10 des routes par revenu total",
         x = "Route", y = "Revenu total (₹)") +
    scale_fill_viridis(name = "Valeur moyenne", option = "plasma") +
    theme(legend.position = "bottom")

# 7.5 Relation entre distance et valeur des courses
completed_rides <- rides_clean %>% 
    filter(is_completed & !is.na(ride_distance) & !is.na(booking_value))

distance_value_plot <- ggplot(completed_rides, aes(x = ride_distance, y = booking_value)) +
    geom_point(aes(color = vehicle_type), alpha = 0.6, size = 1.5) +
    geom_smooth(method = "lm", se = FALSE, color = "red") +
    labs(title = "Relation entre distance et valeur des courses",
         x = "Distance (km)", y = "Valeur (₹)", color = "Type de véhicule") +
    scale_color_viridis(discrete = TRUE) +
    theme(legend.position = "bottom")

# 7.6 Distribution des ratings
ratings_plot <- completed_rides %>%
    filter(!is.na(driver_ratings) | !is.na(customer_rating)) %>%
    select(driver_ratings, customer_rating) %>%
    pivot_longer(cols = everything(), 
                 names_to = "rating_type", 
                 values_to = "rating",
                 values_drop_na = TRUE) %>%
    mutate(rating_type = case_when(
        rating_type == "driver_ratings" ~ "Note au chauffeur",
        rating_type == "customer_rating" ~ "Note du client",
        TRUE ~ rating_type
    )) %>%
    ggplot(aes(x = rating, fill = rating_type)) +
    geom_histogram(binwidth = 0.5, alpha = 0.7, position = "identity") +
    labs(title = "Distribution des notes", 
         x = "Note (1-5)", y = "Nombre de courses", 
         fill = "Type de note") +
    scale_fill_viridis(discrete = TRUE) +
    facet_wrap(~rating_type, ncol = 1) +
    theme(legend.position = "none")

# 7.7 Taux de complétion par type de véhicule
completion_plot <- vehicle_summary %>%
    ggplot(aes(x = vehicle_type, y = completion_rate, fill = vehicle_type)) +
    geom_col(show.legend = FALSE) +
    coord_flip() +
    scale_y_continuous(labels = scales::percent) +
    labs(title = "Taux de complétion par type de véhicule",
         x = NULL, y = "Taux de complétion") +
    scale_fill_viridis(discrete = TRUE)

# Sauvegarde des visualisations
safe_ggsave("figs/status_distribution.png", status_plot, width = 10, height = 6)
safe_ggsave("figs/time_heatmap.png", time_heatmap, width = 10, height = 6)
safe_ggsave("figs/vehicle_volume.png", vehicle_volume_plot, width = 10, height = 6)
safe_ggsave("figs/vehicle_value.png", vehicle_value_plot, width = 10, height = 6)
safe_ggsave("figs/completion_rate.png", completion_plot, width = 10, height = 6)
safe_ggsave("figs/top_routes.png", route_revenue_plot, width = 12, height = 8)
safe_ggsave("figs/distance_value.png", distance_value_plot, width = 10, height = 6)
safe_ggsave("figs/ratings_distribution.png", ratings_plot, width = 10, height = 8)

# Combiner les graphiques de véhicules
vehicle_plots <- (vehicle_volume_plot + vehicle_value_plot) / completion_plot +
    plot_annotation(title = "Performance par type de véhicule", 
                    theme = theme(plot.title = element_text(size = 16)))
safe_ggsave("figs/vehicle_performance.png", vehicle_plots, width = 12, height = 10)

cat("✓ Analyse exploratoire terminée\n")
cat("✓ Visualisations générées dans le dossier 'figs/'\n")

