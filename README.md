
*This README.md has been generated automatically from `fast_spatial_matching.Rmd`*

# Installation

    suppressPackageStartupMessages({
        library(here)        # Working directory management
        
        library(dplyr)       # Manipulating data.frames - core   (Tidyverse)
        library(dbplyr)      # SQL back-end for dplyr            (Tidyverse)
        
        library(duckdb)      # Ultra-fast analytics-oriented DB
        library(arrow)       # Fast and memory-efficient I/O
        
        library(furrr)       # Parallel processing (drop-in replacement for `purrr`)
        
        library(osrm)        # OSM Routing (on-foot distance calculations)
    })

    options(
        scipen = 999L,
        future.rng.onMisuse = "ignore"
    )

    Sys.setenv(RENV_CONFIG_SANDBOX_ENABLED = FALSE)

    future::plan(future::multisession, workers = 32L)

# Objectifs

L’objectif de ce document est de démontrer la faisabilité de répondre à
une question du style “combien de personnes habitent à moins de 500m
d’une station de bus en France ?”, à l’échelle de la France entière.

# Méthodologie

La méthode proposée est découpée en deux étapes:

**1.** Un premier filtre grossier sur la base d’une distance Haversine
entre chaque individu et l’ensemble des stations de bus. Cette première
étape permettra de ne garder que les points qui sont à une distance
&lt;= 500m d’une station à vol d’oiseau.

Cette première étape sera effectuée en utilisant la base de données
DuckDB, qui est un moteur de base de données analytique ultra-rapide.
DuckDB est particulièrement adapté pour les opérations de type “join” et
“filter” sur de gros volumes de données, et est capable de traiter des
milliards de lignes en quelques secondes.

**2.** Pour les paires d’habitants-stations ayant passé le premier
filtre, un second filtre plus précis sur la base de la distance à pieds
sera effectué en utilisant le service de routage OpenStreetMap (OSRM).

Pour accélérer le traitement, les calculs seront effectués en parallèle
sur 32 coeurs (via la librarie `furrr`), en utilisant un serveur OSRM
local pour ne pas être limité à une requête par seconde.

# Données

Les données de cette example incluent deux composantes: - Un millier de
coordonnées de stations de bus en France \* Colonnes: `stop_id`,
`stop_name`, `longitude`, `latitude` - Cinquante millions de points
aléatoires générés dans les frontières de la France \* Colonnes: `id`,
`lng`, `lat`

Les données sont incluses dans ce repository. Si vous souhaitez les
régénérer vous-même, suivez les étapes ci-dessous:

**Chemins vers les données:**

    data_dir <- here("data")                      # Global data directory
    stations_dir <- here(data_dir, "stations")    # Stations sub-directory
    points_dir <- here(data_dir, "points")        # Points sub-directory

## Génération des données (optionnel)

### Stations de bus

Les données des stations de bus ont été téléchargées depuis:
<https://www.data.gouv.fr/fr/datasets/liste-des-stations-de-bus-de-tramway-1/>

**Téléchargement:**

    download.file(
        "https://www.data.gouv.fr/fr/datasets/r/66157055-5a57-4913-b158-11bb75c7da03",
        here::here(stations_dir, "stations.csv")
    )

**Nettoyage:**

    arrow::read_csv2_arrow(here(stations_dir, "stations.csv")) |> 
        select(stop_id, stop_name, stop_coordinates) |> 
        tidyr::separate_wider_delim(stop_coordinates, delim = ",", names = c("latitude", "longitude")) |> 
        utils::type.convert(as.is = TRUE) |> 
        arrow::write_csv_arrow(here(stations_dir, "stations.csv"))

### Habitants

Générons 50 millions de points aléatoires dans les frontières de la
France, pour simuler les coordonnées de ses habitants. N.B.: La
répartition de ces coordonnées sera certainement peu représentative de
la distribution spatiale des habitants en France, mais cela aura peu
d’incidence sur cet example théorique.

    source(here("R/helpers.R"))

    # ATTENTION: re-générer ces données prends plusieurs heures
    generate_random_points(country = "France", n_batches = 100, batch_size = 5e5, output_dir = points_dir)

## Chargement des données

Ici, nous chargons les données en mémoire en utilisant la librairie
`arrow`, puis les passons à DuckDB pour les opérations de filtrage et de
jointure.

**Initialisation de la base de données:**

    con <- duckdb::dbConnect(
      duckdb::duckdb(),
      dbdir = ":memory:", # Instance temporaire en mémoire
      config = list(memory_limit = "50GB", temp_directory = here("_temp"), threads = 32L)
    )

### Stations de bus

    arrow::read_csv_arrow(here(stations_dir, "stations.csv")) |>
        copy_to(con, df = _, "stations", temporary = FALSE, indexes = c("longitude", "latitude"))

### Habitants

Chargeons les 50M de points en mémoire sans passer par R (grace au
*‘lazy streaming’* de `arrow`) pour ensuite les “passer” à DuckDB et les
indexer.

    arrow::open_dataset(points_dir) |>
        rename(lng = X, lat = Y) |>
        arrow::to_duckdb(con, "points") |>
        copy_to(con, df = _, "points", temporary = FALSE, indexes = c("lng", "lat")) # Indexation pour accélérer les jointures

    Durée sur notre machine (pour 50 millions de lignes): ~10 secondes

# Traitement

## Filtre grossier: distance haversine

On commence avec un premier filtre grossier sur la base d’une distance
Haversine. La distance Haversine corresponds à la distance à vol
d’oiseau entre deux points sur la surface d’une sphère.

Créons notre fonction `haversine` en SQL:

    CREATE OR REPLACE FUNCTION haversine(lng1, lat1, lng2, lat2) 
      AS 6378137 * acos( 
        cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lng2) - radians(lng1)) +
        sin(radians(lat1)) * sin(radians(lat2))
      );

Nous pouvons maintenant appeller cette fonction depuis R grace à
`dbplyr`, dans le contexte d’une opération de jointure.

Ici, pour chaque paire d’habitant-station possible, seules les paires
étant à une distance de moins de 500m à vol d’oiseau sont conservées.
Dans cet exemple, cela corresponds à 40 milliards de paires.

    sql_join_clause <- "haversine(LHS.lng, LHS.lat, RHS.longitude, RHS.latitude) <= 500"

    inner_join(tbl(con, "points"), tbl(con, "stations"), sql_on = sql_join_clause) |> 
        compute("rides_stations")

    ## # Source:   table<rides_stations> [?? x 7]
    ## # Database: DuckDB v0.10.2 [unknown@Linux 6.1.0-21-amd64:R 4.2.3/:memory:]
    ##    id                             lng   lat stop_id stop_name latitude longitude
    ##    <chr>                        <dbl> <dbl> <chr>   <chr>        <dbl>     <dbl>
    ##  1 c92ed823-b66a-4f01-83b8-05… -0.294  49.2 COM_rv… Accueil …     49.2    -0.299
    ##  2 c92ed823-b66a-4f01-83b8-05… -0.294  49.2 rvi01   Accueil …     49.2    -0.299
    ##  3 2de14c7b-8ba1-4535-a326-49… -0.352  49.2 lac02   Lac           49.2    -0.348
    ##  4 2de14c7b-8ba1-4535-a326-49… -0.352  49.2 COM_la… Lac           49.2    -0.348
    ##  5 2de14c7b-8ba1-4535-a326-49… -0.352  49.2 lac01   Lac           49.2    -0.348
    ##  6 5c1bdcdb-171c-4a3e-a0f1-e2… -0.522  49.2 cbou03  Cheux Bo…     49.2    -0.525
    ##  7 5c1bdcdb-171c-4a3e-a0f1-e2… -0.522  49.2 COM_cb… Cheux Bo…     49.2    -0.525
    ##  8 5c1bdcdb-171c-4a3e-a0f1-e2… -0.522  49.2 cbou01  Cheux Bo…     49.2    -0.525
    ##  9 fed88bf3-1903-499f-80b2-fd… -0.476  49.2 COM_ec… Rots Eco…     49.2    -0.477
    ## 10 fed88bf3-1903-499f-80b2-fd… -0.476  49.2 rotb01  Rots Bon…     49.2    -0.476
    ## # ℹ more rows

    Durée sur notre machine (pour 40 milliards de calculs de distance): ~3-4 minutes

Une fois ce premier filtre effectué, il nous reste environ 150 mille
paires pour lesquelles il sera nécessaire de calculer une distance à
pieds via OSRM. Ce nombre variera d’une exécution à l’autre, en fonction
de la distribution des points aléatoires.

## Filtre précis: distance à pieds

Pour chaque habitant, nous allons calculer la distance à pieds aux
stations qui sont à moins de 500m à vol d’oiseau. N.B.: Il peut y avoir
plusieurs stations à moins de 500m d’un habitant donné.

    get_table_distance <- function(src_df, dst_df) {
        osrm::osrmTable(
            first(src_df), # Within a group, the origin coordinates are the same on each row, so we only need one
            dst_df,
            measure = "distance",
            osrm.server = "http://127.0.0.1:5000/", # Replace this by your own OSRM server
            osrm.profile = "foot"
        )$distances[1, ]
    }

    tbl(con, "rides_stations") |>
        collect() |>
        group_split(id) |>
        furrr::future_map_dfr(\(x) mutate(x, dist_pieds = get_table_distance(pick(lng, lat), pick(longitude, latitude)))) |>
        copy_to(con, df = _, "rides_stations_dist", temporary = FALSE)

    Durée sur notre machine (pour 150 milles calculs de distances): ~30 secondes

Enfin, nous filtrons les paires habitant-stations qui étaient à plus de
500m à pieds:

    tbl(con, "rides_stations_dist") |> 
        filter(dist_pieds <= 500) |> 
        collect()

    ## # A tibble: 57,996 × 8
    ##    id                  lng   lat stop_id stop_name latitude longitude dist_pieds
    ##    <chr>             <dbl> <dbl> <chr>   <chr>        <dbl>     <dbl>      <dbl>
    ##  1 00150dfa-e11c-4… -0.297  49.1 taso02  Tageret       49.1    -0.297        472
    ##  2 00150dfa-e11c-4… -0.297  49.1 ltou02  Les Tour…     49.1    -0.300        343
    ##  3 00150dfa-e11c-4… -0.297  49.1 COM_lt… Les Tour…     49.1    -0.300        345
    ##  4 00150dfa-e11c-4… -0.297  49.1 ARCOM@… Tageret       49.1    -0.297        476
    ##  5 00150dfa-e11c-4… -0.297  49.1 ltou01  Les Tour…     49.1    -0.300        347
    ##  6 002ef466-ca69-4… -0.543  49.2 mepa02  Mesnil P…     49.2    -0.546        319
    ##  7 002ef466-ca69-4… -0.543  49.2 COM_me… Mesnil P…     49.2    -0.546        309
    ##  8 0033fadb-fc0c-4… -0.350  49.2 COM_ca… Cavelier      49.2    -0.351        333
    ##  9 0033fadb-fc0c-4… -0.350  49.2 COM_pi… Pierre H…     49.2    -0.347        299
    ## 10 0033fadb-fc0c-4… -0.350  49.2 COM_ph… Pierre H…     49.2    -0.347        239
    ## # ℹ 57,986 more rows

De nos 50 millions d’habitants, environ 60 milles sont à moins de 500m
d’une station de bus.

# Conclusion

Au total, nous avons traité environ 40 milliards de paires
d’habitant-station en moins de 5 minutes (la durée varie entre 3 et 5
minutes), en utilisant une combinaison de `arrow` et `duckdb` pour
charger les données et effectuer un premier filtre grossier à vol
d’oiseau, suivi de OSRM (parallélisé avec `furrr`) pour le calcul final
de 150 milles distances à pieds.

    dbDisconnect(con, shutdown = TRUE)
