
# Generate random points within a country's polygon
# Output the results as parquet files in the provided `output_dir`
generate_random_points <- function(country = "France", n_batches = 100, batch_size = 5e5, output_dir) {
    country_poly <- spData::world |> 
        as.data.frame() |> 
        dplyr::filter(name_long == country) |> 
        sf::st_as_sf(sf_column_name = "geom")
    
    purrr::walk(
        1:n_batches,
        function(x) {
            sf::st_sample(country_poly, size = batch_size, type = "random", crs = sf::st_crs(4326)) |> 
                sf::st_coordinates() |>
                as.data.frame() |>
                dplyr::mutate(id = uuid::UUIDgenerate(n = n())) |> 
                arrow::write_parquet(here::here(output_dir, paste0("points_", x, ".parquet")))
        },
        .progress = TRUE
    )
    
}
