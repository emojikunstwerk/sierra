# r-pm/

# - (station, year)
    # sum water     [ 13 of 54 stations lack all 60 (12x5) measurements ]
    # median temp   [ med. of station_med_dist (all years) is 3.5C (!) ]
# - (long, elevation) bin
    # max sum_water
    # mean temp
# - recompute longitude_groups (? high-elev toward middle groups, retain low elev. in rightmost)
# - normalized version of water_mm (log?)


in_csv_path  = "/Users/ryan/Documents/Projects/radio\ histogram/sierra/sierra-observations.csv"
out_csv_path = "/Users/ryan/Documents/Projects/radio\ histogram/sierra/sierra-prep.csv"

load = function() {
    return( read_csv(in_csv_path) )
}

preprocess = function(in_csv) {

    out_csv = in_csv |> 
        mutate(
            year            = year(date),
            elevation_ft    = trunc(elevation_ft),

            water_mm = if_else(
                is.na(precipitation_mm)  & !is.na(snow_water_equiv_mm), snow_water_equiv_mm, if_else(
                !is.na(precipitation_mm) &  is.na(snow_water_equiv_mm), precipitation_mm,    if_else(            
                is.na(precipitation_mm)  &  is.na(snow_water_equiv_mm), NA_real_,   
                snow_water_equiv_mm + precipitation_mm
            )))
        ) |>

        filter( 
            !is.na(water_mm) &
            !is.na(air_temp_obs_c) &        # after the first condition, false 0s in temp are already removed
            !(station_name == "Lake Tahoe Elev Adj - Scale = .001 Ft")
        ) |>

        group_by(year, station_name, elevation_ft, longitude) |>   # note: spatial cols fixed across station records (grouped just to pass thru)
        summarize(
            water_mm_sum = sum(water_mm, na.rm = TRUE),
            temp_c_med   = median(air_temp_obs_c, na.rm = TRUE),
            temp_c_max   = max(air_temp_obs_c, na.rm = TRUE)
        ) |> 
        ungroup() |>

        mutate(
            longitude_group = cut(
                longitude,
                include.lowest = TRUE,
                breaks = quantile(longitude, probs = seq(0, 1, 0.25)),
                labels = FALSE  # integer names (DESCENDING from east-to-west)
            )

            # for exploration, not score
            # elevation_group = cut(
            #     elevation_ft,
            #     include.lowest = TRUE,
            #     breaks = 18,    # arbitrary, eyeballed interesting
            #     labels = FALSE  # integer names (DESCENDING from high-to-low)
            # )
        ) |>
        arrange(year, desc(elevation_ft))

    return(out_csv)
}

run = function() {
    in_csv  = load()
    out_csv = preprocess(in_csv)
    write_csv(out_csv, out_csv_path)
}

# ggplot(d1, aes(water_mm_sum, elevation_ft)) + geom_point(aes(alpha = 0.3, size = water_mm_sum, color = temp_c_max)) + facet_grid(cols = vars(longitude_group), rows = vars(year))
