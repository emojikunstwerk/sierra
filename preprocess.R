# r-pm/

dir = "/Users/ryan/Documents/Projects/radio\ histogram/sierra/code/"
in_csv_f  = "sierra-observations.csv"


# note that the different output file variants to have slightly differing schema

# summed by station
run = function(out_csv_f = "sierra-prep.csv") {
    in_csv  = load()
    out_csv = preprocess(in_csv)
    write_csv(out_csv, str_glue("{dir}{out_csv_f}"))
}

# summed by elevation bin
run_elev = function(out_csv_f = "sierra-prep-elev.csv") {
    in_csv  = load()
    out_csv = elev_summ(in_csv)
    write_csv(out_csv, str_glue("{dir}{out_csv_f}"))

}

load = function() {
    return( read_csv(str_glue("{dir}{in_csv_f}")) )
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
        ) |>
        arrange(year, desc(elevation_ft))

    return(out_csv)
}

elev_summ = function(in_csv) {

    out_csv = preprocess(in_csv) |>
        mutate(
            elevation_group = cut(
                elevation_ft,
                include.lowest = TRUE,
                breaks = 18,    # arbitrary, eyeballed interesting
                labels = FALSE  # integer names (DESCENDING from high-to-low)
            )
        ) |>
        group_by(year, longitude_group, elevation_group) |>
        summarize(
            water_mm_sum  = sum(water_mm_sum),
            temp_c_mu     = mean(temp_c_med),
            temp_c_max    = max(temp_c_max),
            elevation_ft  = max(elevation_ft)   # score "clock" triggers a bin @ upper bound elev
        ) |>
        ungroup() |>
        arrange(year, desc(elevation_group))

    return(out_csv)
}

plot_stash = function() {
    ggplot(d1, aes(water_mm_sum, elevation_ft)) + geom_point(aes(alpha = 0.3, size = water_mm_sum, color = temp_c_max)) + facet_grid(cols = vars(longitude_group), rows = vars(year))
    
    ggplot(dg, aes(water_mm_sum, as.numeric(elevation_group))) + geom_point(aes(alpha = 0.3, size = water_mm_sum, color = temp_c_max)) + facet_grid(cols = vars(longitude_group), rows = vars(year))
    
    ggplot(dg, aes(water_mm_sum, as.numeric(elevation_group))) + geom_point(aes(alpha = 0.3, size = water_mm_sum, color = temp_c_max)) + facet_grid(cols = vars(longitude_group), rows = vars(year)) + scale_x_log(labels = comma)
}
