-- #############################################################################
-- ## Analytics Engineer Technical Assessment (Part 2a: Production Analytics) ##
-- #############################################################################

-- #############################################################################
-- ## Section 0: Schema Setup
-- #############################################################################

-- Create the analytics schema if it doesn't already exist. This will house
-- all the transformed views and models.
CREATE SCHEMA IF NOT EXISTS analytics;

-- #############################################################################
-- ## Section 1: View Creation
-- #############################################################################

-- =============================================================================
-- View 1: well_attributes
-- Purpose: This view consolidates well header data across mulitple ProCount and WellView tables
-- Sources:
--     completiontb 
--         Description: ProCount completion header (csv)
--         Columns: merrickid, wellname, apiwellnumber, ariesid, wellviewid
--     areatb
--         Description: ProCount area lookup table (csv)
--         Columns: areaname
--     batterytb
--         Description: ProCount battery lookup table (csv)
--         Columns: batteryname
--     wellview_wellheader
--         Description: WellView well header (api)
--         Columns: latitude, longitude
-- =============================================================================
CREATE OR REPLACE VIEW analytics.well_attributes AS
SELECT
    ct.merrickid AS id,            -- Primary key for ProCount tables
    ct.wellname AS well_name,
    ct.apiwellnumber AS api,
    at.areaname AS area,
    bt.batteryname AS battery,
    wvh.latitude,
    wvh.longitude,    
    ct.ariesid AS aries_id,        -- Foreign key for Aries tables
    ct.wellviewid AS wellview_id   -- Foreign key for WellView tables
FROM
    stg.completiontb ct
    LEFT JOIN stg.areatb at 
        ON ct.areaid = at.areamerrickid
    LEFT JOIN stg.batterytb bt 
        ON ct.batteryid = bt.batterymerrickid
    LEFT JOIN stg.wellview_wellheader wvh
        ON ct.wellviewid = wvh.idwell;

-- =============================================================================
-- View 2: daily_series
-- Purpose: This view combines daily production and capicity time series tables
-- Sources:
--     procount_completiondailytb
--         Description: ProCount daily production and pressure (api)
--         Columns: merrickid, productiondate, dailydowntime, casingpressure, 
--                  tubingpressure, chokesize, oilproduction, allocestgasvolmcf,
--                  waterproduction
--     aries_daily_capacities
--         Description: Aries daily capacity volumes (api)
--         Columns: oil, gas, water
-- =============================================================================
CREATE OR REPLACE VIEW analytics.daily_series AS
SELECT
    pcd.merrickid AS well_id,
    pcd.productiondate AS "date",
    pcd.dailydowntime AS downtime_hours,
    pcd.casingpressure AS casing_pressure,
    pcd.tubingpressure AS tubing_pressure,
    pcd.chokesize AS choke_size,
    pcd.oilproduction AS oil,
    pcd.allocestgasvolmcf AS gas,
    pcd.waterproduction AS water,
    adc.oil AS oil_capacity,
    adc.gas AS gas_capacity,
    adc.water AS water_capacity
FROM
    stg.procount_completiondailytb pcd
    -- Join to the ProCount completion header to obtain Aries ID
    LEFT JOIN stg.completiontb ct 
        ON pcd.merrickid = ct.merrickid
    LEFT JOIN stg.aries_daily_capacities adc 
        ON ct.ariesid = adc.well_id
            AND pcd.productiondate = adc.date;

-- =============================================================================
-- View 3: well_summary
-- Aggregates production data to provide a summary for each well, including
-- lifetime totals, yearly averages, and cumulative production over the first
-- six months.
-- =============================================================================
CREATE OR REPLACE VIEW analytics.well_summary AS
WITH daily_series_with_prod_rank AS (
    -- First, we need to rank the production days for each well to identify
    -- the first six months of production
    SELECT
        well_id,
        "date",
        oil,
        gas,
        water,
        -- Use a window function to find the first production date for each well
        MIN("date") OVER (PARTITION BY well_id) AS first_prod_date
    FROM
        analytics.daily_series
),
six_month_cumulative AS (
    -- Calculate the cumulative production for the first 6 months (180 days)
    SELECT
        well_id,
        SUM(COALESCE(oil, 0)) AS six_month_cumulative_oil,
        SUM(COALESCE(gas, 0)) AS six_month_cumulative_gas,
        SUM(COALESCE(water, 0)) AS six_month_cumulative_water
    FROM
        daily_series_with_prod_rank
    WHERE
        -- Filter for records within the first 180 days of production
        "date" <= first_prod_date + INTERVAL '180 days'
    GROUP BY
        well_id
),
well_jobs_2023 AS (
    -- Count the number of well work jobs that occurred in 2023
    SELECT
        wa.id AS well_id, -- Using ProCount merrickid as the well_id
        COUNT(wj.idrec) AS job_count_2023
    FROM
        analytics.well_attributes wa
    JOIN
        -- Job data from WellView
        stg.wellview_job wj ON wa.wellview_id = wj.idwell -- Joining on WellView ID
    WHERE
        -- Filter for jobs that started in 2023
        EXTRACT(YEAR FROM wj.dttmstart) = 2023
    GROUP BY
        wa.id
)
SELECT
    ds.well_id,
    SUM(COALESCE(ds.oil, 0)) AS total_oil,
    SUM(COALESCE(ds.gas, 0)) AS total_gas,
    SUM(COALESCE(ds.water, 0)) AS total_water,
    AVG(ds.oil) FILTER (WHERE EXTRACT(YEAR FROM ds."date") = 2022) AS avg_2022_oil_rate,
    AVG(ds.oil) FILTER (WHERE EXTRACT(YEAR FROM ds."date") = 2023) AS avg_2023_oil_rate,
    -- Six-month cumulative production values
    smc.six_month_cumulative_oil,
    smc.six_month_cumulative_gas,
    smc.six_month_cumulative_water,
    COALESCE(wj.job_count_2023, 0) AS count_of_well_work_jobs_2023
FROM
    analytics.daily_series ds
LEFT JOIN
    six_month_cumulative smc ON ds.well_id = smc.well_id
LEFT JOIN
    well_jobs_2023 wj ON ds.well_id = wj.well_id
GROUP BY
    ds.well_id,
    smc.six_month_cumulative_oil,
    smc.six_month_cumulative_gas,
    smc.six_month_cumulative_water,
    wj.job_count_2023;

-- =============================================================================
-- View 4: well_spacing
-- Performs advanced spatial analysis to determine the spacing between wells.
-- This involves creating well trajectories and calculating distances and
-- directions between neighboring wells. The calculations here are spatial and
-- less prone to NULL issues, as rows with NULL coordinates are filtered out.
-- =============================================================================
CREATE OR REPLACE VIEW analytics.well_spacing AS
WITH well_trajectories AS (
    -- Step 1: Transform survey points into line geometries for each well
    SELECT
        s.well_id,
        ST_MakeLine(ST_MakePoint(s.easting_offset_ft, s.northing_offset_ft, s.tvd_offset_ft) ORDER BY s.measured_depth_ft) AS wellbore_trajectory_cartesian,
        ST_MakeLine(ST_MakePoint(s.easting_offset_ft, s.northing_offset_ft, s.tvd_offset_ft) ORDER BY s.measured_depth_ft) FILTER (WHERE s.inclination_deg > 80) AS lateral_trajectory_cartesian,
        AVG(s.tvd_offset_ft) FILTER (WHERE s.inclination_deg > 80) AS avg_lateral_tvd,
        h.latitude AS surface_latitude,
        h.longitude AS surface_longitude
    FROM
        stg.wellview_surveypoint s
    JOIN
        stg.wellview_wellheader h ON s.well_id = h.idwell
    GROUP BY
        s.well_id, h.latitude, h.longitude
),
projected_trajectories AS (
    -- Step 2: Project the cartesian trajectories to the WGS84 coordinate system (lat/lon)
    SELECT
        well_id,
        avg_lateral_tvd,
        ST_SetSRID(
            ST_Translate(
                ST_Scale(
                    ST_Force2D(lateral_trajectory_cartesian),
                    1 / (cos(radians(surface_latitude)) * 364567.2),
                    1 / 364567.2
                ),
                surface_longitude,
                surface_latitude
            ), 4326
        )::geography AS lateral_trajectory_geography,
        ST_Centroid(lateral_trajectory_cartesian) AS lateral_centroid_cartesian
    FROM
        well_trajectories
    WHERE
        lateral_trajectory_cartesian IS NOT NULL
),
well_pairs AS (
    -- Step 3: For each well, find all other wells within a 1500-meter radius
    SELECT
        p1.well_id AS subject_well_id,
        p2.well_id AS offset_well_id,
        ST_Distance(p1.lateral_trajectory_geography, p2.lateral_trajectory_geography) * 3.28084 AS distance_ft,
        p1.avg_lateral_tvd - p2.avg_lateral_tvd AS tvd_diff_ft,
        atan2(
            ST_Y(p2.lateral_centroid_cartesian) - ST_Y(p1.lateral_centroid_cartesian),
            ST_X(p2.lateral_centroid_cartesian) - ST_X(p1.lateral_centroid_cartesian)
        ) * 180 / pi() AS azimuth_degrees
    FROM
        projected_trajectories p1
    JOIN
        projected_trajectories p2 ON p1.well_id <> p2.well_id
        AND ST_DWithin(p1.lateral_trajectory_geography, p2.lateral_trajectory_geography, 1500)
),
ranked_neighbors AS (
    -- Step 4: Rank the neighbors for each well by distance to find the nearest ones.
    SELECT
        subject_well_id,
        offset_well_id,
        distance_ft,
        tvd_diff_ft,
        CASE
            WHEN azimuth_degrees BETWEEN -90 AND 90 THEN 'East'
            ELSE 'West'
        END AS offset_direction,
        ROW_NUMBER() OVER(PARTITION BY subject_well_id,
            CASE WHEN azimuth_degrees BETWEEN -90 AND 90 THEN 'East' ELSE 'West' END
            ORDER BY distance_ft ASC) as rn
    FROM
        well_pairs
    WHERE
        tvd_diff_ft BETWEEN -500 AND 100
)
-- Step 5: Pivot the results to get the final view with one row per well
SELECT
    subject_well_id AS well_id,
    MAX(offset_well_id) FILTER (WHERE offset_direction = 'East' AND rn = 1) AS east_offset_well_id,
    MAX(distance_ft) FILTER (WHERE offset_direction = 'East' AND rn = 1) AS east_spacing_ft,
    MAX(tvd_diff_ft) FILTER (WHERE offset_direction = 'East' AND rn = 1) AS east_vertical_diff_ft,
    MAX(offset_well_id) FILTER (WHERE offset_direction = 'West' AND rn = 1) AS west_offset_well_id,
    MAX(distance_ft) FILTER (WHERE offset_direction = 'West' AND rn = 1) AS west_spacing_ft,
    MAX(tvd_diff_ft) FILTER (WHERE offset_direction = 'West' AND rn = 1) AS west_vertical_diff_ft
FROM
    ranked_neighbors
GROUP BY
    subject_well_id;


-- #############################################################################
-- ## Section 2: Analytical Queries
-- #############################################################################

-- =============================================================================
-- Query 1: Well Production Summary for "Field 1"
-- Calculates the total gross and net oil production for "Field 1" in 2022.
-- =============================================================================
SELECT
    'Field 1' AS field_name,
    SUM(COALESCE(ds.oil, 0)) AS total_gross_oil_2022,
    SUM(COALESCE(ds.oil, 0) * acp.nri) AS total_net_oil_2022
FROM
    analytics.daily_series ds
JOIN
    analytics.well_attributes wa ON ds.well_id = wa.id
JOIN
    stg.wellview_wellheader wvh ON wa.wellview_id = wvh.idwell
JOIN
    stg.ac_property acp ON wa.aries_id = acp.propnum
WHERE
    wvh.fieldname = 'Field 1'
    AND EXTRACT(YEAR FROM ds."date") = 2022;

-- =============================================================================
-- Query 2: Tubing Cleanout Losses
-- Calculates the production losses incurred during "Tubing Cleanout" jobs.
-- =============================================================================
WITH cleanout_jobs AS (
    -- First, identify all "Tubing Cleanout" jobs and their durations
    SELECT
        wa.id AS well_id,
        wa.aries_id,
        wj.dttmstart,
        wj.dttmend
    FROM
        analytics.well_attributes wa
    JOIN
        stg.wellview_job wj ON wa.wellview_id = wj.idwell
    WHERE
        wj.jobsubtyp = 'Tubing Cleanout'
)
SELECT
    cj.well_id,
    cj.dttmstart,
    cj.dttmend,
    SUM(COALESCE(ds.oil, 0)) AS total_oil_during_event,
    SUM(COALESCE(ds.oil_capacity, 0)) AS total_oil_capacity_during_event,
    -- Gross loss is the total actual production minus the total potential production (capacity)
    SUM(COALESCE(ds.oil, 0)) - SUM(COALESCE(ds.oil_capacity, 0)) AS gross_loss_oil,
    -- Net loss applies the NRI to the gross loss.
    (SUM(COALESCE(ds.oil, 0)) - SUM(COALESCE(ds.oil_capacity, 0))) * MAX(acp.nri) AS net_loss_oil
FROM
    cleanout_jobs cj
JOIN
    analytics.daily_series ds ON cj.well_id = ds.well_id
    AND ds."date" BETWEEN cj.dttmstart::date AND cj.dttmend::date
JOIN
    stg.ac_property acp ON cj.aries_id = acp.propnum
GROUP BY
    cj.well_id, cj.dttmstart, cj.dttmend
ORDER BY
    net_loss_oil ASC;

-- =============================================================================
-- Query 3: Spacing vs. Production Performance
-- A series of queries to explore the relationship between well spacing,
-- geography, and production.
-- =============================================================================

-- 3a: Which county has the most and least tightly spaced wells?
SELECT
    wvh.county,
    AVG(
        CASE
            WHEN ws.east_spacing_ft IS NOT NULL AND ws.west_spacing_ft IS NOT NULL THEN (ws.east_spacing_ft + ws.west_spacing_ft) / 2.0
            WHEN ws.east_spacing_ft IS NOT NULL THEN ws.east_spacing_ft
            WHEN ws.west_spacing_ft IS NOT NULL THEN ws.west_spacing_ft
            ELSE NULL
        END
    ) AS avg_spacing_ft
FROM
    analytics.well_spacing ws
JOIN
    analytics.well_attributes wa ON ws.well_id = wa.wellview_id
JOIN
    stg.wellview_wellheader wvh ON ws.well_id = wvh.idwell
WHERE
    ws.east_spacing_ft IS NOT NULL OR ws.west_spacing_ft IS NOT NULL
GROUP BY
    wvh.county
ORDER BY
    avg_spacing_ft ASC;

-- 3b: How does the formation dip based on changes in TVD?
SELECT
    'East' AS direction,
    AVG(east_vertical_diff_ft) AS avg_vertical_diff_ft
FROM analytics.well_spacing
WHERE east_vertical_diff_ft IS NOT NULL
UNION ALL
SELECT
    'West' AS direction,
    AVG(west_vertical_diff_ft) AS avg_vertical_diff_ft
FROM analytics.well_spacing
WHERE west_vertical_diff_ft IS NOT NULL;

-- 3c: What is the correlation between East/West spacing and 6-month cumulative oil production?
SELECT
    CORR(ws.east_spacing_ft, sm.six_month_cumulative_oil) AS correlation_east_spacing_vs_oil,
    CORR(ws.west_spacing_ft, sm.six_month_cumulative_oil) AS correlation_west_spacing_vs_oil
FROM
    analytics.well_spacing ws
JOIN
    analytics.well_attributes wa ON ws.well_id = wa.wellview_id
JOIN
    analytics.well_summary sm ON wa.id = sm.well_id;

