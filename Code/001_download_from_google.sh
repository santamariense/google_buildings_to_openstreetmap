#!/bin/bash

# Create a folder named 'originals' if it doesn't exist
mkdir -p originals

# List of URLs to download
urls=(
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/009_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/00b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/00d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/013_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/063_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/071_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/073_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/075_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/077_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/079_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/07b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/07d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/07f_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8d5_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8d7_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8d9_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8db_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8dd_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8df_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8e1_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/8e3_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/917_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/919_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/91b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/91d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/91f_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/921_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/923_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/925_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/927_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/929_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/92b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/92d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/92f_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/931_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/933_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/935_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/937_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/939_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/93b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/93d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/945_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/947_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/949_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/94b_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/94d_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/94f_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/951_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/953_buildings.csv.gz"
    "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/95b_buildings.csv.gz"
)

# Loop to download each file
while true; do
    for url in "${urls[@]}"; do
        # Download each file to the 'originals' folder and show progress
        wget -c "$url" -P originals/ --progress=bar:force
        echo "Downloaded: $url"
        sleep 60
    done
done

