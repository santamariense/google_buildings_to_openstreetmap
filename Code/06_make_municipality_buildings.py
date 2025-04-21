import os
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 06_make_municipality.py <geocode>")
        sys.exit(1)

    geocode = sys.argv[1]

    # Paths
    sql_path = '/path/to/google_buildings/makers_for_each_municipality/'
    geojson_path = '/path/to/google_buildings/final_buildings_for_each_municipality/'

    # Run the SQL script with the geocode argument
    sql_command = f'psql -U postgres -d google_buildings -f {sql_path}make_geoc_mun_{geocode}.sql'
    os.system(sql_command)

    # Run the ogr2ogr command
    geojson_file = f'mun_{geocode}.geojson'
    ogr2ogr_command = f'ogr2ogr -f "GeoJSON" {geojson_path}{geojson_file} PG:"host=localhost dbname=google_buildings user=postgres password=postgres" "buildings_by_municipality.mun_{geocode}"'
    os.system(ogr2ogr_command)

    # Change directory to the location of the GeoJSON file
    os.chdir(geojson_path)

    # Zip the generated GeoJSON file
    zip_command = f'zip mun_{geocode}.zip {geojson_file}'
    os.system(zip_command)

    # Delete the original GeoJSON file
    delete_command = f'rm {geojson_file}'
    os.system(delete_command)

if __name__ == "__main__":
    main()

