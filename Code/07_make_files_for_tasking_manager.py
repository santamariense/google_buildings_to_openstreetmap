import datetime
import os
import sys
import psycopg2
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
from pyproj import CRS
import numpy as np
import json
from shapely.geometry import Point, shape, Polygon, mapping
import traceback

################# CONFIGURATION BEGIN #################

tm_path_root = '/path/to/google_buildings/files_for_tasking_manager/'

################# CONFIGURATION END   ################# 

def list_tables_in_for_tasking_manager():
    schema_name = "for_tasking_manager"
    dbname = "google_buildings"
    user = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"

    try:
        conn_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema_name,),
        )

        table_names = [row[0] for row in cur.fetchall()]
        return table_names

    except psycopg2.Error as e:
        print(f"Error listing tables: {e}")
        return None

    finally:
        if conn:
            cur.close()
            conn.close()
            
            
def make_buildings_block_by_block(mungeocode,distance_threshold_meters):
    """
    Processes buildings block by block, handling database locks by committing after each block.
    """
    schema_name = "for_tasking_manager"
    dbname = "google_buildings"
    user = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"
    
    db_config = {
        'dbname': dbname,
        'user': user,
        'password': password,
        'host': host,
        'port': port
    }
    
    try:
        # Establish database connection
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Create schema if not exists
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()

        # Drop and create municipality-specific table
        cur.execute(f"""
            DROP TABLE IF EXISTS {schema_name}.municipality_{mungeocode};
            CREATE TABLE {schema_name}.municipality_{mungeocode} AS
            SELECT bu.*, bl.geom 
            FROM (
                SELECT count(id) as building_counter, mun_block as block_id
                FROM public.brazil_04
                WHERE geoc_mun = %s
                GROUP BY mun_block
                ORDER BY building_counter DESC
            ) AS bu 
            LEFT JOIN public.blocks_br AS bl ON bu.block_id = bl.id
            WHERE bu.building_counter > 0;
        """, (mungeocode,))
        conn.commit()

        # Retrieve list of blocks with building count
        cur.execute(f"""
            SELECT block_id, building_counter FROM {schema_name}.municipality_{mungeocode}
        """)
        municipality_blocks = cur.fetchall()
        
        if not municipality_blocks:
            print("No blocks found for the municipality.")
            return
        
        block_counter = len(municipality_blocks)
        cur.close()
        conn.close()
        
        # Process each block with a new connection to avoid lock issues
        for loop_counter, (municipality_block, building_count) in enumerate(municipality_blocks, 1):
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Adjusting buildings for block {municipality_block} ({loop_counter}/{block_counter}) - {building_count} buildings")
        
            try:
                conn = psycopg2.connect(**db_config)
                cur = conn.cursor()
        
                # Validate buildings block by block
                cur.execute("CALL validate_buildings_by_block(%s, %s);", (municipality_block, distance_threshold_meters))
                conn.commit()
        
                # Move block to final schema
                cur.execute(f"""
                    DROP TABLE IF EXISTS {schema_name}.b{municipality_block};
                    CREATE TABLE {schema_name}.b{municipality_block} AS 
                    SELECT 'yes' as building, geometry as geom 
                    FROM chained_buildings_by_block.block_id_{municipality_block};
                    DROP TABLE IF EXISTS chained_buildings_by_block.block_id_{municipality_block};
                """)
                conn.commit()
        
                cur.close()
                conn.close()
        
                # Make geojson for each block without accumulating
                ogr2ogr(mungeocode, maxbuildings=170)
        
            except Exception as e:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] :: Error processing block {municipality_block}")
                traceback.print_exc()  # This prints the full stack trace
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals():
                    conn.close()
                continue


        # Drop temporary schema with a new connection
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS chained_buildings_by_block CASCADE;")
        conn.commit()
        cur.close()
        conn.close()

        print("Processing complete.")

    except Exception as e:
        print(f"Error: {e}")
def drop_temp_tables():
    dbname = "google_buildings"
    user = "postgres"
    password = "postgres"
    host = "localhost"
    port = "5432"

    try:
        # Establishing the connection
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        cur = conn.cursor()

        # SQL command to drop temporary tables in the 'for_tasking_manager' schema
        sql_command = """
        DO $$ 
        DECLARE r RECORD; 
        BEGIN 
            FOR r IN (
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'for_tasking_manager' AND tablename LIKE 'b%'
            ) 
            LOOP 
                EXECUTE 'DROP TABLE IF EXISTS for_tasking_manager.' || r.tablename || ' CASCADE;';
            END LOOP; 
        END $$;
        """

        # Execute the command
        cur.execute(sql_command)
        conn.commit()

        #print("Temporary tables dropped successfully.")

    except psycopg2.Error as e:
        print(f"Error executing SQL: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def ogr2ogr(geocode, maxbuildings=170):
    table_list = list_tables_in_for_tasking_manager()
    
    geojsons_path = os.path.join(tm_path_root, geocode, 'blocks_geojson')
    geojson_municipality = os.path.join(tm_path_root, geocode)

    if not os.path.exists(geojsons_path):
        os.makedirs(geojsons_path)

    for t in table_list:
        geojson_file = f'{t}.geojson'

        if t.startswith('b'):
            output_path = os.path.join(geojsons_path, geojson_file)
            ogr2ogr_command = f'ogr2ogr -f "GeoJSON" {output_path} PG:"host=localhost dbname=google_buildings user=postgres password=postgres" "for_tasking_manager.{t}"'
            os.system(ogr2ogr_command)
            
            # Automatically cut if needed
            try:
                gdf = gpd.read_file(output_path)
                if len(gdf) > maxbuildings:
                    cut_geojson(output_path, maxbuildings)
            except Exception as e:
                print(f"Error processing/cutting {geojson_file}: {e}")

        elif t.startswith('municipality_'):
            output_path = os.path.join(geojson_municipality, geojson_file)
            if not os.path.exists(output_path):
                ogr2ogr_command = f'ogr2ogr -f "GeoJSON" {output_path} PG:"host=localhost dbname=google_buildings user=postgres password=postgres" "for_tasking_manager.{t}"'
                os.system(ogr2ogr_command)

    drop_temp_tables()

     
#### Phase 02 Begin ####

def cut_geojson(geojson_file, maxbuildings):
    """
    Processes a GeoJSON file by adding IDs, buffering and merging polygons, 
    and organizing them by proximity.

    Parameters:
        geojson_file (str): Path to the input GeoJSON file.
        maxbuildings (int): Maximum number of buildings per output file.
    """
    def add_ids_to_polygons(gdf):
        gdf['id'] = gdf.index
        if 'building' not in gdf.columns:
            gdf['building'] = None  
        return gdf
    
    def buffer_and_merge_polygons(gdf):
        gdf = gdf.to_crs(epsg=3395)
        gdf['buffered'] = gdf.geometry.buffer(2.5)
        merged_geometry = unary_union(gdf['buffered'])

        merged_dict = {}

        if merged_geometry is None:
            return merged_dict

        if hasattr(merged_geometry, 'geoms'):
            geometries = merged_geometry.geoms
        else:
            geometries = [merged_geometry]

        for idx, row in gdf.iterrows():
            for merged in geometries:
                if row['buffered'] is not None and merged is not None and row['buffered'].intersects(merged):
                    merged_dict.setdefault(merged, []).append(row['id'])

        return merged_dict
        
    
    def organize_by_proximity(gdf, merged_dict, output_folder, base_filename):
        gdf = gdf.to_crs(epsg=4326)
        gdf['distance'] = gdf.geometry.apply(lambda geom: geom.centroid.distance(Point(0, 0)))
        
        polygon_ids_in_files = set()
        file_count = 1
        
        while len(polygon_ids_in_files) < len(gdf):
            remaining_polygons = gdf[~gdf['id'].isin(polygon_ids_in_files)]
            if remaining_polygons.empty:
                break
            
            remaining_polygons_sorted = remaining_polygons.sort_values('distance')
            pivot_polygon = remaining_polygons_sorted.iloc[0]
            pivot_id = pivot_polygon['id']
            
            current_file_polygons = [pivot_polygon]
            polygon_ids_in_files.add(pivot_id)
            
            while len(current_file_polygons) < maxbuildings and not remaining_polygons.empty:
                distances = remaining_polygons['geometry'].apply(lambda x: pivot_polygon.geometry.centroid.distance(x.centroid))
                min_distance_index = distances.idxmin()
                closest_polygon = remaining_polygons.loc[min_distance_index]
                closest_id = closest_polygon['id']
                
                merged_geometry = next((geom for geom, ids in merged_dict.items() if closest_id in ids), None)
                
                if merged_geometry:
                    for merged_id in merged_dict[merged_geometry]:
                        if merged_id not in polygon_ids_in_files:
                            current_file_polygons.append(gdf[gdf['id'] == merged_id].iloc[0])
                            polygon_ids_in_files.add(merged_id)
                else:
                    if closest_id not in polygon_ids_in_files:
                        current_file_polygons.append(closest_polygon)
                        polygon_ids_in_files.add(closest_id)
                
                remaining_polygons = gdf[~gdf['id'].isin(polygon_ids_in_files)]
            
            output_file = os.path.join(output_folder, f"{base_filename}_{file_count:04d}.geojson")
            gdf_current_file = gpd.GeoDataFrame(current_file_polygons, columns=['building', 'geometry'])
            gdf_current_file = gdf_current_file.set_crs(epsg=4326, allow_override=True)
            gdf_current_file.to_file(output_file, driver="GeoJSON")
            
            file_count += 1
        
        #print(f"GeoJSON files created: {file_count - 1}")
    
    # Load the GeoJSON file
    gdf = gpd.read_file(geojson_file)
    gdf = add_ids_to_polygons(gdf)
    merged_dict = buffer_and_merge_polygons(gdf)

    # Define output directory and base filename
    file_dir = os.path.dirname(geojson_file)
    base_filename = os.path.splitext(os.path.basename(geojson_file))[0]

    # Process and save output files in the same directory
    organize_by_proximity(gdf, merged_dict, file_dir, base_filename)

    # Delete the original file after processing
    os.remove(geojson_file)
    

def get_overpopulated_blocks(maxbuildings, blocks_geojson_path):
    overpopulated_blocks = []
    
    with open(blocks_geojson_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        
        for feature in data.get("features", []):
            properties = feature.get("properties", {})
            building_counter = properties.get("building_counter", 0)
            block_id = properties.get("block_id")
            
            if building_counter > maxbuildings and block_id is not None:
                overpopulated_blocks.append(block_id)
    
    return overpopulated_blocks

def modify_municipality_geojson(municipality_geojson_path, blocks_directory, maxbuildings):
    new_features = []
    
    for file_name in os.listdir(blocks_directory):
        if "_" in file_name and file_name.endswith(".geojson"):
            file_path = os.path.join(blocks_directory, file_name)
            
            gdf = gpd.read_file(file_path)
            
            if not gdf.empty:
                minx, miny, maxx, maxy = gdf.total_bounds
                
                # Get extreme points
                northernmost = gdf.geometry.bounds.iloc[gdf.geometry.bounds['maxy'].idxmax()][['minx', 'maxy']]
                westernmost = gdf.geometry.bounds.iloc[gdf.geometry.bounds['minx'].idxmin()][['minx', 'miny']]
                southernmost = gdf.geometry.bounds.iloc[gdf.geometry.bounds['miny'].idxmin()][['maxx', 'miny']]
                easternmost = gdf.geometry.bounds.iloc[gdf.geometry.bounds['maxx'].idxmax()][['maxx', 'maxy']]
                
                # Create diamond-shaped polygon
                new_polygon = Polygon([
                    (northernmost["minx"], northernmost["maxy"]),
                    (westernmost["minx"], westernmost["miny"]),
                    (southernmost["maxx"], southernmost["miny"]),
                    (easternmost["maxx"], easternmost["maxy"]),
                    (northernmost["minx"], northernmost["maxy"])  # Closing the polygon
                ])
                
                block_id = file_name.replace(".geojson", "").replace("b", "")
                building_counter = len(gdf)
                
                new_feature = {
                    "type": "Feature",
                    "geometry": mapping(new_polygon),
                    "properties": {
                        "block_id": block_id,
                        "building_counter": building_counter
                    }
                }
                new_features.append(new_feature)
    
    with open(municipality_geojson_path, 'r+', encoding='utf-8') as file:
        data = json.load(file)
        
        # Remove polygons that do not contain "_" in block_id and have building_counter > maxbuildings
        data["features"] = [
            feature for feature in data["features"]
            if "_" in str(feature["properties"]["block_id"]) or feature["properties"]["building_counter"] <= maxbuildings
        ]
        
        # Add new features
        data["features"].extend(new_features)
        
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()


#### Phase 02  End  ####
            
def main():
    if len(sys.argv) != 2:
        print("Usage: python3 07_make_files_for_tasking_manager.py <geocode>")
        sys.exit(1)
    
    geocode = sys.argv[1]

    # Define tm_path after geocode is set
    tm_path = f'/path/to/google_buildings/files_for_tasking_manager/{geocode}/'

    # Paths
    os.system(('rm -rf ' + tm_path)) # Clear files from previous processes
    os.makedirs(tm_path)
    
    # Run the SQL script with the geocode argument
    #sql_command = f'psql -U postgres -d google_buildings -c "CALL make_buildings_block_by_block({geocode}); COMMIT;"'
    #os.system(sql_command)
    make_buildings_block_by_block(geocode,0.8)
    
    #geocode = "4303004"
    maxbuildings = 170
    municipality_geojson_path = "/path/to/google_buildings/files_for_tasking_manager/" + geocode + "/municipality_" + geocode + ".geojson"
    blocks_directory = "/path/to/google_buildings/files_for_tasking_manager/" + geocode +"/blocks_geojson/"
    # Cut overpopulated blocks
    overpopulated_blocks_list = get_overpopulated_blocks(maxbuildings, municipality_geojson_path) # Get the list of overpopulated blocks
    ''' - DEPRECIATED
    for block_id in overpopulated_blocks_list:
       cut_geojson(blocks_directory+'b'+str(block_id)+".geojson", maxbuildings)
       #break
    '''
    #Modify Municipalities' set of blocks
    modify_municipality_geojson(municipality_geojson_path, blocks_directory, maxbuildings)
    
    # Gzip and convert the generated GeoJSONs files
    print(f"gziping geojsons")
    geojsons_path = tm_path_root+geocode+'/blocks_geojson/'
    gz_geojsons_path = tm_path+'blocks_geojson_gz/'
    if not os.path.exists(gz_geojsons_path): os.makedirs(gz_geojsons_path) 
    geojsons_list = os.listdir(geojsons_path)
    for g in geojsons_list:
      gzip_geojson_command = f'gzip --best -c {geojsons_path}{g} > {gz_geojsons_path}{g}.gz'
      os.system(gzip_geojson_command)
      #TODO: Converto to OSM format

    # Delete GeoJSONs files that were gzipped
    delete_command = f'rm -r /path/to/google_buildings/files_for_tasking_manager/{geocode}/blocks_geojson/'
    #os.system(delete_command)
    
    # Run the SQL command to drop temporay schema
    sql_command = f'psql -U postgres -d google_buildings -c "DROP SCHEMA IF EXISTS for_tasking_manager CASCADE;"'
    os.system(sql_command)
    
if __name__ == "__main__":
    main()

