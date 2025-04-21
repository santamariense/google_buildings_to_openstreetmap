import psycopg2

def generate_geoc_mun_file(geoc_mun, blocks_in_mun, distance_threshold_meters, file_path):
    # Open a new file with the name make_geoc_mun where geoc_mun is the variable passed to the function
    file_name = f'{file_path}/make_geoc_mun_{geoc_mun}.sql'

    with open(file_name, 'w') as file:
        # Write the initial static content to the file
        file.write(f"-- File maker | geocodigo = {geoc_mun}\n\n")
        file.write("-- Create schema if it does not exist\n")
        file.write("CREATE SCHEMA IF NOT EXISTS chained_buildings_by_block;\n")
        file.write("CREATE SCHEMA IF NOT EXISTS buildings_by_municipality;\n\n")

        # Loop through blocks_in_mun array to create CALL statements
        #block_counter = 0
        for block in blocks_in_mun:
            file.write(f"CALL validate_buildings_by_block({block}, {distance_threshold_meters}); COMMIT;\n")
            #block_counter = block_counter + 1
            #file.write(f"CALL validate_buildings_by_block({block}, {distance_threshold_meters});")
            #if (block_counter % 100 == 0) or block == blocks_in_mun[len(blocks_in_mun)-1]:
            #    file.write(" COMMIT;")
            #file.write("\n")

        file.write("\n-- Unite all municipality blocks\n")
        file.write(f"DROP TABLE IF EXISTS buildings_by_municipality.mun_{geoc_mun};\n")
        file.write(f"CREATE TABLE buildings_by_municipality.mun_{geoc_mun} AS\n")
        file.write("   SELECT 'yes' as building, geometry FROM (\n")

        # Loop through blocks_in_mun array to create UNION ALL statements
        for i, block in enumerate(blocks_in_mun):
            union_all = "UNION ALL " if i < len(blocks_in_mun) - 1 else ""
            file.write(f"      SELECT geometry FROM chained_buildings_by_block.block_id_{block} {union_all}\n")

        file.write("   );\n")
        file.write("DROP SCHEMA IF EXISTS chained_buildings_by_block CASCADE;\n")

# Connect to the database
conn = psycopg2.connect(
    dbname="google_buildings",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Fetch geoc_mun and blocks_in_mun from the blocks_br table
cur.execute("SELECT geocodigo, Array_agg(id) as blocks_list FROM public.blocks_br GROUP by geocodigo;")

# Iterate through the fetched rows and call the function
for row in cur.fetchall():
    geoc_mun, blocks_in_mun = row
    generate_geoc_mun_file(geoc_mun, blocks_in_mun, 0.8, '/path/to/google_buildings/makers_for_each_municipality')

# Close cursor and connection
cur.close()
conn.close()


