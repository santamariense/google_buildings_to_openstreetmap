# Google Buildings to OpenStreetMap

This project processes and prepares Google Building geometries for use in OpenStreetMap. It identifies and resolves overlapping geometries and merges nearby corner points to simplify and clean building outlines. The final output is formatted for smooth integration into tools like the Tasking Manager, minimizing the need for post-processing.

Note that the code is **functional**, but it has not been reviewed or **optimized**. Even after processing, users are expected to review and adjust the building geometries based on available imagery, orthogonalize them, validate the results, and merge with existing data where applicable.

---

## What It Does

- **Unoverrides geometry**: Detects when a building lies within another and moves its vertices to the boundary to eliminate overlap.
- **Merges close corners**: Identifies corners within a defined distance and snaps them together to create cleaner, more consistent geometries.
- **Generates tailored outputs**: Python scripts create files in specific formats ready for use in OSM-assisted imports, such as mapping projects via the Tasking Manager.

---

## Inputs and Outputs

- **Input**: Google building footprints in WKT format (or similar geometries).
- **Output**: GeoJSON files optimized for OpenStreetMap integration.

---

## Example Workflow

1. Load building data into a PostgreSQL + PostGIS-enabled database.
2. Execute a sequence of SQL scripts to clean and adjust geometries.
3. Customize parameters or SQL queries as needed for your region.
4. Run the Python scripts to generate output files tailored for your use case (e.g., Tasking Manager).

*Note: The current setup is tailored for Brazilian territory. Adjustments and additional geographic data may be required for use in other regions.*

---

## Requirements

- Python 3.x
- PostgreSQL
- PostGIS
- Additional Python packages

---

## Clone the repository
   ```bash
   git clone https://github.com/santamariense/google_buildings_to_openstreetmap.git
   cd google_buildings_to_openstreetmap
   ```
## Step-by-Step for Processing Google Buildings

1. **Create the directory** `google_buildings`

2. **Download** all the CSV geometry tiles for Brazil using a Linux shell script from the site https://sites.research.google/gr/open-buildings (see the "Download" section). Save them to `google_buildings/originals`:
   ```bash
   chmod +x download_from_google.sh
   ./download_from_google.sh
   ```

3. **Extract the CSV files** from the downloaded archives:
   ```bash
   chmod +x decompress_downloaded_files.sh
   ./decompress_downloaded_files.sh
   ```

4. **Merge** all decompressed CSV parts into a single file:
   ```bash
   cat decompressed/*.csv > brasil.csv
   ```

5. **Use the Python script** `csv2pgsql.py` to populate the PostgreSQL database with the rows from `brasil.csv`:
   - Script available at: https://github.com/santamariense/csv2pgsql/blob/main/csv2pgsql.py
   - To configure the script, open it with a text editor (e.g., `gedit`) and locate the section **"CONFIGURE SCRIPT HERE"**. Edit the following variables:
     ```python
     os_user_password = 'your_linux_user_password'
     db_name = 'google_buildings'
     path_to_csv_file = '/path/to/google_buildings/brasil.csv'
     ```
     > Other fields require advanced knowledge. Unless you’ve customized the database or have deep understanding of your system architecture, do not modify them.

   - Run the script:
     ```bash
     python3 csv2pgsql.py
     ```

6. **Manipulate the geometries** directly in PostgreSQL by executing the following sequence:
   ```bash
   ./001_download_from_google.sh
   ./002_decompress_downloaded_files.sh
   ```

   Then, run the SQL and Python files in this order. It is highly recommended to run a snippet of SQL code at a time, considering that it will take a long time to run, and solve any issues along the way:
   - `01_Adjust_fields_types.sql`
   - `02_Import_data_from_others_databases.sql`
   - `03_Join_imported_data_to_the_main_table.sql`
   - `04_Adjust_geometries.sql`

   > Optional steps depending on your use case:

   - **Adjust buildings for an entire municipality**:
     ```bash
     python3 05_write_sql_to_make_municipalities.py
     python3 06_make_municipality_buildings.py
     ```

   - **Generate files for a Tasking Manager project**, dividing the municipality into blocks:
     ```bash
     python3 07_make_files_for_tasking_manager.py
     ```


