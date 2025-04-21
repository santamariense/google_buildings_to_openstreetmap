--/*
DROP TABLE IF EXISTS public.phase04_aggregated_buildings;
DROP TABLE IF EXISTS public.phase04_building_points;
DROP TABLE IF EXISTS public.phase04_building_points_modified;
DROP TABLE IF EXISTS public.phase04_building_points_original;
DROP TABLE IF EXISTS public.phase04_close_points_pair;
DROP TABLE IF EXISTS public.phase04_close_points_pair_unique_point_geom;
DROP TABLE IF EXISTS public.phase04_merged_points;
DROP TABLE IF EXISTS public.phase04_building_points_temp;
DROP TABLE IF EXISTS public.phase04_validated_building_block;
DROP TABLE IF EXISTS public.phase04_point_distances;


--------------------
-- FUNCTIONS: BEGIN
--------------------


      -- Function to truncate coords to 7 digits according to OSM server pattern
      CREATE OR REPLACE FUNCTION osm_trunc(geom geometry)
      RETURNS geometry AS $$
      BEGIN
          RETURN ST_GeomFromText(
              regexp_replace(
                  ST_AsText(geom),
                  '(-?\d+\.\d{7})\d*',  -- Match up to 7 decimal digits and discard the rest
                  '\1',                 -- Keep only the first 7 decimal digits
                  'g'                   -- Global replace
              ),
              ST_SRID(geom)            -- Preserve original SRID
          );
      END;
      $$ LANGUAGE plpgsql IMMUTABLE;


      -- Function to join points that are close to buildings but still not part of them
      DROP FUNCTION IF EXISTS public.aggregate_points_and_construct_geometry(text, text[]);
      CREATE OR REPLACE FUNCTION public.aggregate_points_and_construct_geometry(building_geometry text, point_geometries text[])
      RETURNS TEXT
      AS $$
        import math, re
        #  python functions #math
        def distance_point_to_line(lat1, lon1, lat2, lon2, lat, lon):
          # Convert latitudes and longitudes from degrees to radians
          lat1_rad = math.radians(lat1)
          lon1_rad = math.radians(lon1)
          lat2_rad = math.radians(lat2)
          lon2_rad = math.radians(lon2)
          lat_rad = math.radians(lat)
          lon_rad = math.radians(lon)
      
          # Earth radius (in kilometers)
          R = 6371.0  # Earth radius in kilometers
      
          # Vector components from (lat1, lon1) to (lat2, lon2)
          x1 = R * math.cos(lat1_rad) * math.cos(lon1_rad)
          y1 = R * math.cos(lat1_rad) * math.sin(lon1_rad)
          z1 = R * math.sin(lat1_rad)
      
          x2 = R * math.cos(lat2_rad) * math.cos(lon2_rad)
          y2 = R * math.cos(lat2_rad) * math.sin(lon2_rad)
          z2 = R * math.sin(lat2_rad)
      
          # Vector components from (lat1, lon1) to (lat, lon)
          x = R * math.cos(lat_rad) * math.cos(lon_rad)
          y = R * math.cos(lat_rad) * math.sin(lon_rad)
          z = R * math.sin(lat_rad)
      
          # Vector AB (line direction)
          ABx = x2 - x1
          ABy = y2 - y1
          ABz = z2 - z1
      
          # Vector AP (from point A to point P)
          APx = x - x1
          APy = y - y1
          APz = z - z1
      
          # Dot product of AB and AP
          AB_dot_AP = ABx * APx + ABy * APy + ABz * APz
      
          # Projection of AP onto AB
          projection_factor = AB_dot_AP / (ABx**2 + ABy**2 + ABz**2)
          projection_x = x1 + projection_factor * ABx
          projection_y = y1 + projection_factor * ABy
          projection_z = z1 + projection_factor * ABz
      
          # Calculate the distance from point P to the projection point
          distance = math.sqrt((x - projection_x)**2 + (y - projection_y)**2 + (z - projection_z)**2)
      
          # Check if the projection point is outside the line segment
          if projection_factor < 0:
              # Projection is before point A, calculate distance from (lat, lon) to (lat1, lon1)
              distance = math.sqrt((x - x1)**2 + (y - y1)**2 + (z - z1)**2)
          elif projection_factor > 1:
              # Projection is beyond point B, calculate distance from (lat, lon) to (lat2, lon2)
              distance = math.sqrt((x - x2)**2 + (y - y2)**2 + (z - z2)**2)
      
          return distance

        def find_duplicates(lines):
            line_counts = {}
            duplicates = []
        
            for line in lines:
                line_counts[line] = line_counts.get(line, 0) + 1
        
            for line, count in line_counts.items():
                if count > 1:
                    duplicates.append(line)
        
            return duplicates
        
        #  Código principal #
        modified_building_geometry = building_geometry

        
        if len(point_geometries) == 0:
            return building_geometry  # Retorna a geometria original se não houver pontos
        else:
            unique_point_geometries = list(set(point_geometries))
        
            for point in unique_point_geometries:
                lon, lat = point.replace("POINT(", "").replace(")", "").split(" ")
                #plpy.info('modified_building_geometry=',modified_building_geometry)
                lines_points = modified_building_geometry.replace("MULTI", "").replace("POLYGON", "").replace("LINESTRING","").replace("GEOMETRYCOLLECTION","").replace(")", "").replace("(", "").split(",")
                lines = [lines_points[i] + "," + lines_points[i + 1] for i in range(len(lines_points) - 1)]

                #plpy.info('lines = ',str(lines))

                if find_duplicates(lines):
                    pass  # Pode ser substituído por um log
        
                closest_line_to_point = None
                closest_distance_line_to_point = float("inf")
        
                for line in lines:
                    #plpy.info('line = ',str(line))
                    try:
                        parts = line.split(",")
                        if len(parts) < 2:
                            continue  # Ignora linhas inválidas
        
                        lon1, lat1 = parts[0].split(" ")
                        lon2, lat2 = parts[1].split(" ")
                    except IndexError:
                        continue  # Ignora caso a linha não tenha dois pontos válidos
        
                    if (lon1 != lon2) and (lat1 != lat2):
                        distance_from_point_to_line = distance_point_to_line(
                            float(lat1), float(lon1), float(lat2), float(lon2), float(lat), float(lon)
                        )
                        if distance_from_point_to_line < closest_distance_line_to_point:
                            closest_line_to_point = line
                            closest_distance_line_to_point = distance_from_point_to_line
        
                # Apenas modificar a geometria se encontrou uma linha válida
                if closest_line_to_point:
                    parts = closest_line_to_point.split(",")
                    if len(parts) >= 2 and line != parts[0] and line != parts[1]:
                        modified_building_geometry = modified_building_geometry.replace(
                            closest_line_to_point, closest_line_to_point.replace(",", "," + lon + " " + lat + ",")
                        )
        
        return modified_building_geometry
        
      $$ LANGUAGE plpython3u;

      -- Function to convert refactored geometry to geometry
      CREATE OR REPLACE FUNCTION try_convert_to_geometry(
          SD_geometry_text text,
          default_geom geometry
      ) 
      RETURNS geometry 
      AS $$
      DECLARE
          result_geom geometry;
      BEGIN
          -- Attempt to convert the SD_geometry_text to a geometry
          BEGIN
				    --RAISE NOTICE ' Success try_convert_to_geometry';
              result_geom := ST_GeomFromText(SD_geometry_text,4326);
          EXCEPTION
              WHEN others THEN
                  -- If conversion fails, assign the default_geom to result_geom
				         --RAISE NOTICE ' Failed try_convert_to_geometry';
                  result_geom := default_geom;
          END;
      
          -- Return the resulting geometry
          RETURN result_geom;
      END;
      $$ LANGUAGE plpgsql;


      -- Function to add a counter of points as array
      DROP FUNCTION IF EXISTS public.make_array_counter(TEXT);
      CREATE OR REPLACE FUNCTION public.make_array_counter(commas TEXT)
        RETURNS INTEGER[]
      AS $$
        array_out = []
        for i in range(len(commas) + 1):
          array_out.append(i+1)
        return array_out
      $$ LANGUAGE plpython3u;
      
      -- Function to replace aligned points in original google building
      DROP FUNCTION IF EXISTS public.remake_geometry();
      CREATE OR REPLACE FUNCTION public.remake_geometry(multipolygon_text TEXT, points TEXT[])
        RETURNS TEXT
      AS $$
        import re
      
        def replace_points_in_multipolygon(multipolygon_text, points):
            #plpy.info('multipolygon_text=',multipolygon_text)
            #plpy.info('points=',points)
            # Extract the contents inside the MULTIPOLYGON
            multipolygon_contents = re.search(r'\(\((.*)\)\)', multipolygon_text).group(1).replace(')),((',',')
            #plpy.info('multipolygon_contents=',multipolygon_contents)
      
            # Split the contents into individual polygons (rings)
            polygons = multipolygon_contents.split('),(')
      
            # Replace coordinates in each polygon with corresponding points
            for i in range(len(polygons)):
                # Extract the coordinates of the current polygon
                coordinates = polygons[i].split(',')
      
                # Replace coordinates with corresponding point coordinates from the points array
                for j in range(len(coordinates)):
                    point_coords = points[i * len(coordinates) + j].replace('POINT(', '').replace(')', '')
                    coordinates[j] = point_coords
      
                # Join the updated coordinates back together for the current polygon
                polygons[i] = ','.join(coordinates)
      
            # Join the updated polygons back together to form the updated MULTIPOLYGON text
            updated_multipolygon_text = 'MULTIPOLYGON(((' + '),('.join(polygons) + ')))'
      
            return updated_multipolygon_text
        #plpy.info('multipolygon_text=',multipolygon_text) # This print in the "Messages" tab
        #plpy.info('points=',points)
        # Call the replace_points_in_multipolygon function with the provided inputs
        updated_multipolygon_text = replace_points_in_multipolygon(multipolygon_text, points)
      
        # Check if the updated geometry contains polygons that share points
        def has_shared_points(multipolygon_text, updated_multipolygon_text):
            # Extract point sets from the original and updated geometries
            original_points_set = set(re.findall(r'\(([^)]+)\)', multipolygon_text))
            updated_points_set = set(re.findall(r'\(([^)]+)\)', updated_multipolygon_text))
      
            # Check for shared points between polygons
            shared_points = original_points_set.intersection(updated_points_set)
      
            return len(shared_points) > 0
      
        # Check if the updated geometry contains shared points
        if has_shared_points(multipolygon_text, updated_multipolygon_text):
            # If shared points are found, return the original multipolygon_text
            return multipolygon_text
        else:
            # Otherwise, return the updated MULTIPOLYGON text
            return updated_multipolygon_text
      $$ LANGUAGE plpython3u;
      
      -- Function to add a counter of points as array
      DROP FUNCTION IF EXISTS public.insert_to_list(TEXT);
      CREATE OR REPLACE FUNCTION public.insert_to_list(unformatted_list TEXT)
        RETURNS TEXT[]
      AS $$
        return sorted(list(set(str(unformatted_list).replace('[','').replace(']','').replace('"','').replace("'",'').split(', ')))) 
      $$ LANGUAGE plpython3u;


      -- Function to remove duplicated points in the same building in sequence
      DROP FUNCTION IF EXISTS public.merge_close_points_same_building(TEXT);
      CREATE OR REPLACE FUNCTION public.merge_close_points_same_building(geometry TEXT)
        RETURNS TEXT
      AS $$
        import math
        geometry_modified = geometry
        is_multi_geometry = True if '),(' in geometry_modified else False
        point_array = geometry_modified.replace("MULTI", "").replace("POLYGON", "").replace("LINESTRING","").replace("GEOMETRYCOLLECTION","").replace(")", "").replace("(", "").split(",")
        for i in range(len(point_array)-1):
           x1, y1 = point_array[i].split(' ')
           x2, y2 = point_array[i+1].split(' ')
           distance = math.sqrt((float(x2) - float(x1))**2 + (float(y2) - float(y1))**2)
           # Remove points that repeat in sequence
           #if distance == 0:
           #   geometry_modified = geometry_modified.replace((point_array[i]+','+point_array[i]),point_array[i])
           if distance > 0 and distance < 0.1:
              mx = (float(x1) + float(x2)) / 2
              my = (float(y1) + float(y2)) / 2
              geometry_modified = geometry_modified.replace(point_array[i],(str(mx)+' '+str(my)))
        return geometry_modified
      $$ LANGUAGE plpython3u;


-- Function to join points from one geometry that are touches another's border but still not part of them
CREATE OR REPLACE FUNCTION weld_snapped_points_into_ring(
    geom_points_from geometry, -- obtain points from this to weld into the next variable
    geom_points_to geometry, -- only this geometry is modified and returned
    tolerance numeric
)
RETURNS geometry AS $$
DECLARE
    geom_points_to_modified geometry;
    all_points_from geometry[];
    all_points_to geometry[];
    points_to_insert geometry[];
    pt geometry;
    any_point_touches boolean := false;
    welded_ring geometry;
    insert_location float8;
    updated_polygon geometry;
    updated_geom geometry;
BEGIN

	-- Simplify geom_points_to to only 1 ring (largest outer ring)
    SELECT ST_Multi(ST_MakePolygon(ring))
    INTO geom_points_to_modified   -- <<<< Filling variable
    FROM ( WITH all_polygons AS (
                   SELECT (ST_Dump(ST_MakeValid(geom_points_to))).geom AS geom_part
                ),
                outer_rings AS (
                   SELECT ST_ExteriorRing(geom_part) AS ring, ST_Area(geom_part) AS area
                   FROM all_polygons
                )
                   SELECT ring
                   FROM outer_rings
                   ORDER BY area DESC
                   LIMIT 1
         ) AS largest;
    RAISE NOTICE 'geom_points_to_modified = %', ST_AsText(geom_points_to_modified);
	
    -- Extract points from source and destination geometries
    SELECT array_agg((dp).geom) INTO all_points_from
    FROM (SELECT ST_DumpPoints(geom_points_from) AS dp) AS dump1;

    SELECT array_agg((dp).geom) INTO all_points_to
    FROM (SELECT ST_DumpPoints(geom_points_to_modified) AS dp) AS dump2;

    -- Identify points from "from" that intersect "to" and aren't already in it
    FOREACH pt IN ARRAY COALESCE(all_points_from, ARRAY[]::geometry[]) LOOP -- FOREACH pt IN ARRAY all_points_from LOOP
        IF ST_Intersects( -- If intersects
             ST_Transform(ST_Buffer(ST_Transform(pt, 3857), tolerance*20), 4326),-- #TODO  tolerance * ?
             geom_points_to_modified
           ) 
		THEN
		   RAISE NOTICE 'Intersected';
		   IF NOT EXISTS ( -- if there is already a point at that coord
                 SELECT 1 FROM unnest(all_points_to) AS to_pt
                 WHERE ST_Within(ST_Transform(pt, 3857), ST_Buffer(ST_Transform(to_pt, 3857), tolerance))
              ) 
		   THEN
              points_to_insert := points_to_insert || pt;
              any_point_touches := true;
              RAISE NOTICE 'Point % will be inserted.', ST_AsText(pt);
		   ELSE
		      RAISE NOTICE 'Point % will NOT be inserted.', ST_AsText(pt);
		   END IF;
        END IF;
    END LOOP;

    -- If no relevant point, return original multipolygon
    IF NOT any_point_touches THEN
	    RAISE NOTICE '........ GEOMETRY NOT MODIFIED...... ';
        RETURN geom_points_to_modified;
    END IF;

    -- Get outer ring of the first polygon as a LINESTRING
    SELECT ST_ExteriorRing(ST_GeometryN(geom_points_to_modified, 1)) INTO welded_ring;

    -- Insert points at closest locations on the ring
    FOREACH pt IN ARRAY COALESCE(points_to_insert, ARRAY[]::geometry[]) LOOP -- FOREACH pt IN ARRAY points_to_insert LOOP
        insert_location := ST_LineLocatePoint(welded_ring, pt);
        welded_ring := ST_AddPoint(
            welded_ring,
            pt,
            FLOOR(insert_location * ST_NPoints(welded_ring))::INT
        );
    END LOOP;

    -- Ensure the ring is closed
    IF NOT ST_IsClosed(welded_ring) THEN
        welded_ring := ST_AddPoint(welded_ring, ST_StartPoint(welded_ring));
    END IF;

    -- Create a polygon from updated outer ring
    updated_polygon := ST_MakePolygon(welded_ring);

    -- Rebuild multipolygon (only replacing first polygon)
    updated_geom := ST_Collect(updated_polygon);


	RAISE NOTICE '........ GEOMETRY    MODIFIED...... ';
    RETURN ST_SetSRID(ST_Multi(updated_geom), ST_SRID(geom_points_to_modified));
END;
$$ LANGUAGE plpgsql;


	-- Clean self intersections removing geometry's tails
CREATE OR REPLACE FUNCTION clean_self_intersections(geom geometry)
RETURNS geometry AS $$
DECLARE
    final_geom geometry;
BEGIN
    WITH input AS (
        SELECT ST_Buffer(geom, 0) AS g
    ),
    ordered_points AS (
        SELECT 
            ROW_NUMBER() OVER () AS id,
            (dp).geom AS pt
        FROM (
            SELECT ST_DumpPoints(g) AS dp
            FROM input
        ) AS dumped
    ),
    segments AS (
        SELECT
            p1.id AS seg_id,
            ST_MakeLine(p1.pt, p2.pt) AS seg
        FROM ordered_points p1
        JOIN ordered_points p2 ON p1.id + 1 = p2.id
    ),
    normalized_segments AS (
        SELECT
            seg_id,
            LEAST(ST_AsText(seg), ST_AsText(ST_Reverse(seg))) AS seg_norm,
            seg
        FROM segments
    ),
    duplicates AS (
        SELECT seg_norm
        FROM normalized_segments
        GROUP BY seg_norm
        HAVING COUNT(*) > 1
    ),
    repeated_geom AS (
        SELECT ST_LineMerge(ST_Collect(seg)) AS repeated_line
        FROM normalized_segments
        WHERE seg_norm IN (SELECT seg_norm FROM duplicates)
    ),
    repeated_poly AS (
        SELECT 
            CASE 
                WHEN ST_IsClosed(repeated_line) AND ST_NPoints(repeated_line) > 3 THEN 
                    ST_MakePolygon(ST_AddPoint(repeated_line, ST_StartPoint(repeated_line)))
                ELSE NULL
            END AS poly
        FROM repeated_geom
        WHERE repeated_line IS NOT NULL AND ST_NPoints(repeated_line) > 3
    ),
    result AS (
        SELECT 
            CASE 
                WHEN r.poly IS NOT NULL THEN ST_Difference(i.g, r.poly)
                ELSE i.g
            END AS cleaned_geom
        FROM input i
        LEFT JOIN repeated_poly r ON true
    )
    SELECT cleaned_geom INTO final_geom FROM result;

    RETURN final_geom;
END;
$$ LANGUAGE plpgsql;


-- Function that returns true if geometry contains 2 points or less | To be a polygon area it must contain at least 3 points
CREATE OR REPLACE FUNCTION contains2ptsorless(geom geometry)
RETURNS boolean AS
$$
DECLARE
    num_unique_pts integer;
BEGIN
    WITH exploded AS (
        SELECT (dp).geom AS pt
        FROM (
            SELECT ST_DumpPoints(geom) AS dp
        ) AS foo
    ),
    filtered AS (
        SELECT pt
        FROM exploded
        WHERE NOT (ST_X(pt) = 0 AND ST_Y(pt) = 0)
    )
    SELECT COUNT(DISTINCT ST_AsText(pt)) INTO num_unique_pts
    FROM filtered;

    IF num_unique_pts <= 2 THEN
        RETURN TRUE; -- Flag to remove geometry
    ELSE
        RETURN FALSE;
    END IF;
END;
$$
LANGUAGE plpgsql IMMUTABLE;

-- Function that tests if geometry contains repeated points | return boolean
CREATE OR REPLACE FUNCTION has_repeated_points(geom geometry)
RETURNS boolean AS $$
DECLARE
    poly geometry;
    ring geometry;
    pt geometry;
    key TEXT;
    seen_points TEXT[];
    num_polys INT;
    num_rings INT;
    i INT;
    j INT;
    k INT;
    ring_npoints INT;
BEGIN
    IF geom IS NULL THEN
        RETURN FALSE;
    END IF;

    num_polys := ST_NumGeometries(geom);

    FOR i IN 1..num_polys LOOP
        poly := ST_GeometryN(geom, i);
        num_rings := ST_NumInteriorRings(poly) + 1;

        FOR j IN 1..num_rings LOOP
            IF j = 1 THEN
                ring := ST_ExteriorRing(poly);
            ELSE
                ring := ST_InteriorRingN(poly, j - 1);
            END IF;

            seen_points := ARRAY[]::TEXT[];
            ring_npoints := ST_NPoints(ring);

            FOR k IN 1..(ring_npoints - 1) LOOP  -- ignore closing point
                pt := ST_PointN(ring, k);
                key := ROUND(ST_X(pt)::numeric, 7)::TEXT || ' ' || ROUND(ST_Y(pt)::numeric, 7)::TEXT;

                IF key = ANY(seen_points) THEN
                    RETURN TRUE;
                ELSE
                    seen_points := seen_points || key;
                END IF;
            END LOOP;
        END LOOP;
    END LOOP;

    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Function that remove repeated points from geomtry | returns geometry
CREATE OR REPLACE FUNCTION remove_repeated_points(geom geometry)
RETURNS geometry AS $$
DECLARE
    poly geometry;
    ring geometry;
    new_poly geometry;
    new_polygons geometry[];
    num_polys INT;
    num_rings INT;
    i INT;
    j INT;
    pt geometry;
    key TEXT;
    seen_points TEXT[];
    clean_points geometry[];
    ring_npoints INT;
    k INT;
	output_geom geometry;
BEGIN
    IF geom IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        num_polys := ST_NumGeometries(geom);

        FOR i IN 1..num_polys LOOP
            poly := ST_GeometryN(geom, i);
            num_rings := ST_NumInteriorRings(poly) + 1;
            DECLARE
                rings geometry[];
            BEGIN
                FOR j IN 1..num_rings LOOP
                    IF j = 1 THEN
                        ring := ST_ExteriorRing(poly);
                    ELSE
                        ring := ST_InteriorRingN(poly, j - 1);
                    END IF;

                    seen_points := ARRAY[]::TEXT[];
                    clean_points := ARRAY[]::geometry[];

                    ring_npoints := ST_NPoints(ring);

                    FOR k IN 1..(ring_npoints - 1) LOOP  -- exclude closing point
                        pt := ST_PointN(ring, k);
                        key := ROUND(ST_X(pt)::numeric, 7)::TEXT || ' ' || ROUND(ST_Y(pt)::numeric, 7)::TEXT;

                        IF NOT key = ANY(seen_points) THEN
                            clean_points := clean_points || pt;
                            seen_points := seen_points || key;
                        END IF;
                    END LOOP;

                    -- Close the ring
                    IF array_length(clean_points, 1) >= 1 THEN
                        clean_points := clean_points || clean_points[1];
                    END IF;

                    -- Create LineString from cleaned points
                    rings := rings || ST_MakeLine(clean_points);
                END LOOP;

                -- Rebuild polygon
                new_poly := ST_MakePolygon(rings[1]);
                IF array_length(rings, 1) > 1 THEN
                    FOR k IN 2..array_length(rings, 1) LOOP
                        new_poly := ST_SetInteriorRingN(new_poly, k - 1, rings[k]);
                    END LOOP;
                END IF;

                new_polygons := new_polygons || new_poly;
            END;
        END LOOP;

		output_geom = ST_Multi(ST_Collect(new_polygons));

		--IF NOT ST_isValid(output_geom) THEN
		--   RETURN geom;
		--END IF;

        RETURN ST_Multi(ST_Collect(new_polygons));

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error: %', SQLERRM;
            RETURN geom;
    END;
END;
$$ LANGUAGE plpgsql;


--------------------
-- FUNCTIONS: END --
--------------------




------------------------------------------------------
-- PROCEDURE modify_overlapping_geometries : BEGIN --
------------------------------------------------------

CREATE OR REPLACE PROCEDURE modify_overlapping_geometries(block_id_var INTEGER, source_table TEXT) 
LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
    modified_geom_1 geometry;
    modified_geom_2 geometry;
    p RECORD;
    border_geom geometry;
    new_points_1 geometry[];
    new_points_2 geometry[];
    points_moved_1 INT := 0;
    points_moved_2 INT := 0;
BEGIN
    -- Create working table for geometries in the block using dynamic SQL : BEGIN --

/*


    -- Create working table for geometries in the block using dynamic SQL
    DROP TABLE IF EXISTS public.phase04_geometries_in_a_block;
    EXECUTE 'CREATE TABLE public.phase04_geometries_in_a_block AS
             SELECT * FROM ' || source_table || ' WHERE mun_block = ' || block_id_var || ';';
    

--*/

--/*	
	DROP TABLE IF EXISTS public.phase04_geometries_in_a_block;
	EXECUTE '
CREATE TABLE public.phase04_geometries_in_a_block AS
WITH cleaned AS (
    SELECT 
        id, latitude, longitude, area_in_meters, confidence, geoc_mun, 
        zone_type, mun_block, full_plus_code,
        osm_trunc(ST_CollectionExtract(ST_MakeValid(geometry), 3)) AS cleaned_geom
    FROM ' || source_table || ' 
    WHERE mun_block = ' || block_id_var || '
),

filtered AS (
    SELECT *,
        CASE 
            WHEN GeometryType(cleaned_geom) = ''POLYGON'' THEN ST_Multi(cleaned_geom)
            WHEN GeometryType(cleaned_geom) = ''MULTIPOLYGON'' THEN cleaned_geom
            ELSE NULL
        END AS geometry
    FROM cleaned
)

SELECT 
    id, latitude, longitude, area_in_meters, confidence, geoc_mun, 
    zone_type, mun_block, full_plus_code,
    geometry
FROM filtered
WHERE geometry IS NOT NULL;
';
--*/

        -- Step 1: Create temp table for single-ring geometries
/*
        EXECUTE '
        DROP TABLE IF EXISTS public.phase04_geometries_in_a_block_single_ring_geoms_temp;
        CREATE TABLE public.phase04_geometries_in_a_block_single_ring_geoms_temp AS
        SELECT 
            id, latitude, longitude, area_in_meters, confidence, geoc_mun, 
            zone_type, mun_block, full_plus_code,
            ST_MakeValid(geometry) AS geometry
        FROM ' || source_table || '
        WHERE mun_block = ' || block_id_var || '
        AND ST_NumGeometries(geometry) = 1;';
        
        -- Step 2: Create temp table for multi-ring geometries
        EXECUTE '
        DROP TABLE IF EXISTS public.phase04_geometries_in_a_block_multi_ring_largest_geom_temp;
        CREATE TABLE public.phase04_geometries_in_a_block_multi_ring_largest_geom_temp AS
        WITH valid_geoms AS (
            SELECT 
                id, latitude, longitude, area_in_meters, confidence, geoc_mun, 
                zone_type, mun_block, full_plus_code,
                ST_MakeValid(geometry) AS valid_geom
            FROM ' || source_table || '
            WHERE mun_block = ' || block_id_var || '
            AND ST_NumGeometries(geometry) > 1
        ),
        dumped AS (
            SELECT 
                vg.id, vg.latitude, vg.longitude, vg.area_in_meters, vg.confidence, vg.geoc_mun,
                vg.zone_type, vg.mun_block, vg.full_plus_code,
                (ST_Dump(vg.valid_geom)).geom AS single_geom
            FROM valid_geoms vg
        ),
        areas AS (
            SELECT *,
                ST_Area(ST_Transform(single_geom, 3857)) AS area_metric
            FROM dumped
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY area_metric DESC) AS rank
            FROM areas
        )
        SELECT 
            id, latitude, longitude, area_in_meters, confidence, geoc_mun,
            zone_type, mun_block, full_plus_code,
            single_geom AS geometry
        FROM ranked
        WHERE rank = 1;
        ';
		
        -- Step 3: Union result
        EXECUTE '
        DROP TABLE IF EXISTS public.phase04_geometries_in_a_block;
        CREATE TABLE public.phase04_geometries_in_a_block AS
        SELECT * FROM phase04_geometries_in_a_block_single_ring_geoms_temp
        UNION ALL
        SELECT * FROM phase04_geometries_in_a_block_multi_ring_largest_geom_temp;
        ';
        
        -- Drop temporay tables
        DROP TABLE IF EXISTS public.phase04_geometries_in_a_block_single_ring_geoms_temp;
        DROP TABLE IF EXISTS public.phase04_geometries_in_a_block_multi_ring_largest_geom_temp;
        
        -- Raise notice
        RAISE NOTICE 'Total geometries in result: %',
          (SELECT COUNT(*) FROM phase04_geometries_in_a_block);

--*/   
    -- Create working table for geometries in the block using dynamic SQL : END --

    
    -- Extract all individual points from original building polygons
    DROP TABLE IF EXISTS public.phase04_original_points_from_geometries_in_a_block;
    CREATE TABLE public.phase04_original_points_from_geometries_in_a_block AS
    SELECT 
        g.id AS building_id, 
        (dp.geom) AS point_geom
    FROM public.phase04_geometries_in_a_block g,
         LATERAL ST_DumpPoints(g.geometry) dp;

    -- Identify intersections areas : BEGIN -- 
    BEGIN
        RAISE NOTICE 'Trying fast ST_Intersection...';
    
        DROP TABLE IF EXISTS public.phase04_intersection_areas;
    
        CREATE TABLE public.phase04_intersection_areas AS
            SELECT 
                a.id AS id_building_1, 
                b.id AS id_building_2,
                ST_NPoints(ST_Intersection(a.geometry, b.geometry)) - 1 AS num_points,
                ST_Area(ST_Transform(ST_Intersection(a.geometry, b.geometry), 3857)) AS area_m2,
                ST_Multi(ST_CollectionExtract(ST_Intersection(a.geometry, b.geometry), 3)) AS intersect_geom
            FROM public.phase04_geometries_in_a_block a
            JOIN public.phase04_geometries_in_a_block b 
                 ON a.id < b.id
            WHERE ST_Overlaps(a.geometry, b.geometry);
    
        RAISE NOTICE 'Fast version succeeded.';
    
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Fast version failed, switching to safe cleaned version...';
    
            -- Try the safe version with geometry cleaning and validation
            DROP TABLE IF EXISTS public.phase04_intersection_areas;
    
            CREATE TABLE public.phase04_intersection_areas AS
                SELECT 
                    a.id AS id_building_1, 
                    b.id AS id_building_2,
                    ST_NPoints(ST_Intersection(a.geom_clean, b.geom_clean)) - 1 AS num_points,
                    ST_Area(ST_Transform(ST_Intersection(a.geom_clean, b.geom_clean), 3857)) AS area_m2,
                    ST_Multi(ST_CollectionExtract(ST_Intersection(a.geom_clean, b.geom_clean), 3)) AS intersect_geom
                FROM (
                    SELECT id, ST_Buffer(ST_MakeValid(geometry), 0) AS geom_clean
                    FROM public.phase04_geometries_in_a_block
                    WHERE geometry IS NOT NULL AND ST_IsValid(geometry)
                ) a
                JOIN (
                    SELECT id, ST_Buffer(ST_MakeValid(geometry), 0) AS geom_clean
                    FROM public.phase04_geometries_in_a_block
                    WHERE geometry IS NOT NULL AND ST_IsValid(geometry)
                ) b 
                ON a.id < b.id
                WHERE ST_Overlaps(a.geom_clean, b.geom_clean);
    
            RAISE NOTICE 'Fallback version completed.';
    END;
    -- Identify intersections areas : END -- 


    -- Calculate distances between intersection points and filter smallest ones, checking if the point also exists in original data
    DROP TABLE IF EXISTS public.phase04_intersection_distances;
    CREATE TABLE public.phase04_intersection_distances AS
    WITH geom_parts AS (
        -- Dump multipolygons into individual polygons
        SELECT 
            ia.id_building_1,
            ia.id_building_2,
            ia.num_points,
            (ST_Dump(ia.intersect_geom)).geom AS polygon_geom
        FROM public.phase04_intersection_areas ia
    ),
    rings AS (
        -- Extract outer rings from each polygon
        SELECT 
            gp.id_building_1,
            gp.id_building_2,
            gp.num_points,
            ROW_NUMBER() OVER (PARTITION BY gp.id_building_1, gp.id_building_2 ORDER BY gp.polygon_geom) AS ring_id,
            ST_ExteriorRing(gp.polygon_geom) AS ring_geom
        FROM geom_parts gp
    ),
    points_raw AS (
        SELECT 
            rng.id_building_1,
            rng.id_building_2,
            rng.ring_id,
            rng.num_points,
            dp.path[1] AS point_index,
            dp.geom,
            COUNT(*) OVER (PARTITION BY rng.id_building_1, rng.id_building_2, rng.ring_id) AS total_points,
            rng.ring_geom
        FROM rings rng,
             LATERAL ST_DumpPoints(rng.ring_geom) dp
    ),
    first_points AS (
        -- Grab the first point of each ring to detect closed rings
        SELECT 
            id_building_1,
            id_building_2,
            ring_id,
            geom AS first_geom
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id_building_1, id_building_2, ring_id ORDER BY point_index) AS rn
            FROM points_raw
        ) sub
        WHERE rn = 1
    ),
    cleaned_points AS (
        -- Remove last point if it duplicates the first (closed ring)
        SELECT pr.*
        FROM points_raw pr
        LEFT JOIN first_points fp
          ON pr.id_building_1 = fp.id_building_1
         AND pr.id_building_2 = fp.id_building_2
         AND pr.ring_id = fp.ring_id
        WHERE NOT (
            pr.point_index = pr.total_points AND pr.geom = fp.first_geom
        )
    ),
    point_pairs AS (
        -- Pair all distinct points within the same ring
        SELECT 
            p1.id_building_1,
            p1.id_building_2,
            p1.ring_id,
            p1.num_points,
            p1.geom AS geom1,
            p2.geom AS geom2,
            ST_Distance(ST_Transform(p1.geom, 3857), ST_Transform(p2.geom, 3857)) AS distance
        FROM cleaned_points p1
        JOIN cleaned_points p2
          ON p1.id_building_1 = p2.id_building_1
         AND p1.id_building_2 = p2.id_building_2
         AND p1.ring_id = p2.ring_id
         AND p1.point_index < p2.point_index
    ),
    ranked_pairs AS (
        -- Rank distances per building pair per ring
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id_building_1, id_building_2, ring_id
                   ORDER BY distance
               ) AS distance_rank
        FROM point_pairs
    )
    SELECT 
        id_building_1, 
        id_building_2,
        distance,
        geom1 AS point_1,
        geom2 AS point_2,
        EXISTS (
            SELECT 1 
            FROM public.phase04_original_points_from_geometries_in_a_block op 
            WHERE ST_DWithin(ST_Transform(geom1, 3857), ST_Transform(op.point_geom, 3857), 0.001)
        ) AS point_1_in_original,
        EXISTS (
            SELECT 1 
            FROM public.phase04_original_points_from_geometries_in_a_block op 
            WHERE ST_DWithin(ST_Transform(geom2, 3857), ST_Transform(op.point_geom, 3857), 0.001)
        ) AS point_2_in_original,
        ST_MakeLine(geom1, geom2) AS line_geom
    FROM ranked_pairs
    WHERE distance_rank <= FLOOR(num_points / 2);
        
    -- Identify pair move_from_point and move_to_point
    --/*
DROP TABLE IF EXISTS public.phase04_move_point_from_to;
CREATE TABLE public.phase04_move_point_from_to AS
SELECT *
FROM (
    SELECT 
        id_building_1, 
        id_building_2, 
        point_1_in_original,
        point_2_in_original,
        CASE 
            WHEN point_1_in_original = true THEN point_1
            WHEN point_2_in_original = true THEN point_2
        END AS move_from_point,
        CASE 
            WHEN point_1_in_original = false THEN point_1
            WHEN point_2_in_original = false THEN point_2
        END AS move_to_point
    FROM public.phase04_intersection_distances
    WHERE point_1_in_original <> point_2_in_original  -- only one of them is true
          
    UNION ALL

    SELECT -- point_1 to middle
        id_building_1, 
        id_building_2, 
        point_1_in_original,
        point_2_in_original,
        point_1 AS move_from_point,
        ST_LineInterpolatePoint(ST_MakeLine(point_1, point_2), 0.5) AS move_to_point
    FROM public.phase04_intersection_distances
    WHERE point_1_in_original = true AND point_2_in_original = true  -- both are true
	
    UNION ALL

    SELECT -- point_2 to middle
        id_building_1, 
        id_building_2, 
        point_1_in_original,
        point_2_in_original,
        point_2 AS move_from_point,
        ST_LineInterpolatePoint(ST_MakeLine(point_1, point_2), 0.5) AS move_to_point
    FROM public.phase04_intersection_distances
    WHERE point_1_in_original = true AND point_2_in_original = true  -- both are true
) AS combined
ORDER BY id_building_1, id_building_2;

   /*
	DROP TABLE IF EXISTS public.phase04_move_point_from_to;
    CREATE TABLE public.phase04_move_point_from_to AS
    SELECT --DISTINCT ON (id_building_1, id_building_2) 
           id_building_1, 
           id_building_2, 
           --point_1, 
           --point_2,
           point_1_in_original,
           point_2_in_original,
           CASE 
                WHEN point_1_in_original = true THEN point_1
                WHEN point_2_in_original = true THEN point_2
           END AS move_from_point,
           CASE 
                WHEN point_1_in_original = false THEN point_1
                WHEN point_2_in_original = false THEN point_2
           END AS move_to_point
    FROM public.phase04_intersection_distances
    WHERE point_1_in_original <> point_2_in_original -- They cant be both true or both false
    --  AND point_1_in_original IS DISTINCT FROM point_2_in_original -- Ensures only one is true
    ORDER BY id_building_1, id_building_2, distance;--*/

    -- Create new table with updated geometries

--/*
    DROP TABLE IF EXISTS public.phase04_geometries_with_moved_points;
    CREATE TABLE public.phase04_geometries_with_moved_points AS
    WITH dumped_geoms AS (
        SELECT 
            g.id,
            dp.geom AS polygon_geom,
            dp.path AS polygon_path
        FROM public.phase04_geometries_in_a_block g,
             LATERAL ST_Dump(g.geometry) AS dp  -- This ensures each polygon in a multipolygon is handled separately
    ),
    replaced_points AS (
        SELECT 
            dg.id,
            dg.polygon_path,
            ring.path,
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM public.phase04_move_point_from_to mp
                    WHERE ST_DWithin(
                        ST_Transform(ring.geom, 3857),
                        ST_Transform(mp.move_from_point, 3857),
                        0.015
                    )
                ) THEN (
                    SELECT mp.move_to_point
                    FROM public.phase04_move_point_from_to mp
                    WHERE ST_DWithin(
                        ST_Transform(ring.geom, 3857),
                        ST_Transform(mp.move_from_point, 3857),
                        0.015
                    )
                    LIMIT 1
                )
                ELSE ring.geom
            END AS geom
        FROM dumped_geoms dg,
             LATERAL ST_DumpPoints(dg.polygon_geom) AS ring
    ),
    rings_grouped AS (
        SELECT 
            id,
            polygon_path,
            (path)[1] AS ring_id,  -- Outer ring or inner ring identifier
            ARRAY_AGG(geom ORDER BY path) AS ring_points
        FROM replaced_points
        GROUP BY id, polygon_path, (path)[1]
    ),
    polygons_built AS (
        SELECT 
            id,
            polygon_path,
            ST_MakePolygon(ST_MakeLine(ring_points)) AS polygon
        FROM rings_grouped
        GROUP BY id, polygon_path, ring_points
    ),
    multipolygons AS (
        SELECT 
            id,
            ST_Multi(ST_Collect(polygon)) AS geometry
        FROM polygons_built
        GROUP BY id
    )
    SELECT 
        mp.id,
        gib.latitude, gib.longitude, gib.area_in_meters, gib.confidence, gib.geoc_mun,
        gib.zone_type, gib.mun_block, gib.full_plus_code,
        ST_MakeValid(mp.geometry) as geometry
    FROM multipolygons mp
    LEFT JOIN public.phase04_geometries_in_a_block gib ON mp.id = gib.id; --*/

--/* 

-- Unoverlap final table | Clean and normalize geometries
DROP TABLE IF EXISTS public.phase04_geometries_unoverlapped;
CREATE TABLE public.phase04_geometries_unoverlapped AS
WITH only_collections AS (
    SELECT 
        id,
        ST_CollectionExtract(ST_MakeValid(geometry), 3) AS clean_geom
    FROM public.phase04_geometries_with_moved_points
    WHERE GeometryType(geometry) = 'GEOMETRYCOLLECTION'
),
filtered AS (
    SELECT 
        id,
        CASE 
            WHEN GeometryType(clean_geom) = 'POLYGON' THEN ST_Multi(clean_geom)
            WHEN GeometryType(clean_geom) = 'MULTIPOLYGON' THEN clean_geom
            ELSE NULL
        END AS geometry
    FROM only_collections
)
SELECT 
    b.id,
    b.latitude, 
    b.longitude, 
    b.area_in_meters, 
    b.confidence, 
    b.geoc_mun,
    b.zone_type, 
    b.mun_block, 
    b.full_plus_code,
    ST_RemoveRepeatedPoints(COALESCE(f.geometry, b.geometry)) AS geometry
FROM public.phase04_geometries_with_moved_points b
LEFT JOIN filtered f ON b.id = f.id;


--*/

-- Raise notice
RAISE NOTICE 'NOT MULTIPOLYGONs: %',
  (SELECT COUNT(*) FROM phase04_geometries_unoverlapped WHERE ST_GeometryType(geometry) NOT LIKE 'ST_MultiPolygon%');
  
--*/

-- Load unoverlapped table to its own schema
/*
     CREATE SCHEMA IF NOT EXISTS unoverlapped_buildings_by_block;         
     EXECUTE 'DROP TABLE IF EXISTS unoverlapped_buildings_by_block.block_id_' || block_id_var || ';
         CREATE TABLE unoverlapped_buildings_by_block.block_id_' || block_id_var || ' AS
         SELECT * FROM public.phase04_geometries_unoverlapped;';
--*/


    /*-- Drop temporary tables
    DROP TABLE IF EXISTS public.phase04_geometries_in_a_block;
    DROP TABLE IF EXISTS public.phase04_move_point_from_to;
    DROP TABLE IF EXISTS public.phase04_intersection_areas;
    DROP TABLE IF EXISTS public.phase04_intersection_distances;
    DROP TABLE IF EXISTS public.phase04_original_points_from_geometries_in_a_block;*/
END;
$$;


-- Call the procedure for a specific block TEST
--CALL modify_overlapping_geometries(2432814); -- Cachoeira do Sul: 2256321 | contain multipolygon: 2316057
-- CALL modify_overlapping_geometries(2256321);

------------------------------------------------------
-- PROCEDURE modify_overlapping_geometries : END --
------------------------------------------------------







------------------------------------------------------
-- PROCEDURE get_chained_buildings_by_block : BEGIN --
------------------------------------------------------

DROP PROCEDURE IF EXISTS get_chained_buildings_by_block(INTEGER,FLOAT);
CREATE OR REPLACE PROCEDURE get_chained_buildings_by_block(block_id_var INTEGER, distance_threshold_meters FLOAT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    building_id INTEGER;
    current_building_id INTEGER := 0;
    building_id_list INTEGER[];
    overlapped_building_id_list INTEGER[];
    overlapped_building_id INTEGER;
    loop_counter INTEGER := 0;
    count_intersec_buildings INTEGER;
    current_block_pieces_id INTEGER;
    blocks_that_need_cut_list INTEGER[];
    mosaic_id INTEGER;
    count_buildings_in_mosaic INTEGER;
    lines_to_make_mosaic INTEGER;
    rec RECORD; -- Just to print on tests
BEGIN

   -- Try to unoverlap geometries and create a table containing the buildings to be processed
   CALL modify_overlapping_geometries(block_id_var,'public.brazil_04');

   -- Filter building that are in the block
   DROP TABLE IF EXISTS public.phase04_buildings_of_a_block;
   CREATE TABLE public.phase04_buildings_of_a_block AS
      SELECT id, mun_block as block_id, geometry
      FROM public.phase04_geometries_unoverlapped -- phase04_geometries_unoverlapped | previously brazil_04 
      WHERE mun_block = block_id_var;
   ALTER TABLE public.phase04_buildings_of_a_block ADD COLUMN block_pieces_id INTEGER;
   --DROP INDEX IF EXISTS idx_100; CREATE INDEX idx_100 ON public.phase04_buildings_of_a_block(id);
   --DROP INDEX IF EXISTS idx_101; CREATE INDEX idx_101 ON public.phase04_buildings_of_a_block USING GIST(geometry);

   -- Catch all blocks with too many buildings. They need to be subdivided
   SELECT array_agg(too.block_id) INTO blocks_that_need_cut_list FROM public.blocks_br_with_too_many_buildings as too;
   --RAISE NOTICE '  blocks_that_need_cut_list = %', blocks_that_need_cut_list;

   -- If block has too many buildings : Generate mosaic (1%x1%) over the block
   IF block_id_var = ANY(blocks_that_need_cut_list) THEN
      -- Get the amount of buildings in the block
	  SELECT (too.building_counter/100)::integer INTO lines_to_make_mosaic FROM public.blocks_br_with_too_many_buildings as too WHERE too.block_id = block_id_var;
      
      -- Step 1: Calculate the bounding box of all geometries in the table
      DROP TABLE IF EXISTS public.phase04_block_mosaic_grid;
      -- Step 2: Create a temporary table to hold the intermediate grid results
      EXECUTE '
         CREATE TEMP TABLE temp_adjusted_grid AS
         WITH bbox AS (
            SELECT
                ST_Transform(ST_SetSRID(ST_Extent(geometry), 4326), 3857) AS extent
            FROM
                public.phase04_buildings_of_a_block
         ),
         extent_values AS (
            SELECT
                ST_XMin(extent) AS xmin,
                ST_YMin(extent) AS ymin,
                ST_XMax(extent) AS xmax,
                ST_YMax(extent) AS ymax
            FROM
                bbox
         ),
         grid_dimensions AS (
            SELECT
                (xmax - xmin) / 10.0 AS cell_width,  --  || lines_to_make_mosaic::float::text || 
                (ymax - ymin) / 10.0 AS cell_height, -- || lines_to_make_mosaic::float::text || 
                xmin,
                ymin
            FROM
                extent_values
         ),
         grid AS (
            SELECT
                row_number() OVER () AS id,
                ST_MakeEnvelope(
                    xmin + (i * cell_width),
                    ymin + (j * cell_height),
                    xmin + ((i + 1) * cell_width),
                    ymin + ((j + 1) * cell_height),
                    3857
                ) AS geom
            FROM
                grid_dimensions,
                generate_series(0, 10) AS i, --  || lines_to_make_mosaic::text || 
                generate_series(0, 10) AS j  --  || lines_to_make_mosaic::text || 
         ),
         adjusted_grid AS (
            SELECT
                id,
                ST_Transform(geom, 4326) AS geom
            FROM
                grid
         )
         SELECT
            id,
            geom
         FROM
            adjusted_grid
      ';

      -- Step 3: Create the final table from the temporary table
      EXECUTE '
         CREATE TABLE public.phase04_block_mosaic_grid AS
         SELECT
            id,
            geom
         FROM
            temp_adjusted_grid
      ';

      -- Step 4: Drop the temporary table
      DROP TABLE temp_adjusted_grid;

   ELSE -- if there are < 1000 buildings in the block, 
      -- Mosaic grid will be the bounding box itself
      DROP TABLE IF EXISTS public.phase04_block_mosaic_grid;  
      CREATE TABLE public.phase04_block_mosaic_grid AS
      WITH bbox AS (
          SELECT
              ST_Extent(geometry) AS extent
          FROM
              public.phase04_buildings_of_a_block
      )
      SELECT
		  1 as id,
          ST_SetSRID(
              ST_MakeEnvelope(
                  ST_XMin(extent),
                  ST_YMin(extent),
                  ST_XMax(extent),
                  ST_YMax(extent)
              ),
              4326
          ) AS geom
      FROM
          bbox;
   END IF;


   FOR mosaic_id IN SELECT DISTINCT id FROM public.phase04_block_mosaic_grid LOOP

      -- Divide buildings in the block by mosaic
      DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_by_mosaic;
      CREATE TABLE public.phase04_buildings_of_a_block_by_mosaic AS
      SELECT a.*
      FROM public.phase04_buildings_of_a_block a
      JOIN public.phase04_block_mosaic_grid b
      ON ST_Within(ST_Centroid(a.geometry), b.geom)
      WHERE b.id = mosaic_id AND a.block_pieces_id is null;


      SELECT count(*) INTO count_buildings_in_mosaic FROM public.phase04_buildings_of_a_block_by_mosaic;
      --RAISE NOTICE '  count_buildings_in_mosaic = %', count_buildings_in_mosaic;

      IF count_buildings_in_mosaic > 0 THEN
      WHILE TRUE LOOP
      
         loop_counter = loop_counter + 1;
         RAISE NOTICE 'loop_counter = %', loop_counter;

         --IF array_length(building_id_list, 1) IS NULL THEN
         SELECT min(id) INTO current_building_id FROM public.phase04_buildings_of_a_block_by_mosaic WHERE block_pieces_id IS NULL;
         --RAISE NOTICE 'current_building_id = %', current_building_id;
         IF current_building_id IS NULL THEN EXIT; END IF;
         building_id_list = ARRAY[current_building_id];
         --END IF;

         -- Set variable values
         --SELECT block_pieces_id INTO current_block_pieces_id FROM public.phase04_buildings_of_a_block WHERE building_id = id;
         --IF current_block_pieces_id IS NULL THEN
         --   current_block_pieces_id = loop_counter; 
         --END IF;
      
         WHILE array_length(building_id_list, 1) IS NOT NULL LOOP

            -- Always start by the minimum value to be processed
            SELECT MIN(val) INTO building_id FROM unnest(building_id_list) AS val;

            -- Update current building_id 
            UPDATE public.phase04_buildings_of_a_block_by_mosaic AS b SET block_pieces_id = loop_counter WHERE b.id = building_id;

            -- Catch buildings that intersects this
            DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_by_mosaic_intersect;
            CREATE TABLE public.phase04_buildings_of_a_block_by_mosaic_intersect AS
               SELECT a.id AS building_id1, b.id AS building_id2
               FROM public.phase04_buildings_of_a_block_by_mosaic a
               JOIN public.phase04_buildings_of_a_block_by_mosaic b
               ON a.id <> b.id
               WHERE a.id = building_id AND b.block_pieces_id IS NULL
                 AND ST_Intersects(
                        ST_Buffer(ST_Transform(a.geometry, 3857), distance_threshold_meters*2),
                        ST_Buffer(ST_Transform(b.geometry, 3857), distance_threshold_meters*2)
                     );
   
            -- Add overlapping buildings to list
            --RAISE NOTICE 'building_id_list before = %', building_id_list;
            SELECT array_agg(i.building_id2) INTO overlapped_building_id_list 
               FROM public.phase04_buildings_of_a_block_by_mosaic_intersect as i
               LEFT JOIN public.phase04_buildings_of_a_block_by_mosaic as b ON i.building_id2 = b.id
               WHERE b.block_pieces_id IS NULL;
            --RAISE NOTICE 'overlapped_building_id_list = %', overlapped_building_id_list;
            IF overlapped_building_id_list IS NOT NULL THEN
               FOREACH overlapped_building_id IN ARRAY overlapped_building_id_list LOOP
                  IF overlapped_building_id <> ANY(building_id_list) THEN
                     building_id_list := building_id_list || ARRAY[overlapped_building_id];
                  END IF;
               END LOOP;
            END IF;
            --RAISE NOTICE 'building_id_list after = %', building_id_list;

            -- Update table column block_pieces_id
            SELECT count(*) INTO count_intersec_buildings FROM public.phase04_buildings_of_a_block_by_mosaic_intersect;
            --RAISE NOTICE 'count_intersec_buildings = %', count_intersec_buildings;
            --IF count_intersec_buildings > 0 THEN
            UPDATE public.phase04_buildings_of_a_block_by_mosaic AS b
            SET block_pieces_id = loop_counter
            FROM public.phase04_buildings_of_a_block_by_mosaic_intersect AS i
            WHERE b.id = i.building_id2 OR b.id = building_id;
               --RAISE NOTICE '- UPDATED - UPDATED - UPDATED - UPDATED - UPDATED - ';
            --ELSE
               --building_id := 0;
               --RAISE NOTICE 'NO UPDATE - NO UPDATE - NO UPDATE - NO UPDATE';
            --END IF;

            -- Remove just processed building_id
            building_id_list := array_remove(building_id_list, building_id);

         --RAISE NOTICE '';
 
         END LOOP; -- END WHILE array_length

      --EXIT;
      END LOOP; -- End outer while

      -- Save mosaic values
      DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_original;
      CREATE TABLE public.phase04_buildings_of_a_block_original AS
      SELECT * FROM public.phase04_buildings_of_a_block;
      DROP TABLE IF EXISTS public.phase04_buildings_of_a_block;
      CREATE TABLE public.phase04_buildings_of_a_block AS
      SELECT o.id, o.block_id, o.geometry,  COALESCE(mo.block_pieces_id, o.block_pieces_id) AS block_pieces_id
         FROM public.phase04_buildings_of_a_block_original as o 
         LEFT JOIN public.phase04_buildings_of_a_block_by_mosaic as mo ON o.id = mo.id;

      --IF loop_counter > 2 THEN EXIT; END IF; --> CHECK THIS ERROR

   END IF; -- END IF count_buildings_in_mosaic
   END LOOP; -- End for

   -- Save permanently into chained_buildings_by_block schema
   --DROP SCHEMA IF EXISTS chained_buildings_by_block CASCADE;
   CREATE SCHEMA IF NOT EXISTS chained_buildings_by_block;
   EXECUTE 'DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id_var || ';
            CREATE TABLE chained_buildings_by_block.block_id_' || block_id_var || ' AS
	           SELECT * FROM public.phase04_buildings_of_a_block';

   -- Drop temporary tables 
   DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_by_mosaic_intersect;
   DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_by_mosaic;
   DROP TABLE IF EXISTS public.phase04_buildings_no_validation_needed;
   DROP TABLE IF EXISTS public.phase04_buildings_of_a_block;
   DROP TABLE IF EXISTS public.phase04_buildings_of_a_block_original;
   DROP TABLE IF EXISTS public.phase04_buildings_under_validation;
   DROP TABLE IF EXISTS public.phase04_merged_close_points;
   DROP TABLE IF EXISTS public.phase04_validated_building_block;
   DROP TABLE IF EXISTS public.phase04_block_mosaic_grid;
   --DROP TABLE IF EXISTS public.phase04_geometries_unoverlapped;

END $BODY$;

----------------------------------------------------
-- PROCEDURE get_chained_buildings_by_block : END --
----------------------------------------------------


---------------------------------------------------
-- PROCEDURE validate_buildings_by_block : BEGIN --
---------------------------------------------------
CREATE OR REPLACE PROCEDURE validate_buildings_by_block(block_id INTEGER, distance_threshold_meters FLOAT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    --distance_threshold_meters FLOAT := 1.0; -- Set the distance threshold in meters (change this value as needed)
    block_pieces_id_var INT;
    rows_different INT;
    rows_different_prev INT;
    unique_point_geom_var TEXT := '';
    unique_point_geom_list TEXT[] := ARRAY[]::TEXT[];
    point_geom_pair_list TEXT[] := ARRAY[]::TEXT[];
    column_exists boolean;
    --loop_counter_just_for_test INTEGER := 0; -- #TODO Delete Me!
	--column_list TEXT;
    prev_overlap_count INTEGER := -1;
    curr_overlap_count INTEGER := 0;
    column_list TEXT;
    rec RECORD;
    modified_geom1 geometry;
    modified_geom2 geometry;
BEGIN

	-- First of all, divide block into pieces of block according to chained buildings
    CALL get_chained_buildings_by_block(block_id, distance_threshold_meters); --COMMIT;
	
    -- Make a table containing the buildings to be validated and other with the ones that does not need validation
    EXECUTE '
        DROP TABLE IF EXISTS public.phase04_buildings_under_validation;
        CREATE TABLE public.phase04_buildings_under_validation AS
            SELECT id, block_id as mun_block, block_pieces_id, geometry
            FROM chained_buildings_by_block.block_id_' || block_id || '
            WHERE block_pieces_id IN (
                SELECT block_pieces_id
                FROM chained_buildings_by_block.block_id_' || block_id || '
                GROUP BY block_pieces_id
                HAVING COUNT(*) > 1  );';

    EXECUTE '
        DROP TABLE IF EXISTS public.phase04_buildings_no_validation_needed;
        CREATE TABLE public.phase04_buildings_no_validation_needed AS
            SELECT id, block_id as mun_block, block_pieces_id, geometry
            FROM chained_buildings_by_block.block_id_' || block_id || '
            WHERE block_pieces_id IN (
                SELECT block_pieces_id
                FROM chained_buildings_by_block.block_id_' || block_id || '
                GROUP BY block_pieces_id
                HAVING COUNT(*) = 1  );';

FOR block_pieces_id_var IN SELECT DISTINCT block_pieces_id FROM public.phase04_buildings_under_validation LOOP
	RAISE NOTICE ' ~~~ block_pieces_id_var = %', block_pieces_id_var;

  -- Step 1: Generate points for each building geometry with automatic point IDs starting from 1 --
  -------------------------------------------------------------------------------------------------

	-- Create building_points relation
    DROP TABLE IF EXISTS public.phase04_building_points_temp;
    CREATE TABLE public.phase04_building_points_temp AS
    SELECT 
            id AS building_id,
            make_array_counter(regexp_replace(
            St_AsText(geometry),
            '[^,]+',  
            '',       
            'g'       
             )) as points_id,
    	    string_to_array(regexp_replace(
            St_AsText(geometry),
            '[^0-9\-,\. ]',  -- Regular expression pattern to keep only specified characters
            '',              -- Replacement string (empty string to remove unwanted characters)
            'g'              -- 'g' flag for global replacement
             ),',') as points_geom,
    	 geometry as geom_original_google
        FROM public.phase04_buildings_under_validation
        WHERE block_pieces_id_var = block_pieces_id; -- mun_block = block_id;--1505379
    DROP TABLE IF EXISTS public.phase04_building_points;
    CREATE TABLE public.phase04_building_points AS
    SELECT
        building_id,
    	point_id,
        ST_GeomFromText('POINT(' || point_geom || ')',4326) AS point_geom,
        geom_original_google
    FROM (
        SELECT
            building_id,
    	unnest(points_id) AS point_id,
            unnest(points_geom) AS point_geom,
    	geom_original_google
        FROM
            public.phase04_building_points_temp
    ) AS sub
    GROUP BY
        building_id, point_id, point_geom, geom_original_google
    ORDER BY building_id, point_id;
    DROP TABLE IF EXISTS public.phase04_building_points_temp;

    -- Glue points close to buildings to them
    DROP TABLE IF EXISTS public.phase04_aggregated_buildings;
    CREATE TABLE public.phase04_aggregated_buildings AS
    SELECT
        id as building_id,
        public.try_convert_to_geometry(
		   public.aggregate_points_and_construct_geometry(
		      ST_AsText(geometry), 
		      ARRAY(
                 SELECT ST_AsText(point_geom)
                 FROM public.phase04_building_points
                 WHERE ( ST_DWithin(ST_Transform(geometry, 3857), ST_Transform(point_geom, 3857), distance_threshold_meters)
		                 --AND ST_Distance(ST_Transform(geometry, 3857), ST_Transform(point_geom, 3857)) > 0 -- Remove me
		               ) OR ST_Contains(geometry,point_geom)
              )
           )
		, geometry) as geom_glued_points_to_buildings
    FROM
        public.phase04_buildings_under_validation
    WHERE
        block_pieces_id_var = block_pieces_id; -- mun_block = block_id;

	-- RE-Create building_points relation (amount of points where modified in the buildings)
    DROP TABLE IF EXISTS public.phase04_building_points_temp;
    CREATE TABLE public.phase04_building_points_temp AS
    SELECT 
            building_id,
            make_array_counter(regexp_replace(
            St_AsText(geom_glued_points_to_buildings),
            '[^,]+',  
            '',       
            'g'       
             )) as points_id,
    	    string_to_array(regexp_replace(
            St_AsText(geom_glued_points_to_buildings),
            '[^0-9\-,\. ]',  -- Regular expression pattern to keep only specified characters
            '',              -- Replacement string (empty string to remove unwanted characters)
            'g'              -- 'g' flag for global replacement
             ),',') as points_geom,
    	 geom_glued_points_to_buildings-- as geom_original_google
        FROM public.phase04_aggregated_buildings;
    DROP TABLE IF EXISTS public.phase04_building_points;
    CREATE TABLE public.phase04_building_points AS
    SELECT
        building_id,
    	point_id,
        ST_GeomFromText('POINT(' || point_geom || ')',4326) AS point_geom,
        geom_glued_points_to_buildings --geom_original_google
    FROM (
        SELECT
            building_id,
    	unnest(points_id) AS point_id,
            unnest(points_geom) AS point_geom,
    	geom_glued_points_to_buildings -- geom_original_google
        FROM
            public.phase04_building_points_temp
    ) AS sub
    GROUP BY
        building_id, point_id, point_geom, geom_glued_points_to_buildings --geom_original_google
    ORDER BY building_id, point_id;
    DROP TABLE IF EXISTS public.phase04_building_points_temp;


    -- Join phase04_aggregated_buildings to phase04_building_points
    --DROP TABLE IF EXISTS public.phase04_building_points_backup;
    --CREATE TABLE public.phase04_building_points_backup AS
    --  SELECT * FROM public.phase04_building_points;
    --DROP TABLE IF EXISTS public.phase04_building_points;
    --CREATE TABLE public.phase04_building_points AS
    --  SELECT * FROM public.phase04_building_points_backup;
    --    --LEFT JOIN public.phase04_aggregated_buildings as ab on bp.building_id = ab.building_id;
    --DROP TABLE IF EXISTS public.phase04_building_points_backup;
    --DROP INDEX IF EXISTS idx_0001; CREATE INDEX idx_0001 ON public.phase04_building_points(building_id);
    --DROP INDEX IF EXISTS idx_0002; CREATE INDEX idx_0002 ON public.phase04_building_points(point_id);
    --DROP INDEX IF EXISTS idx_0003; CREATE INDEX idx_0003 ON public.phase04_building_points USING GIST(point_geom);
    --DROP INDEX IF EXISTS idx_0004; CREATE INDEX idx_0004 ON public.phase04_building_points USING GIST(geom_original_google);
    --DROP INDEX IF EXISTS idx_0005; CREATE INDEX idx_0005 ON public.phase04_building_points USING GIST(geom_glued_points_to_buildings);
   

   -- Step 2: BEGIN: JOIN POINTS within a given distance at their intermediary point --
   ------------------------------------------------------------------------------------
--/*
    -- Loop until no rows left in phase04_building_points_modified or rows_different does not decrease
    WHILE TRUE LOOP    
	    
	    --loop_counter_just_for_test := loop_counter_just_for_test + 1;
	    
        -- Try to count the number of rows in phase04_building_points_modified
        BEGIN
            --EXECUTE 'SELECT COUNT(*) FROM public.phase04_building_points_modified' INTO rows_modified;
            EXECUTE 'SELECT COUNT(*)
                     FROM (
                            SELECT *
                            FROM public.phase04_building_points_modified
                            EXCEPT
                            SELECT *
                            FROM public.phase04_building_points
                           ) AS diff_rows
                     ' INTO rows_different;
            RAISE NOTICE '                                                                  Number of differing rows: %', rows_different;
        EXCEPTION
            WHEN undefined_table THEN
                -- Table does not exist, set rows_different to a small value
                rows_different := -1;
        END;

        -- Print message
        RAISE NOTICE '__________________________ rows_different_prev: % | rows_different: %', rows_different_prev, rows_different;

        -- MAIN CODE STARTS HERE


        -- If rows_different does not decrease (infinite loop) OR if it is the last loop
		IF ( rows_different > 0 AND rows_different >= rows_different_prev AND rows_different_prev <> -1 ) OR rows_different = 0 THEN -- TRUE THEN
		   RAISE NOTICE '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
		   RAISE NOTICE '                                             ';
		   RAISE NOTICE '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
           -- Catch pairs of close points and the centroid between them
           DROP TABLE IF EXISTS public.phase04_merged_close_points;
           CREATE  TABLE public.phase04_merged_close_points AS
           SELECT DISTINCT
              unnested_geom as replace_from,
              p.point_geom_centroid as replace_to
           FROM (
                  SELECT DISTINCT
                      insert_to_list(ST_AsText(a.point_geom,4326) || ', ' || ST_AsText(b.point_geom,4326)) AS point_geom_list,
                      ST_Centroid(ST_Union(
                          ST_Transform(ST_Buffer(ST_Transform(a.point_geom, 3857), distance_threshold_meters/2), 4326),
                          ST_Transform(ST_Buffer(ST_Transform(b.point_geom, 3857), distance_threshold_meters/2), 4326)
                      )) AS point_geom_centroid
                  FROM
                      public.phase04_building_points a
                  JOIN
                      public.phase04_building_points b
                  ON
                      a.point_geom < b.point_geom
                      AND ST_Intersects(
                          ST_Transform(ST_Buffer(ST_Transform(a.point_geom, 3857), distance_threshold_meters/1.9), 4326),
                          ST_Transform(ST_Buffer(ST_Transform(b.point_geom, 3857), distance_threshold_meters/1.9), 4326)
                      ) 
                  ) p CROSS JOIN LATERAL unnest(p.point_geom_list) AS unnested_geom;

           -- Update point positions
           UPDATE public.phase04_building_points AS bp
           SET point_geom = mcp.replace_to
           FROM public.phase04_merged_close_points AS mcp
           WHERE ST_AsText(bp.point_geom) = mcp.replace_from;

           -- Remove duplicated points that are in sequence in the same building
           DROP TABLE IF EXISTS public.phase04_building_points_temp;
           CREATE TABLE public.phase04_building_points_temp AS
              SELECT * FROM public.phase04_building_points;
           DROP TABLE IF EXISTS public.phase04_building_points;
           CREATE TABLE public.phase04_building_points AS
              SELECT building_id, point_id, point_geom, 
                 try_convert_to_geometry(
                    ST_AsText(
                       ST_Transform(
                          ST_GeomFromText(
                             public.merge_close_points_same_building(
                                ST_AsText(
                                   ST_Transform(
                                      geom_glued_points_to_buildings
                                   ,3857)
                                )
                             )
                          ,3857)
                       ,4326)
                    ) 
                 ,geom_glued_points_to_buildings ) as geom_glued_points_to_buildings
              FROM public.phase04_building_points_temp;
           DROP TABLE IF EXISTS public.phase04_building_points_temp;
            -- Recover original table
            --RAISE NOTICE ' : -- previous table was recoverd -- :';
            --DROP TABLE IF EXISTS public.phase04_building_points;
            --CREATE TABLE public.phase04_building_points AS
            --SELECT * FROM public.phase04_building_points_original;
            -- Find points that have other points (more than one) closer to them
/*
			DROP TABLE IF EXISTS public.phase04_close_points_pair;
            CREATE TABLE public.phase04_close_points_pair AS 
              SELECT
                  DISTINCT LEAST(ST_AsText(p1.point_geom), ST_AsText(p2.point_geom)) AS point_geom_from,
                  GREATEST(ST_AsText(p1.point_geom), ST_AsText(p2.point_geom)) AS point_geom_to,
				  ST_Centroid(ST_Union(
                     ST_Transform(ST_Buffer(ST_Transform(p1.point_geom, 3857), distance_threshold_meters/1.9), 4326),
                     ST_Transform(ST_Buffer(ST_Transform(p2.point_geom, 3857), distance_threshold_meters/1.9), 4326)
                  )) AS point_geom_centroid
              FROM
                  public.phase04_building_points p1
              INNER JOIN
                  public.phase04_building_points p2 ON p1.point_id <> p2.point_id
              WHERE
                  ST_DWithin(ST_Transform(p1.point_geom, 3857), ST_Transform(p2.point_geom, 3857), distance_threshold_meters)
                  AND ST_Distance(ST_Transform(p1.point_geom, 3857), ST_Transform(p2.point_geom, 3857)) > 0
              ORDER BY
                  point_geom_from, point_geom_to;
			DROP TABLE IF EXISTS public.phase04_close_points_pair_unique_point_geom_list;
            CREATE TABLE public.phase04_close_points_pair_unique_point_geom_list AS 
              SELECT DISTINCT * FROM (
                SELECT 
                  point_geom_from as unique_point_geom
                FROM 
                  public.phase04_close_points_pair
                UNION ALL
                SELECT 
                  point_geom_to as unique_point_geom_list
                FROM 
                  public.phase04_close_points_pair
              );

*/

              --EXIT;


 /*           WHILE TRUE LOOP
                SELECT ARRAY_AGG(unique_point_geom) INTO unique_point_geom_list FROM public.phase04_close_points_pair_unique_point_geom_list;
                SELECT ARRAY_AGG(ARRAY[point_geom_from, point_geom_to]) INTO point_geom_pair_list FROM public.phase04_close_points_pair;
                RAISE NOTICE 'point_geom_pair_list = %',point_geom_pair_list;
                RAISE NOTICE 'chain_close_points = %',public.chain_close_points(point_geom_pair_list);

                FOREACH unique_point_geom_var IN ARRAY unique_point_geom_list LOOP
				    RAISE NOTICE 'unique_point_geom_var = %',unique_point_geom_var;
                    unique_point_geom_list := unique_point_geom_list || unique_point_geom_var;
                    SELECT ARRAY_AGG(point_geom_to) INTO point_geom_list FROM public.phase04_close_points_pair WHERE point_geom_from = unique_point_geom_var;
		    		RAISE NOTICE 'unique_point_geom_list = %',unique_point_geom_list;
                END LOOP;
                EXIT;
            END LOOP;
*/
        END IF;
        -- Check if no rows left or rows_different does not decrease
        IF rows_different = 0 THEN
	        -- Reconstruct buildings based on their new aligned points
            DROP TABLE IF EXISTS public.phase04_validated_building_block;
            CREATE TABLE public.phase04_validated_building_block AS
              SELECT
                building_id as id, -- KEEP IT COMMENTED
				--'yes' as building,
                --ST_MakePolygon(ST_AddPoint(ST_MakeLine(points), points[1])) AS geom
                --ST_GeomFromText(public.(ST_AsText(geom_original_google), points) ,4326) as geom
                try_convert_to_geometry(public.remake_geometry(ST_AsText(geom_glued_points_to_buildings), points), geom_glued_points_to_buildings ) as geom
              FROM (
                SELECT
                    building_id,
                    array_agg(ST_AsText(point_geom) ORDER BY point_id) AS points,
                    --geom_original_google,
                    geom_glued_points_to_buildings
                FROM
                    public.phase04_building_points
                GROUP BY
                    building_id, /*geom_original_google,*/ geom_glued_points_to_buildings
              ) AS building_points_agg;
			  
            -- Update geometry buildings
            EXECUTE 'DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id || '_temp;
                     CREATE TABLE chained_buildings_by_block.block_id_' || block_id || '_temp AS
                        SELECT *
                        FROM chained_buildings_by_block.block_id_' || block_id || ';                 
                     DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id || ';
                     CREATE TABLE chained_buildings_by_block.block_id_' || block_id || ' AS
                        SELECT bbb.id, bbb.block_id, bbb.block_pieces_id, TRUE as validated, 
                               COALESCE(vbb.geom, bbb.geometry) as geometry
                        FROM chained_buildings_by_block.block_id_' || block_id || '_temp as bbb
                        LEFT JOIN public.phase04_validated_building_block as vbb ON bbb.id = vbb.id;
					 DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id || '_temp;';
            /* EXECUTE 'DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id || '_temp;
                     CREATE TABLE chained_buildings_by_block.block_id_' || block_id || '_temp AS
                        SELECT *
                        FROM chained_buildings_by_block.block_id_' || block_id || ';                 
                     DROP TABLE IF EXISTS chained_buildings_by_block.block_id_' || block_id || ';
                     CREATE TABLE chained_buildings_by_block.block_id_' || block_id || ' AS
                        SELECT bbb.id, bbb.block_id, bbb.block_pieces_id, TRUE as validated, 
                               COALESCE(vbb.geom, bbb.geometry) as geometry
                        FROM chained_buildings_by_block.block_id_' || block_id || '_temp as bbb
                        LEFT JOIN public.phase04_validated_building_block as vbb ON bbb.id = vbb.id;'; */
            -- then...
            EXIT; -- Exit the loop / chained buildings validation finished
            ELSE RAISE NOTICE '.............................. start new cycle';
        END IF;

        DROP TABLE IF EXISTS public.phase04_point_distances_temp;
        CREATE TABLE public.phase04_point_distances_temp AS
        SELECT 
            ARRAY[ARRAY[a.building_id, a.point_id], ARRAY[b.building_id, b.point_id]] AS point_pair,
            ST_Distance(a.point_geom::geography, b.point_geom::geography) AS distance,
            ST_LineInterpolatePoint(ST_MakeLine(a.point_geom, b.point_geom), 0.5) AS intermediary_point,
            --a.geom_original_google,
            a.geom_glued_points_to_buildings
        FROM 
            public.phase04_building_points a
            CROSS JOIN public.phase04_building_points b
        WHERE 
            a.building_id < b.building_id
            AND ST_DWithin(a.point_geom, b.point_geom, distance_threshold_meters);

        DROP TABLE IF EXISTS public.phase04_point_distances;
        CREATE TABLE public.phase04_point_distances AS
        SELECT * FROM public.phase04_point_distances_temp WHERE distance > 0 AND distance < distance_threshold_meters;
        DROP TABLE IF EXISTS public.phase04_point_distances_temp;

        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0006; CREATE INDEX idx_0006 ON public.phase04_point_distances(point_pair);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0007; CREATE INDEX idx_0007 ON public.phase04_point_distances(distance);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0008; CREATE INDEX idx_0008 ON public.phase04_point_distances USING GIST(intermediary_point);

        DROP TABLE IF EXISTS public.phase04_building_points_modified;
        CREATE TABLE public.phase04_building_points_modified AS
        SELECT DISTINCT ON (building_id, point_id)
           building_id,
           point_id,
           intermediary_point AS point_geom,
           --geom_original_google,
           geom_glued_points_to_buildings
        FROM (
           SELECT 
              a.point_pair[1][1] AS building_id,
              a.point_pair[1][2] AS point_id,
              a.intermediary_point,
              --a.geom_original_google,
              a.geom_glued_points_to_buildings
           FROM 
              public.phase04_point_distances a
           UNION ALL
           SELECT 
              b.point_pair[2][1] AS building_id,
              b.point_pair[2][2] AS point_id,
              b.intermediary_point,
              --b.geom_original_google,
              b.geom_glued_points_to_buildings
           FROM 
              public.phase04_point_distances b
           ) AS combined_data
        ORDER BY building_id, point_id;  -- Optional: specify order for DISTINCT ON
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0009; CREATE INDEX idx_0009 ON public.phase04_building_points(building_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0010; CREATE INDEX idx_0010 ON public.phase04_building_points(point_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0011; CREATE INDEX idx_0011 ON public.phase04_building_points USING GIST(point_geom);
        --DROP INDEX IF EXISTS idx_0012; CREATE INDEX idx_0012 ON public.phase04_building_points USING GIST(geom_original_google);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0013; CREATE INDEX idx_0013 ON public.phase04_building_points USING GIST(geom_glued_points_to_buildings);


        DROP TABLE IF EXISTS public.phase04_building_points_original;
        CREATE TABLE public.phase04_building_points_original AS
        SELECT * FROM public.phase04_building_points;
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0014; CREATE INDEX idx_0014 ON public.phase04_building_points(building_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0015; CREATE INDEX idx_0015 ON public.phase04_building_points(point_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0016; CREATE INDEX idx_0016 ON public.phase04_building_points USING GIST(point_geom);
        --DROP INDEX IF EXISTS idx_0017; CREATE INDEX idx_0017 ON public.phase04_building_points USING GIST(geom_original_google);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0018; CREATE INDEX idx_0018 ON public.phase04_building_points USING GIST(geom_glued_points_to_buildings);

        DROP TABLE IF EXISTS public.phase04_building_points;
        CREATE TABLE public.phase04_building_points AS
        SELECT 
            COALESCE(modified.building_id, original.building_id) AS building_id,
            COALESCE(modified.point_id, original.point_id) AS point_id,
            COALESCE(modified.point_geom, original.point_geom) AS point_geom,
            --original.geom_original_google,
			original.geom_glued_points_to_buildings
        FROM 
            public.phase04_building_points_original AS original
        LEFT JOIN 
            public.phase04_building_points_modified AS modified
        ON 
            original.building_id = modified.building_id
            AND original.point_id = modified.point_id;

        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0001; CREATE INDEX idx_0001 ON public.phase04_building_points(building_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0002; CREATE INDEX idx_0002 ON public.phase04_building_points(point_id);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0003; CREATE INDEX idx_0003 ON public.phase04_building_points USING GIST(point_geom);
        --DROP INDEX IF EXISTS idx_0004; CREATE INDEX idx_0004 ON public.phase04_building_points USING GIST(geom_original_google);
        --TEST_WITHOUT_INDEXES_DROP INDEX IF EXISTS idx_0005; CREATE INDEX idx_0005 ON public.phase04_building_points USING GIST(geom_glued_points_to_buildings);

        -- MAIN CODE ENDS HERE

        -- Update rows_different_prev for next iteration
        rows_different_prev := rows_different;

        --IF loop_counter_just_for_test = 9999 THEN EXIT; END IF;

    END LOOP;
--*/

END LOOP; -- END FOR block_pieces_id_var LOOP
		  
--/*-- Try to unoverlap (again!) validated geometries : BEGIN -- 
    LOOP
        -- Step 1: Count overlaps (safe version)
        BEGIN
            EXECUTE format(
            $$SELECT COUNT(*) FROM (
                SELECT a.id, b.id
                FROM (
                    SELECT id, ST_Buffer(ST_MakeValid(geometry), 0) AS geometry
                    FROM chained_buildings_by_block.block_id_%s
                    WHERE NOT ST_IsEmpty(geometry)
                ) a
                JOIN (
                    SELECT id, ST_Buffer(ST_MakeValid(geometry), 0) AS geometry
                    FROM chained_buildings_by_block.block_id_%s
                    WHERE NOT ST_IsEmpty(geometry)
                ) b
                ON a.id < b.id AND ST_Overlaps(a.geometry, b.geometry)
            ) AS overlap_result;$$, block_id, block_id)
            INTO curr_overlap_count;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING '! Error during overlap check for block_id % - skipping problematic geometries', block_id;
                curr_overlap_count := 0; -- or -1, depending on how you want to handle fallback
        END;

        RAISE NOTICE 'Current overlap count: % | Previous: %', curr_overlap_count, prev_overlap_count;

        -- Step 2: Stop if overlap count did not improve
        EXIT WHEN curr_overlap_count = 0 OR ( curr_overlap_count >= prev_overlap_count AND prev_overlap_count <> -1);

        -- Step 3: Backup current table
        EXECUTE format(
            $$DROP TABLE IF EXISTS public.phase04_validated_block_backup;
              CREATE TABLE public.phase04_validated_block_backup AS
              SELECT id, block_id, block_pieces_id, TRUE as validated, geometry
              FROM chained_buildings_by_block.block_id_%s;$$, block_id);

        -- Optional: Show column names for debugging
        SELECT string_agg(column_name, ', ')
        INTO column_list
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'phase04_validated_block_backup';
        RAISE NOTICE 'Columns: %', column_list;

        -- Step 4: Recreate phase04_validated_block
        EXECUTE format(
            $$DROP TABLE IF EXISTS public.phase04_validated_block;
              CREATE TABLE public.phase04_validated_block AS
              SELECT guo.id, guo.latitude, guo.longitude, guo.area_in_meters, guo.confidence,
                     guo.geoc_mun, guo.zone_type, guo.mun_block, guo.full_plus_code,
                     ST_RemoveRepeatedPoints(cbb.geometry) as geometry
              FROM chained_buildings_by_block.block_id_%s AS cbb
              LEFT JOIN public.phase04_geometries_unoverlapped AS guo ON cbb.id = guo.id;$$, block_id);

        -- Step 5: Reprocess overlapping geometries
        CALL modify_overlapping_geometries(block_id, 'public.phase04_validated_block');

        -- Step 6: Replace original table with new unoverlapped version
        EXECUTE format(
            $$DROP TABLE IF EXISTS chained_buildings_by_block.block_id_%s;
              CREATE TABLE chained_buildings_by_block.block_id_%s AS
              SELECT cbb.id, cbb.block_id, cbb.block_pieces_id, cbb.validated,
                     ST_RemoveRepeatedPoints(guo.geometry) as geometry
              FROM public.phase04_validated_block_backup AS cbb
              LEFT JOIN public.phase04_geometries_unoverlapped AS guo ON cbb.id = guo.id;$$, block_id, block_id);

        -- Step 7: Store current count as previous for next iteration
        prev_overlap_count := curr_overlap_count;
    END LOOP;
-- Try to unoverlap (again!) validated geometries : END --*/

-- Final adjustments: BEGIN --
--/*

  -- Create a temporary table for backup
  EXECUTE format(
            $$DROP TABLE IF EXISTS public.phase04_backup_prev_final_adj;
              CREATE TABLE public.phase04_backup_prev_final_adj AS
              SELECT *
              FROM chained_buildings_by_block.block_id_%s;$$, block_id);

  -- Truncate geometries to 7 digits after point to meet JOSM pattern
    UPDATE public.phase04_backup_prev_final_adj 
    SET geometry = osm_trunc(geometry);


  -- Geometry that intersects itself | cut at the intersection and preserve the largest area
    UPDATE public.phase04_backup_prev_final_adj 
    SET geometry = clean_self_intersections(geometry)
    WHERE upper(ST_IsValidReason(geometry)) LIKE 'SELF-INTERSECTION%';

  -- Weld a geometry's point into the line of another, when it touches
    -- Step 1: Create a temporary table to store the intersecting pairs
    CREATE TEMP TABLE temp_geom_pairs (
        id1 integer,
        id2 integer,
        geom1 geometry,
        geom2 geometry
    ) ON COMMIT DROP;

    -- Step 2: Populate temp table with pairs of intersecting geometries
    INSERT INTO temp_geom_pairs (id1, id2, geom1, geom2)
    SELECT a.id, b.id, a.geometry, b.geometry
    FROM public.phase04_backup_prev_final_adj a
    JOIN public.phase04_backup_prev_final_adj b
      ON ( ST_Intersects(ST_MakeValid(a.geometry), ST_MakeValid(b.geometry)) OR
	       ST_Touches(ST_MakeValid(a.geometry), ST_MakeValid(b.geometry))
	     )
    WHERE a.id < b.id;

    -- Step 3: Loop through each pair
    FOR rec IN SELECT * FROM temp_geom_pairs
    LOOP
        -- Show the pair being processed
        RAISE NOTICE 'Processing pair: (%,%) = [ % ; % ]', rec.id1, rec.id2, ST_AsText(rec.geom1), ST_AsText(rec.geom2);

        -- 1. Weld geom1 into geom2 → update geom2
        SELECT weld_snapped_points_into_ring(rec.geom1, rec.geom2, 0.005)
        INTO modified_geom2;
		RAISE NOTICE 'modified_geom2 = %',ST_AsText(modified_geom2);

        UPDATE public.phase04_backup_prev_final_adj 
        SET geometry = modified_geom2 
        WHERE id = rec.id2;

        -- 2. Weld geom2 (updated) into geom1 → update geom1
        SELECT weld_snapped_points_into_ring(modified_geom2, rec.geom1, 0.005)
        INTO modified_geom1;
		RAISE NOTICE 'modified_geom1 = %',ST_AsText(modified_geom1);

        UPDATE public.phase04_backup_prev_final_adj 
        SET geometry = modified_geom1 
        WHERE id = rec.id1;
    END LOOP;

  -- (Again!) Geometry that intersects itself | cut at the intersection and preserve the largest area
  UPDATE public.phase04_backup_prev_final_adj 
  SET geometry = clean_self_intersections(geometry)
  WHERE upper(ST_IsValidReason(geometry)) LIKE '%SELF-INTERSECTION%';

  -- Remove repeated points from multipolygons
  UPDATE public.phase04_backup_prev_final_adj 
  SET geometry = remove_repeated_points(geometry)
  WHERE has_repeated_points(geometry);

  -- (Again!) Truncate geometries to 7 digits after point to meet JOSM pattern
  UPDATE public.phase04_backup_prev_final_adj 
  SET geometry = osm_trunc(geometry);

  -- Delete geometries that are utterly inside another
  DELETE FROM public.phase04_backup_prev_final_adj AS bpfa1
  USING public.phase04_backup_prev_final_adj AS bpfa2
  WHERE bpfa1.id <> bpfa2.id
    AND ST_Within(bpfa1.geometry, bpfa2.geometry);
	  
  -- Delete geometries that doesn't form an area (2 points or less)
  DELETE 
  FROM public.phase04_backup_prev_final_adj
  WHERE contains2ptsorless(geometry);

  -- Reconstruct final table
  EXECUTE format(
            $$DROP TABLE IF EXISTS chained_buildings_by_block.block_id_%s;
              CREATE TABLE chained_buildings_by_block.block_id_%s AS
              SELECT * --id, block_id, block_pieces_id, geometry
              FROM public.phase04_backup_prev_final_adj;$$, block_id, block_id);

  --*/
  


--*/
-- Final adjustments: END --



-- Drop tempory tables
DROP TABLE IF EXISTS public.phase04_aggregated_buildings;
DROP TABLE IF EXISTS public.phase04_building_points;
DROP TABLE IF EXISTS public.phase04_building_points_modified;
DROP TABLE IF EXISTS public.phase04_building_points_original;
DROP TABLE IF EXISTS public.phase04_close_points_pair;
DROP TABLE IF EXISTS public.phase04_close_points_pair_unique_point_geom;
DROP TABLE IF EXISTS public.phase04_merged_points;
DROP TABLE IF EXISTS public.phase04_building_points_temp;
--DROP TABLE IF EXISTS public.phase04_validated_building_block;
DROP TABLE IF EXISTS public.phase04_point_distances;
DROP TABLE IF EXISTS public.phase04_buildings_no_validation_needed;
DROP TABLE IF EXISTS public.phase04_buildings_under_validation;
DROP TABLE IF EXISTS public.phase04_merged_close_points;
DROP TABLE IF EXISTS public.phase04_validated_block_backup;
DROP TABLE IF EXISTS public.phase04_validated_block;
--DROP TABLE IF EXISTS public.phase04_geometries_unoverlapped;

END $BODY$;

-------------------------------------------------
-- PROCEDURE validate_buildings_by_block : END --
-------------------------------------------------

------------------------------------------------------------
-- PROCEDURE validate_whole_municipality_or_state : BEGIN --
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE validate_whole_municipality_or_state(geoc_mun_or_uf INTEGER, distance_threshold_meters FLOAT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    municipality_geocodigo INTEGER;
    municipality_block INTEGER;
    block_geom GEOMETRY;
    loop_counter INTEGER := 0;
BEGIN
   EXECUTE 'DROP TABLE IF EXISTS public._geoc_mun_or_uf_' || geoc_mun_or_uf;
   IF geoc_mun_or_uf < 100 THEN -- If it is a state
      --FOR municipality_geocodigo IN SELECT DISTINCT geocodigo FROM public.municipalities_br WHERE geocodigo >= (geoc_mun_or_uf*100000) AND geocodigo < ((geoc_mun_or_uf+1)*100000) LOOP
      --RAISE NOTICE 'municipality_geocodigo = %', municipality_geocodigo;
      --END LOOP;
   ELSE -- If it is a municipality
     FOR municipality_block IN SELECT DISTINCT id FROM public.blocks_br WHERE geocodigo = geoc_mun_or_uf LOOP
        -- Count loops
        loop_counter := loop_counter + 1;
        RAISE NOTICE 'municipality_block = %', municipality_block;
        -- Validate buildings in a block
        CALL validate_buildings_by_block(municipality_block,distance_threshold_meters); --COMMIT;
        -- Save block to geoc_mun_or_uf building table
        IF loop_counter = 1 THEN 
           EXECUTE 'DROP TABLE IF EXISTS public._geoc_mun_or_uf_' || geoc_mun_or_uf || ';' ||
                   'CREATE TABLE public._geoc_mun_or_uf_' || geoc_mun_or_uf || ' AS
                      SELECT * FROM public.phase04_validated_building_block;';
        ELSE 
           EXECUTE 'DROP TABLE IF EXISTS public._geoc_mun_or_uf_' || geoc_mun_or_uf || '_temp;' ||
                   'CREATE TABLE public._geoc_mun_or_uf_' || geoc_mun_or_uf || '_temp AS
                      SELECT * FROM public._geoc_mun_or_uf_' || geoc_mun_or_uf || ';' ||
                   'DROP TABLE IF EXISTS public._geoc_mun_or_uf_' || geoc_mun_or_uf || ';' ||
                   'CREATE TABLE public._geoc_mun_or_uf_' || geoc_mun_or_uf || ' AS
                      SELECT * FROM public.phase04_validated_building_block
                      UNION ALL
                      SELECT * FROM public._geoc_mun_or_uf_' || geoc_mun_or_uf || '_temp;' ||
			       'DROP TABLE IF EXISTS public._geoc_mun_or_uf_' || geoc_mun_or_uf || '_temp;';

        END IF;
        --IF loop_counter = 3 THEN EXIT; END IF;
     END LOOP;
   END IF;
END $BODY$;

----------------------------------------------------------
-- PROCEDURE validate_whole_municipality_or_state : END --
----------------------------------------------------------


-----------------------------------------------------
-- PROCEDURE make_buildings_block_by_block : BEGIN --
-----------------------------------------------------

/* DEPRECIATED / Replaced by code direct on Python


CREATE OR REPLACE PROCEDURE make_buildings_block_by_block(MUNGEOCODE INTEGER)
 LANGUAGE plpgsql
 AS $BODY$
 DECLARE
    municipality_geocodigo INTEGER;
	municipality_block_list INTEGER[];
    municipality_block INTEGER;
    block_geom GEOMETRY;
	block_counter INTEGER;
    loop_counter INTEGER := 0;
 BEGIN
 
	 -- Create schema
	 EXECUTE 'CREATE SCHEMA IF NOT EXISTS for_tasking_manager;';

     -- List buildings within each block in a municipality
     EXECUTE format('DROP TABLE IF EXISTS for_tasking_manager.municipality_%s;
                     CREATE TABLE for_tasking_manager.municipality_%s AS
                        SELECT bu.*, bl.geom 
                         FROM (
                           SELECT count(id) as building_counter, mun_block as block_id
                           FROM public.brazil_04
                           WHERE geoc_mun = %s
                           GROUP BY mun_block
                           ORDER BY building_counter DESC
                         ) AS bu 
                         LEFT JOIN public.blocks_br AS bl ON bu.block_id = bl.id
                         WHERE bu.building_counter > 0;',
                    MUNGEOCODE, MUNGEOCODE, MUNGEOCODE
             );

     -- Get the list of blocks
     EXECUTE 'SELECT ARRAY(SELECT DISTINCT block_id FROM for_tasking_manager.municipality_' || MUNGEOCODE || ')' INTO municipality_block_list;
	 block_counter := array_length(municipality_block_list, 1);

     -- Validate buildings block by block
     FOREACH municipality_block IN ARRAY municipality_block_list LOOP
	    loop_counter := loop_counter + 1;
        RAISE NOTICE '************************** Adjusting buildings for block % (%/%)', municipality_block, loop_counter, block_counter;
		CALL validate_buildings_by_block(municipality_block,0.8); --COMMIT;
		-- Move block to final schema
		EXECUTE format('DROP TABLE IF EXISTS for_tasking_manager.b%s; 
		                CREATE TABLE for_tasking_manager.b%s AS 
						  SELECT %L as building, geometry as geom FROM chained_buildings_by_block.block_id_%s;
						DROP TABLE IF EXISTS chained_buildings_by_block.block_id_%s; ',
                       municipality_block, municipality_block, 'yes', municipality_block, municipality_block
					  );
     END LOOP;
	 
     -- Drop temporary schema
     EXECUTE 'DROP SCHEMA IF EXISTS chained_buildings_by_block CASCADE;';

END $BODY$;

DEPRECIATED _END */
---------------------------------------------------
-- PROCEDURE make_buildings_block_by_block : END --
---------------------------------------------------


-- Call procedures test
--CALL validate_buildings_by_block(2256321,0.8); COMMIT; -- 81437 2268981 | block with more buildings (50742 buildings) = 1503475
--CALL validate_whole_municipality_or_state(4304630,0.75); COMMIT; -- 4304630 | smallest mun 3157336
--CALL make_buildings_block_by_block(5201207); COMMIT; -- TEST HERE
