
--/*
-- Define each building's municipality location
DROP TABLE IF EXISTS public.brazil_02;
CREATE TABLE public.brazil_02 AS
SELECT
    b.*,
    m.geocodigo
FROM
    public.brazil_01 AS b
LEFT JOIN
    public.municipalities_br AS m
ON
    ST_Within(b.geometry, m.geom)
--LIMIT 1000000
;
-- Delete rows where geocodigo is null (not in Brazil)
DELETE FROM public.brazil_02 WHERE geocodigo is null;
-- (re)Load indexes
DROP INDEX IF EXISTS idx_id_02; CREATE INDEX idx_id_02 ON public.brazil_02(id);
DROP INDEX IF EXISTS idx_geometry_spatial_02; CREATE INDEX idx_geometry_spatial_02 ON public.brazil_02 USING GIST(geometry);
--*/


--/*
-- Define if each building is located in rural or urban zone
DROP TABLE IF EXISTS public.brazil_03;
CREATE TABLE public.brazil_03 AS
SELECT
    b.*,
    u.tipo
FROM
    public.brazil_02 AS b
LEFT JOIN
    public.urban_areas_br AS u
ON
    ST_Within(b.geometry, u.geom)
--LIMIT 1000000
;
-- (re)Load indexes
DROP INDEX IF EXISTS idx_id_03; CREATE INDEX idx_id_03 ON public.brazil_03(id);
DROP INDEX IF EXISTS idx_geometry_spatial_03; CREATE INDEX idx_geometry_spatial_03 ON public.brazil_03 USING GIST(geometry);
--*/

--------------------------------------------------------------------------
-- Organize buildings by block -----------
--------------------------------------------------------------------------
/*
-- Create bulding_block relation
DROP TABLE IF EXISTS public.building_block;
CREATE TABLE public.building_block (
	building_id bigint,
	block_id bigint
);
*/
-- Fill building_block table
CREATE OR REPLACE PROCEDURE join_buildings_by_block(geoc_min INTEGER, geoc_max INTEGER)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    municipality_geocodigo INTEGER;
    block_geom GEOMETRY;
BEGIN
    FOR municipality_geocodigo IN SELECT DISTINCT geocodigo FROM public.municipalities_br WHERE geocodigo >= geoc_min AND geocodigo <= geoc_max LOOP
        BEGIN  -- Start a block to catch exceptions
            -- Print message
            RAISE NOTICE 'Processing municipality_geocodigo: %', municipality_geocodigo;
            
            -- Drop and create temporary table for current municipality_geocodigo
            EXECUTE 'DROP TABLE IF EXISTS public.building_block_' || municipality_geocodigo;
            EXECUTE 'CREATE TABLE public.building_block_' || municipality_geocodigo || ' AS 
                     SELECT br.id as building_id, bl.id as block_id
                     FROM ( 
                     SELECT * FROM public.brazil_03 WHERE geocodigo = ' || municipality_geocodigo || '
                     ) as br
                     LEFT JOIN (
                     SELECT * FROM public.blocks_br WHERE geocodigo = ' || municipality_geocodigo || '
                     ) as bl ON ST_Within(ST_Centroid(br.geometry), bl.geom)
                     --LIMIT 1
                     ;';
            
            -- Insert data from temporary table into main table
            EXECUTE 'INSERT INTO public.building_block (building_id,block_id)
                     SELECT building_id,block_id FROM public.building_block_' || municipality_geocodigo;
            
            -- Drop temporary table
            EXECUTE 'DROP TABLE IF EXISTS public.building_block_' || municipality_geocodigo;
        EXCEPTION  -- Catch any exceptions
            WHEN others THEN  -- Catch any type of exception
                -- Print error message
                RAISE NOTICE 'Error processing municipality_geocodigo %: %', municipality_geocodigo, SQLERRM;
                -- You can also log the error, rollback the transaction, or take other actions as needed
        END;  -- End of exception block
    END LOOP;
END $BODY$;


-- Call procedure that fills the relation building_block
--   Call each state and Commit the transaction to make changes permanent
--   Run each CALL once a time
CALL join_buildings_by_block(1200000,1299999); COMMIT; -- Acre
CALL join_buildings_by_block(2700000,2799999); COMMIT; -- Alagoas
CALL join_buildings_by_block(1600000,1699999); COMMIT; -- Amapá
CALL join_buildings_by_block(1300000,1399999); COMMIT; -- Amazonas
CALL join_buildings_by_block(2900000,2999999); COMMIT; -- Bahia
CALL join_buildings_by_block(2300000,2399999); COMMIT; -- Ceará
CALL join_buildings_by_block(5300000,5399999); COMMIT; -- Distrito Federal
CALL join_buildings_by_block(3200000,3299999); COMMIT; -- Espírito Santo
CALL join_buildings_by_block(5200000,5299999); COMMIT; -- Goiás
CALL join_buildings_by_block(2100000,2199999); COMMIT; -- Maranhão
CALL join_buildings_by_block(5100000,5199999); COMMIT; -- Mato Grosso
CALL join_buildings_by_block(5000000,5099999); COMMIT; -- Mato Grosso do Sul
CALL join_buildings_by_block(3100000,3199999); COMMIT; -- Minas Gerais
CALL join_buildings_by_block(1500000,1599999); COMMIT; -- Pará
CALL join_buildings_by_block(2500000,2599999); COMMIT; -- Paraíba
CALL join_buildings_by_block(4100000,4199999); COMMIT; -- Paraná
CALL join_buildings_by_block(2600000,2699999); COMMIT; -- Pernambuco
CALL join_buildings_by_block(2200000,2299999); COMMIT; -- Piauí
CALL join_buildings_by_block(2400000,2499999); COMMIT; -- Rio Grande do Norte
CALL join_buildings_by_block(4300000,4399999); COMMIT; -- Rio Grande do Sul
CALL join_buildings_by_block(3300000,3399999); COMMIT; -- Rio de Janeiro
CALL join_buildings_by_block(1100000,1199999); COMMIT; -- Rondônia
CALL join_buildings_by_block(1400000,1499999); COMMIT; -- Roraima
CALL join_buildings_by_block(4200000,4299999); COMMIT; -- Santa Catarina
CALL join_buildings_by_block(3500000,3599999); COMMIT; -- São Paulo
CALL join_buildings_by_block(2800000,2899999); COMMIT; -- Sergipe
CALL join_buildings_by_block(1700000,1799999); COMMIT; -- Tocantins

--*/

------------------------------------------------------------------------------------
-- BEGIN: COMPLETE NULL block_ids
------------------------------------------------------------------------------------

DELETE FROM public.building_block WHERE block_id is NULL;

DROP TABLE IF EXISTS public.temp2_null_building_block_join_brazil_03;
CREATE TABLE public.temp2_null_building_block_join_brazil_03 AS
SELECT bubl.*, brfi.*
FROM public.building_block as bubl
RIGHT JOIN public.brazil_03 as brfi ON bubl.building_id = brfi.id
WHERE bubl.building_id IS NULL
--LIMIT 10
;
DROP INDEX IF EXISTS tmp2_bbjbb_idx_id; CREATE INDEX tmp2_bbjbb_idx_id ON public.temp2_null_building_block_join_brazil_03(id);
DROP INDEX IF EXISTS tmp2_bbjbb_idx_geoc_mun; CREATE INDEX tmp2_bbjbb_idx_geoc_mun ON public.temp2_null_building_block_join_brazil_03(geoc_mun);
DROP INDEX IF EXISTS tmp2_bbjbb_idx_geometry_spatial; CREATE INDEX tmp2_bbjbb_idx_geometry_spatial ON public.temp2_null_building_block_join_brazil_03 USING GIST(geometry);


-- Fill building_block table
CREATE OR REPLACE PROCEDURE join_buildings_by_block(geoc_min INTEGER, geoc_max INTEGER)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    municipality_geocodigo INTEGER;
    block_geom GEOMETRY;
BEGIN
    FOR municipality_geocodigo IN SELECT DISTINCT geocodigo FROM public.municipalities_br WHERE geocodigo >= geoc_min AND geocodigo <= geoc_max LOOP
        BEGIN  -- Start a block to catch exceptions
            -- Print message
            RAISE NOTICE 'Processing municipality_geocodigo: %', municipality_geocodigo;
            
            -- Drop and create temporary table for current municipality_geocodigo
            EXECUTE 'DROP TABLE IF EXISTS public.building_block_' || municipality_geocodigo;
            EXECUTE 'CREATE TABLE public.building_block_' || municipality_geocodigo || ' AS 
                     SELECT br.id as building_id, bl.id as block_id
                     FROM ( 
                     SELECT * FROM public.temp2_null_building_block_join_brazil_03 WHERE geocodigo = ' || municipality_geocodigo || '
                     ) as br
                     LEFT JOIN (
                     SELECT * FROM public.blocks_br_no_buffer_1p3 WHERE geocodigo = ' || municipality_geocodigo || '
                     ) as bl ON ST_Within(ST_Centroid(br.geometry), bl.geom)
                     --LIMIT 1
                     ;';
            
            -- Insert data from temporary table into main table
            EXECUTE 'INSERT INTO public.building_block (building_id,block_id)
                     SELECT building_id,block_id FROM public.building_block_' || municipality_geocodigo;
            
            -- Drop temporary table
            EXECUTE 'DROP TABLE IF EXISTS public.building_block_' || municipality_geocodigo;
        EXCEPTION  -- Catch any exceptions
            WHEN others THEN  -- Catch any type of exception
                -- Print error message
                RAISE NOTICE 'Error processing municipality_geocodigo %: %', municipality_geocodigo, SQLERRM;
                -- You can also log the error, rollback the transaction, or take other actions as needed
        END;  -- End of exception block
    END LOOP;
END $BODY$;


-- Call procedure that fills the relation building_block
--   Call each state and Commit the transaction to make changes permanent

--/*
CALL join_buildings_by_block(1200000,1299999); COMMIT; -- Acre
CALL join_buildings_by_block(2700000,2799999); COMMIT; -- Alagoas
CALL join_buildings_by_block(1600000,1699999); COMMIT; -- Amapá
CALL join_buildings_by_block(1300000,1399999); COMMIT; -- Amazonas
CALL join_buildings_by_block(2900000,2999999); COMMIT; -- Bahia
CALL join_buildings_by_block(2300000,2399999); COMMIT; -- Ceará
CALL join_buildings_by_block(5300000,5399999); COMMIT; -- Distrito Federal
CALL join_buildings_by_block(3200000,3299999); COMMIT; -- Espírito Santo
CALL join_buildings_by_block(5200000,5299999); COMMIT; -- Goiás
CALL join_buildings_by_block(2100000,2199999); COMMIT; -- Maranhão
CALL join_buildings_by_block(5100000,5199999); COMMIT; -- Mato Grosso
CALL join_buildings_by_block(5000000,5099999); COMMIT; -- Mato Grosso do Sul
CALL join_buildings_by_block(3100000,3199999); COMMIT; -- Minas Gerais
CALL join_buildings_by_block(1500000,1599999); COMMIT; -- Pará
CALL join_buildings_by_block(2500000,2599999); COMMIT; -- Paraíba
CALL join_buildings_by_block(4100000,4199999); COMMIT; -- Paraná
CALL join_buildings_by_block(2600000,2699999); COMMIT; -- Pernambuco
CALL join_buildings_by_block(2200000,2299999); COMMIT; -- Piauí
CALL join_buildings_by_block(2400000,2499999); COMMIT; -- Rio Grande do Norte
CALL join_buildings_by_block(4300000,4399999); COMMIT; -- Rio Grande do Sul
CALL join_buildings_by_block(3300000,3399999); COMMIT; -- Rio de Janeiro
CALL join_buildings_by_block(1100000,1199999); COMMIT; -- Rondônia
CALL join_buildings_by_block(1400000,1499999); COMMIT; -- Roraima
CALL join_buildings_by_block(4200000,4299999); COMMIT; -- Santa Catarina
CALL join_buildings_by_block(3500000,3599999); COMMIT; -- São Paulo
CALL join_buildings_by_block(2800000,2899999); COMMIT; -- Sergipe
CALL join_buildings_by_block(1700000,1799999); COMMIT; -- Tocantins

-- Delete null block_id
DELETE FROM public.building_block WHERE block_id is NULL;



------------------------------------------------------------------------------------
-- END: COMPLETE NULL block_ids
------------------------------------------------------------------------------------


--/*
-- Generate table table buildings with "block" field  + indexes
DROP TABLE IF EXISTS public.brazil_04;
CREATE TABLE public.brazil_04 AS
SELECT
    br03.id, br03.latitude, br03.longitude, br03.area_in_meters, br03.confidence,
    br03.geocodigo as geoc_mun, br03.tipo as zone_type, bubl.block_id as mun_block,
    br03.full_plus_code, br03.geometry
FROM public.brazil_03 br03
LEFT JOIN public.building_block bubl on br03.id = bubl.building_id
;
-- Update zone_type value
UPDATE public.brazil_04 SET zone_type = 'urban' WHERE zone_type = 'URBANO';
UPDATE public.brazil_04 SET zone_type = 'rural' WHERE zone_type is null;

-- (re)Load indexes
DROP INDEX IF EXISTS idx_id_04; CREATE INDEX idx_id_04 ON public.brazil_04(id);
DROP INDEX IF EXISTS idx_latitude_04; CREATE INDEX idx_latitude_04 ON public.brazil_04(latitude);
DROP INDEX IF EXISTS idx_longitude_04; CREATE INDEX idx_longitude_04 ON public.brazil_04(longitude);
DROP INDEX IF EXISTS idx_area_in_meters_04; CREATE INDEX idx_area_in_meters_04 ON public.brazil_04(area_in_meters);
DROP INDEX IF EXISTS idx_confidence_04; CREATE INDEX idx_confidence_04 ON public.brazil_04(confidence);
DROP INDEX IF EXISTS idx_zone_type_04; CREATE INDEX idx_zone_type_04 ON public.brazil_04(zone_type);
DROP INDEX IF EXISTS idx_geoc_mun_04; CREATE INDEX idx_geoc_mun_04 ON public.brazil_04(geoc_mun);
DROP INDEX IF EXISTS idx_mun_block_04; CREATE INDEX idx_mun_block_04 ON public.brazil_04(mun_block);
DROP INDEX IF EXISTS idx_full_plus_code_04; CREATE INDEX idx_full_plus_code_04 ON public.brazil_04(full_plus_code);
DROP INDEX IF EXISTS idx_geometry_spatial_04; CREATE INDEX idx_geometry_spatial_04 ON public.brazil_04 USING GIST(geometry);

----------------------------------------------------------------------
-- Delete duplicates: begin
----------------------------------------------------------------------
  -- Create serial column, fill it, set it as PK and create index for the new index
  ALTER TABLE public.brazil_04 ADD COLUMN id_id SERIAL;
  ALTER TABLE public.brazil_04 ADD PRIMARY KEY (id_id);
  UPDATE public.brazil_04 SET id_id = DEFAULT;
  DROP INDEX IF EXISTS idx_id_id_04; CREATE INDEX idx_id_id_04 ON public.brazil_04(id_id); 
  
  -- List duplicated id
  DROP TABLE IF EXISTS public.brazil_04_duplicated_id;
  CREATE TABLE public.brazil_04_duplicated_id AS
  WITH ranked_buildings AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY id ORDER BY mun_block) AS row_num
      FROM public.brazil_04
  )
  SELECT * FROM public.brazil_04
  WHERE (id, mun_block) IN (
      SELECT id, mun_block
      FROM ranked_buildings
      WHERE row_num > 1
  );
  
  -- List all id_id to be deleted (always save the oldest id_id of each set of id)
  DROP TABLE IF EXISTS public.brazil_04_to_delete_list;
  CREATE TABLE public.brazil_04_to_delete_list AS
  SELECT id, id_id
  FROM public.brazil_04_duplicated_id
  WHERE id_id NOT IN (
      SELECT MIN(id_id)  -- Exclude the minimum id_id for each id
      FROM public.brazil_04_duplicated_id
      GROUP BY id
  )
  ORDER BY id;
  
  -- Remove duplicated id - APPLY
  CREATE OR REPLACE PROCEDURE remove_rows_with_duplicated_id()
  LANGUAGE plpgsql
  AS $BODY$
  DECLARE
      id_id_var INTEGER;
      total_count INTEGER;
      processed_count INTEGER := 0;
  BEGIN
      -- Get total count of distinct id_id values
      SELECT COUNT(DISTINCT id_id) INTO total_count FROM public.brazil_04_to_delete_list;
  
      FOR id_id_var IN SELECT DISTINCT id_id FROM public.brazil_04_to_delete_list LOOP
          BEGIN
              -- Increment the processed count
              processed_count := processed_count + 1;
  
              -- Print progress message with current count and total count
              RAISE NOTICE 'Processing id_id: % (%/%)', id_id_var, processed_count, total_count;
  
              -- Delete rows from the target table
              DELETE FROM public.brazil_04 WHERE id_id = id_id_var;
  
          EXCEPTION
              WHEN others THEN
                  -- Print error message
                  RAISE NOTICE 'Error processing id_id %: %', id_id_var, SQLERRM;
                  -- You can also log the error, rollback the transaction, or take other actions as needed
          END;
      END LOOP;
  END $BODY$;
  CALL remove_rows_with_duplicated_id(); COMMIT; 
  
  -- Set id as PK again, then remove temporary id_id column
  DO $$ -- Move PK from id_id to id: begin
  DECLARE
    constraint_name text;
  BEGIN
    -- Find the constraint name associated with the id_id column
    SELECT conname
    INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'public.brazil_04'::regclass
    AND conkey = ARRAY(SELECT attnum FROM pg_attribute WHERE attrelid = 'public.brazil_04'::regclass AND attname = 'id_id')
    AND contype = 'p';

    -- Drop the constraint if found
    IF constraint_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE public.brazil_04 DROP CONSTRAINT ' || quote_ident(constraint_name);
    END IF;

    -- Add new primary key constraint on the id column
    EXECUTE 'ALTER TABLE public.brazil_04 ADD PRIMARY KEY (id)';
  END $$; -- Move PK from id_id to id: end
  ALTER TABLE public.brazil_04 DROP COLUMN id_id;

  -- List buildings with too many buildings inside (>1000)
  DROP TABLE IF EXISTS public.blocks_br_with_too_many_buildings;
  CREATE TABLE public.blocks_br_with_too_many_buildings AS
    SELECT * FROM (
      SELECT count(id) as building_counter, (13.0*count(id)/51000)::float as estimated_time , mun_block as block_id
      FROM public.brazil_04
      GROUP BY mun_block
      ORDER BY building_counter DESC
      LIMIT 10000 -- Adjust to get all building_counter > 1000
    ) WHERE building_counter >= 1000;
  
  -- Drop temporary tables
  --DROP TABLE IF EXISTS public.brazil_04_duplicated_id;
  --DROP TABLE IF EXISTS public.brazil_04_to_delete_list;
  
----------------------------------------------------------------------
-- Delete duplicates: end
----------------------------------------------------------------------


-- Drop indexes from temporary tables
DROP INDEX IF EXISTS idx_id;
DROP INDEX IF EXISTS idx_geometry_spatial;
DROP INDEX IF EXISTS idx_geocodigo;
DROP INDEX IF EXISTS idx_munic_br_geom;
DROP INDEX IF EXISTS idx_gid_urban_areas_br;
DROP INDEX IF EXISTS idx_geom_urban_areas_br;
DROP INDEX IF EXISTS idx_id_02;
DROP INDEX IF EXISTS idx_geometry_spatial_02;
DROP INDEX IF EXISTS idx_id_03;
DROP INDEX IF EXISTS idx_geometry_spatial_03;
--*/
/*
-- Drop temporary tables
DROP TABLE IF EXISTS public.brazil;
DROP TABLE IF EXISTS public.brazil_01;
DROP TABLE IF EXISTS public.brazil_02;
DROP TABLE IF EXISTS public.brazil_03;

*/



