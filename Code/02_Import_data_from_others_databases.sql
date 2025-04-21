
--/*
-- Import municipalities boundaries
DROP TABLE IF EXISTS public.municipalities_br;
CREATE TABLE public.municipalities_br AS 
SELECT * -- municipality: lml_municipio_a | state: lml_unidade_federacao_a
 FROM   dblink('dbname=ibge_bc250_shapefile_2021_11_18','SELECT geocodigo, geom FROM public.lml_municipio_a')
 AS     al(geocodigo int, geom geometry);
--*/


--/*
-- Import urban areas
DROP TABLE IF EXISTS public.urban_areas_br;
CREATE TABLE public.urban_areas_br AS 
SELECT * 
 FROM   dblink('dbname=brroadclass','SELECT gid, tipo, geom FROM public.areasconurbadas')
 AS     al(gid int, tipo varchar(10), geom geometry);
ALTER TABLE public.urban_areas_br ADD PRIMARY KEY (gid);

-- Import brazilian territory
DROP TABLE IF EXISTS public.national_territory_brazil;
CREATE TABLE public.national_territory_brazil AS 
SELECT * 
 FROM   dblink('dbname=ibge_bc250_shapefile_2021_11_18','SELECT gid, geom FROM public.lml_pais_a WHERE gid=15')
 AS     al(gid int, geom geometry);

-- Import osm roads
DROP TABLE IF EXISTS public.osm_roads;
CREATE TABLE public.osm_roads AS 
SELECT * 
 FROM   dblink('dbname=brazil_20240228t212054z','SELECT pk_id, geom FROM public.osm_roads;')
 AS     al(pk_id int, geom geometry);

/* -- Check if this was used
-- Generate urban and rural blocks
DROP TABLE IF EXISTS public.blocks_brazil;
CREATE TABLE public.blocks_brazil AS 
SELECT (ST_Dump(ST_Split(national_territory_brazil.geom, osm_roads.way))).geom AS geom
FROM public.national_territory_brazil, public.osm_roads;
--*/

--/*
-- Create indexes
DROP INDEX IF EXISTS idx_geocodigo; CREATE INDEX idx_geocodigo ON public.municipalities_br(geocodigo);
DROP INDEX IF EXISTS idx_munic_br_geom; CREATE INDEX idx_munic_br_geom ON public.municipalities_br USING GIST(geom);
DROP INDEX IF EXISTS idx_gid_urban_areas_br; CREATE INDEX idx_gid_urban_areas_br ON public.urban_areas_br(gid);
DROP INDEX IF EXISTS idx_geom_urban_areas_br; CREATE INDEX idx_geom_urban_areas_br ON public.urban_areas_br USING GIST(geom);
DROP INDEX IF EXISTS idx_geom_national_territory_brazil; CREATE INDEX idx_geom_national_territory_brazil ON public.national_territory_brazil USING GIST(geom);
DROP INDEX IF EXISTS idx_pk_id_osm_roads; CREATE INDEX idx_pk_id_osm_roads ON public.osm_roads(pk_id);
DROP INDEX IF EXISTS idx_geom_osm_roads; CREATE INDEX idx_geom_osm_roads ON public.osm_roads USING GIST(geom);
--*/




--/*
--------------------------------------------------------------------------
-- Generate blocks using municipality boundaries and OSM roads -----------
--------------------------------------------------------------------------

-- Create blocks_br table
DROP TABLE IF EXISTS public.blocks_br;
CREATE TABLE public.blocks_br (
    id SERIAL PRIMARY KEY,
	geocodigo bigint,
    geom GEOMETRY
);

-- Fill blocks_br table
CREATE OR REPLACE PROCEDURE generate_blocks_br(geoc_min INTEGER, geoc_max INTEGER)
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
            EXECUTE 'DROP TABLE IF EXISTS public.blocks_br_' || municipality_geocodigo;
            EXECUTE 'CREATE TABLE public.blocks_br_' || municipality_geocodigo || ' AS 
                     SELECT 
				          m.geocodigo as geocodigo,
                         (ST_Dump(ST_Difference(m.geom, aggregated_roads.geom))).geom AS geom                         
                     FROM 
                         (
                             SELECT
                                 ST_Union(ST_Transform(ST_Buffer(ST_Transform(o.geom, 3857), 1), 4326)) AS geom
                             FROM 
                                 public.osm_roads o,
                                 public.municipalities_br m
                             WHERE 
                                 m.geocodigo = ' || municipality_geocodigo || '
                                 AND (
                                     ST_Intersects(o.geom, m.geom) 
                                     OR ST_Crosses(o.geom, m.geom) 
                                     OR ST_Touches(o.geom, m.geom) 
                                     OR ST_Within(o.geom, m.geom)
                                 )
                         ) aggregated_roads
                         CROSS JOIN (
                             SELECT
                                 geom,
                                 geocodigo
                             FROM 
                                 public.municipalities_br
                             WHERE 
                                 geocodigo = ' || municipality_geocodigo || '
                         ) m';
            
            -- Insert data from temporary table into main table
            EXECUTE 'INSERT INTO public.blocks_br (geocodigo,geom)
                     SELECT geocodigo, geom FROM public.blocks_br_' || municipality_geocodigo;
            
            -- Drop temporary table
            EXECUTE 'DROP TABLE IF EXISTS public.blocks_br_' || municipality_geocodigo;
        EXCEPTION  -- Catch any exceptions
            WHEN others THEN  -- Catch any type of exception
                -- Print error message
                RAISE NOTICE 'Error processing municipality_geocodigo %: %', municipality_geocodigo, SQLERRM;
                -- You can also log the error, rollback the transaction, or take other actions as needed
        END;  -- End of exception block
    END LOOP;
END $BODY$;


-- Call procedure that fills blocks_br table state by state to avoid running "out of shared memory error"
--   Call each state and Commit the transaction to make changes permanent
CALL generate_blocks_br(1200000,1299999); COMMIT; -- Acre
CALL generate_blocks_br(2700000,2799999); COMMIT; -- Alagoas
CALL generate_blocks_br(1600000,1699999); COMMIT; -- Amapá
CALL generate_blocks_br(1300000,1399999); COMMIT; -- Amazonas
CALL generate_blocks_br(2900000,2999999); COMMIT; -- Bahia
CALL generate_blocks_br(2300000,2399999); COMMIT; -- Ceará
CALL generate_blocks_br(5300000,5399999); COMMIT; -- Distrito Federal
CALL generate_blocks_br(3200000,3299999); COMMIT; -- Espírito Santo
CALL generate_blocks_br(5200000,5299999); COMMIT; -- Goiás
CALL generate_blocks_br(2100000,2199999); COMMIT; -- Maranhão
CALL generate_blocks_br(5100000,5199999); COMMIT; -- Mato Grosso
CALL generate_blocks_br(5000000,5099999); COMMIT; -- Mato Grosso do Sul
CALL generate_blocks_br(3100000,3199999); COMMIT; -- Minas Gerais
CALL generate_blocks_br(1500000,1599999); COMMIT; -- Pará
CALL generate_blocks_br(2500000,2599999); COMMIT; -- Paraíba
CALL generate_blocks_br(4100000,4199999); COMMIT; -- Paraná
CALL generate_blocks_br(2600000,2699999); COMMIT; -- Pernambuco
CALL generate_blocks_br(2200000,2299999); COMMIT; -- Piauí
CALL generate_blocks_br(2400000,2499999); COMMIT; -- Rio Grande do Norte
CALL generate_blocks_br(4300000,4399999); COMMIT; -- Rio Grande do Sul
CALL generate_blocks_br(3300000,3399999); COMMIT; -- Rio de Janeiro
CALL generate_blocks_br(1100000,1199999); COMMIT; -- Rondônia
CALL generate_blocks_br(1400000,1499999); COMMIT; -- Roraima
CALL generate_blocks_br(4200000,4299999); COMMIT; -- Santa Catarina
CALL generate_blocks_br(3500000,3599999); COMMIT; -- São Paulo
CALL generate_blocks_br(2800000,2899999); COMMIT; -- Sergipe
CALL generate_blocks_br(1700000,1799999); COMMIT; -- Tocantins

-- Create blocks with no empty space among them
DROP TABLE IF EXISTS public.blocks_br_no_buffer;
CREATE TABLE public.blocks_br_no_buffer AS
SELECT  id, geocodigo,
	ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 1), 4326) as geom
FROM public.blocks_br;

ALTER TABLE public.blocks_br_no_buffer ADD PRIMARY KEY (id);

    -- Create indexes
    DROP INDEX IF EXISTS idx_blocks_br_no_buffer_id; CREATE INDEX idx_blocks_br_no_buffer_id ON public.blocks_br_no_buffer(id);
    DROP INDEX IF EXISTS idx_blocks_br_no_buffer_geocodigo; CREATE INDEX idx_blocks_br_no_buffer_geocodigo ON public.blocks_br_no_buffer(geocodigo);
    DROP INDEX IF EXISTS idx_blocks_br_no_buffer_geom; CREATE INDEX idx_blocks_br_no_buffer_geom ON public.blocks_br_no_buffer USING GIST(geom);

-- Create blocks that overlaps 60 cm (30 + 30) | Used to correct null results on joining block_id and building_id
DROP TABLE IF EXISTS public.blocks_br_negative_buffer_1p3;
CREATE TABLE public.blocks_br_negative_buffer_1p3 AS
SELECT  id, geocodigo,
	ST_Transform(ST_Buffer(ST_Transform(geom, 3857), 1.3), 4326) as geom
FROM public.blocks_br;

ALTER TABLE public.blocks_br_negative_buffer_1p3 ADD PRIMARY KEY (id);

    -- Create indexes
    DROP INDEX IF EXISTS idx_blocks_br_negative_buffer_1p3_id; CREATE INDEX idx_blocks_br_negative_buffer_1p3_id ON public.blocks_br_negative_buffer_1p3(id);
    DROP INDEX IF EXISTS idx_blocks_br_negative_buffer_1p3_geocodigo; CREATE INDEX idx_blocks_br_negative_buffer_1p3_geocodigo ON public.blocks_br_negative_buffer_1p3(geocodigo);
    DROP INDEX IF EXISTS idx_blocks_br_negative_buffer_1p3_geom; CREATE INDEX idx_blocks_br_negative_buffer_1p3_geom ON public.blocks_br_negative_buffer_1p3 USING GIST(geom);

--*/






