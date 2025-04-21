--SELECT count(*) FROM public.brazil

-- SELECT * FROM public.brazil LIMIT 3

--/*
-- Create extensions
CREATE EXTENSION IF NOT EXISTS postgis; 
CREATE EXTENSION IF NOT EXISTS postgis_topology; 
CREATE EXTENSION IF NOT EXISTS hstore; 
CREATE EXTENSION IF NOT EXISTS dblink; 
CREATE EXTENSION IF NOT EXISTS plpython3u;
--*/

--/*
-- Delete rows with field names
DELETE FROM public.brazil
WHERE latitude='latitude';
--*/

--/*
-- Create table correcting data types
-- And, fill table with data
DROP TABLE IF EXISTS public.brazil_01;
CREATE TABLE public.brazil_01 (
    id SERIAL PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    area_in_meters FLOAT,
    confidence FLOAT,
	geoc_mun INT,
    geometry GEOMETRY(MultiPolygon, 4326),
    full_plus_code TEXT
);
INSERT INTO public.brazil_01 (latitude, longitude, area_in_meters, confidence, geoc_mun, geometry, full_plus_code)
SELECT 
    b.latitude::FLOAT,
    b.longitude::FLOAT,	
    b.area_in_meters::FLOAT,
    b.confidence::FLOAT,
	null as geocodigo,
	--gc.geocodigo as geoc_mun,
    ST_SetSRID(ST_GeomFromText(b.geometry), 4326) as geometry,
    b.full_plus_code
FROM public.brazil AS b;
--*/

--/*
-- Create indexes
DROP INDEX IF EXISTS idx_id; CREATE INDEX idx_id ON public.brazil_01(id);
DROP INDEX IF EXISTS idx_geometry_spatial; CREATE INDEX idx_geometry_spatial ON public.brazil_01 USING GIST(geometry);
--*/





