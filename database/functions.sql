--------------------------------------------------------------------
-- Snap tolerance for the system to use in spatial queries
-- Example:
--  select dbiait_analysis.snap_tolerance()
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.snap_tolerance(
) RETURNS DOUBLE PRECISION AS $$
BEGIN
    RETURN 0.001;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Extract X coordinate from geometry with specific decimals
-- Example:
--  select st_round_x(geom, 4) from geom_table
CREATE OR REPLACE FUNCTION public.st_round_x(
	v_geom GEOMETRY,
	v_decimal INTEGER DEFAULT 6
) RETURNS NUMERIC AS $$
BEGIN
    RETURN ROUND(ST_X(v_geom)::NUMERIC, v_decimal);
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$  LANGUAGE plpgsql;
--------------------------------------------------------------------
-- Extract Y coordinate from geometry with specific decimals
-- Example:
--  select st_round_y(geom, 4) from geom_table
CREATE OR REPLACE FUNCTION public.st_round_y(
	v_geom GEOMETRY,
	v_decimal INTEGER DEFAULT 6
) RETURNS NUMERIC AS $$
BEGIN
    RETURN ROUND(ST_Y(v_geom)::NUMERIC, v_decimal);
EXCEPTION WHEN OTHERS THEN
	RETURN NULL;
END;
$$  LANGUAGE plpgsql;	
--------------------------------------------------------------------
-- Convert a string into an integer
-- Example:
--  select dbiait_analysis.to_integer('9')
CREATE OR REPLACE FUNCTION public.to_integer(
	v_number VARCHAR,
	v_default INTEGER DEFAULT 0
) RETURNS INTEGER AS $$
BEGIN
    RETURN CAST(v_number as NUMERIC)::INTEGER;
EXCEPTION WHEN OTHERS THEN
	RETURN v_default;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
    --------------------------------------------------------------------
-- Convert a float in integer
-- Example:
--  select dbiait_analysis.from_float_to_int(9.4) -> 9
CREATE OR REPLACE FUNCTION public.from_float_to_int(
	v_number FLOAT,
	v_default INTEGER DEFAULT 0
) RETURNS INTEGER AS $$
BEGIN
    RETURN v_number::INTEGER;
EXCEPTION WHEN OTHERS THEN
	RETURN v_default;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------
-- Transform a Geometry from EPSG:25832 to EPSG:3003 using NTV2 nadgrids
--SELECT ST_X(geom), ST_Y(geom) FROM(
--	SELECT ST_TRANSFORM_RM40_ETRS89(
--		ST_SETSRID(ST_POINT(705438.9186,4830672.536), 25832)
--	) geom
--) t
CREATE OR REPLACE FUNCTION public.ST_TRANSFORM_RM40_ETRS89(
	v_geom GEOMETRY
) RETURNS GEOMETRY AS $$
DECLARE 
	v_folder VARCHAR := '/apps/pgsql/data/';
BEGIN
	RETURN ST_TRANSFORM(
		v_geom, 
		'+proj=tmerc +lat_0=0 +lon_0=9 +k=0.9996 +x_0=1500000 +y_0=0 +ellps=intl +units=m +nadgrids=' || v_folder || 'gridRM40_ETRS89.gsb'
	);
EXCEPTION WHEN OTHERS THEN
	RETURN ST_TRANSFORM(v_geom, 3003);
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public;
------------------------------------------------------------------
-- Transform a Geometry to EPSG:4326
CREATE OR REPLACE FUNCTION public.ST_TRANSFORM_4326(
	v_geom GEOMETRY
) RETURNS GEOMETRY AS $$
BEGIN
	RETURN ST_TRANSFORM(v_geom, 4326);
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public;	
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.IS_NULL(
	v_value anyelement
) RETURNS INTEGER AS $$
	SELECT 
		case when v_value IS NULL then 1
		else 0
		end;
$$  LANGUAGE sql;	
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.TO_BIT(
	v_value VARCHAR
) RETURNS BIT AS $$
	SELECT 
		case when UPPER(v_value) IN ('SI','YES', 'S', 'Y', '1') then 1::BIT
		else 0::BIT
		end;
$$  LANGUAGE sql;	
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.GB_X(
	v_geom GEOMETRY
) RETURNS DOUBLE PRECISION AS $$
BEGIN
	IF UPPER(ST_GeometryType(v_geom)) = 'POINT' THEN
		RETURN ST_X(ST_TRANSFORM_RM40_ETRS89(v_geom));
	ELSE
		RETURN ST_X(ST_CENTROID(ST_TRANSFORM_RM40_ETRS89(v_geom)));
	END IF;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public;
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.GB_Y(
	v_geom GEOMETRY
) RETURNS DOUBLE PRECISION AS $$
BEGIN
	IF UPPER(ST_GeometryType(v_geom)) = 'POINT' THEN
		RETURN ST_Y(ST_TRANSFORM_RM40_ETRS89(v_geom));
	ELSE
		RETURN ST_Y(ST_CENTROID(ST_TRANSFORM_RM40_ETRS89(v_geom)));
	END IF;
	
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public;
--------------------------------------------------------------------
-- Decode a municipal code to obtain the code of grouped municipal
-- This function uses the internal table "decod_com"
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.decode_municipal(
	v_pro_com INTEGER
) RETURNS INTEGER AS $$
DECLARE
	v_result INTEGER;
BEGIN
	SELECT coalesce(d.pro_com_acc, c.cod_comune::INTEGER) cod_comune
	INTO v_result
	FROM (select v_pro_com as cod_comune) c
	LEFT JOIN decod_com d on c.cod_comune::INTEGER = d.pro_com;
	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data into the POP_RES_LOC table using information
-- from LOCALITA ISTAT (2011) - (Ref. 2.3. LOCALITA ISTAT)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_pop_res_loc();
--------------------------------------------------------------------	
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_pop_res_loc(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_result BOOLEAN := FALSE;
BEGIN
    
	DELETE FROM POP_RES_LOC;

	INSERT INTO POP_RES_LOC(anno_rif, data_rif, pro_com, id_localita_istat, popres)
	SELECT 
		p.anno_rif,
		p.data_rif,
		l.pro_com,
		id_localita_istat,
		--loc.popres as popres_before, 
		ROUND(loc.popres*(p.pop_res/l.popres)) popres 
	FROM LOCALITA loc,
	(
		SELECT anno_rif, data_rif, pro_com, pop_res
		FROM POP_RES_COMUNE 
	) p,
	(
		SELECT pro_com, sum(popres) popres 
		FROM LOCALITA 
		GROUP BY pro_com
	) l 
	WHERE 
		p.pro_com::VARCHAR = l.pro_com::VARCHAR 
		AND l.pro_com = loc.pro_com;

	-- update delta (group by pro_com)
	UPDATE POP_RES_LOC
	SET popres = t.new_popres
	FROM (
		SELECT l.id_localita_istat, (l.popres + d.delta) new_popres
		FROM 
		(
			SELECT DISTINCT ON (pro_com)
				   pro_com,id_localita_istat,popres
			FROM   POP_RES_LOC
			ORDER  BY pro_com, popres DESC
		) l,
		(
			SELECT p.pro_com, (p.pop_res - l.popres) delta
			FROM pop_res_comune p,
			(
				SELECT pro_com, sum(popres) popres 
				FROM POP_RES_LOC
				GROUP BY pro_com
			) l
			WHERE p.pro_com = l.pro_com
			AND  p.pop_res - l.popres  <> 0
		) d
		WHERE l.pro_com = d.pro_com
	) t
	WHERE t.id_localita_istat = POP_RES_LOC.id_localita_istat;
	
	v_result:= TRUE;

    RETURN v_result;
--EXCEPTION WHEN OTHERS THEN
--	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data into the DISTRIB_LOC_SERV table using information 
-- from LOCALITA ISTAT (2011) and ACQ_RETE_DISTRIB
-- (Ref. 2.3. Percentuale Popolazione Servita Per Localita)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_distrib_loc_serv();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_distrib_loc_serv(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_result BOOLEAN := FALSE;
BEGIN
    
	DELETE FROM DISTRIB_LOC_SERV;

	INSERT INTO DISTRIB_LOC_SERV(codice_opera, id_localita_istat, perc_popsrv)
	SELECT 
		codice_ato, 
		id_localita_istat, 
		--sum(popres), 
		CASE 
			WHEN sum(perc)<0 THEN
				0
			WHEN sum(perc)>100 THEN
				100
			else
				sum(perc)
		END
	FROM (
		SELECT codice_ato, id_localita_istat, popres, 100*ST_AREA(ST_INTERSECTION(r.geom,l.geom))/ST_AREA(l.geom) perc
		FROM ACQ_RETE_DISTRIB r, LOCALITA l
		WHERE r.D_GESTORE = 'PUBLIACQUA' AND COALESCE(r.D_AMBITO, 'AT3')='AT3' 
		AND r.D_STATO NOT IN ('IPR','IAC')
		AND r.geom && l.geom AND ST_INTERSECTS(r.geom, l.geom)
	) t
	GROUP BY id_localita_istat, codice_ato;
	v_result:= TRUE;
    RETURN v_result;
--EXCEPTION WHEN OTHERS THEN
--	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Update fields (pop_ser_acq and perc_acq) into the POP_RES_COMUNE table 
-- using information from DISTRIB_LOC_SERV and POP_RES_LOC
-- (Ref. 2.4. POPOLAZIONE RESIDENTE ISTAT PER COMUNE)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_pop_res_comune();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_pop_res_comune(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_result BOOLEAN := FALSE;
BEGIN
    
	-- reset dei dati
	UPDATE POP_RES_COMUNE
	SET 
		pop_ser_acq = NULL, --OK (1)
		perc_acq = NULL,	--OK (1)
		perc_fgn = NULL,	--OK
		pop_ser_fgn = NULL, --OK
		perc_dep = NULL,	--OK		
		pop_ser_dep = NULL;	--OK

	-- updating field pop_ser_acq
	UPDATE POP_RES_COMUNE
	SET pop_ser_acq = t2.ab_srv_com, perc_acq = 100*t2.ab_srv_com/POP_RES_COMUNE.pop_res
	FROM (
		SELECT t.pro_com, sum(t.ab_srv_loc) as ab_srv_com 
		FROM(
			SELECT 
				--locistat_2_procom(loc_serv.id_localita_istat) as pro_com,
				loc_pop.pro_com,	
				loc_serv.perc_popsrv*loc_pop.popres/100 as ab_srv_loc 
			FROM 
			DISTRIB_LOC_SERV loc_serv,
			POP_RES_LOC loc_pop
			WHERE loc_serv.id_localita_istat = loc_pop.id_localita_istat
		) t
		GROUP BY t.pro_com
	) t2
	WHERE t2.pro_com = POP_RES_COMUNE.pro_com;

	v_result:= TRUE;
    RETURN v_result;
--EXCEPTION WHEN OTHERS THEN
--	RAISE NOTICE 'Exception: %', SQLERRM;
--	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate the DISTRIB_COM_SERV table 
-- using information from ACQ_RETE_DISTRIB, LOCALITA and POP_RES_COMUNE
-- (Ref. 2.5. PERCENTUALE POPOLAZIONE SERVITA SULLA RETE PER COMUNE)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_distr_com_serv();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_distr_com_serv(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_result BOOLEAN := FALSE;
BEGIN
    
	-- reset dei dati
	DELETE FROM DISTRIB_COM_SERV;
	
	-- populate table
	EXECUTE '
	INSERT INTO DISTRIB_COM_SERV(codice_opera, id_comune_istat, perc_popsrv)
	select t1.codice_ato, t1.pro_com, 100*t1.pop_ser_acq/p.pop_res perc_acq
	from (
		select codice_ato, pro_com, CEIL(sum(popres*perc)) pop_ser_acq 
		from(
			SELECT codice_ato, l.pro_com::VARCHAR, popres, ST_AREA(ST_INTERSECTION(r.geom,l.geom))/ST_AREA(l.geom) perc 
			FROM ACQ_RETE_DISTRIB r, LOCALITA l
			WHERE r.D_GESTORE = ''PUBLIACQUA'' AND COALESCE(r.D_AMBITO, ''AT3'')=''AT3'' 
				AND r.D_STATO NOT IN (''IPR'',''IAC'')
				AND r.geom && l.geom AND ST_INTERSECTS(r.geom, l.geom)
		) t
		group by t.codice_ato, t.pro_com
	) t1, POP_RES_COMUNE p
	WHERE t1.pro_com=p.pro_com';
	v_result:= TRUE;
    RETURN v_result;
--EXCEPTION WHEN OTHERS THEN
--	RAISE NOTICE 'Exception: %', SQLERRM;
--	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate the UTENZA_SERVIZIO table 
-- using information from ACQ_RETE_DISTRIB, LOCALITA and FGN_RETE_RACC, FGN_BACINO (FGN_TRATTAMENTO, FGN_PNT_SCARICO)
-- (Ref. 4.2. SERVIZIO UTENZA)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_utenza_servizio();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_utenza_servizio(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_result BOOLEAN := FALSE;
BEGIN    
	-- reset dei dati
	DELETE FROM UTENZA_SERVIZIO;
	DELETE FROM UTENZA_SERVIZIO_LOC;
	DELETE FROM UTENZA_SERVIZIO_ACQ;
	DELETE FROM UTENZA_SERVIZIO_FGN;
	DELETE FROM UTENZA_SERVIZIO_BAC;
	
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'UTENZA_SERVIZIO';
	
	--LOCALITA
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_LOC(impianto, id_ubic_contatore, codice)
		SELECT DISTINCT ON(uc.idgis) uc.id_impianto, uc.idgis as id_ubic_contatore, g.id_localita_istat
		FROM acq_ubic_contatore uc, localita g
		WHERE (g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom))
		AND uc.id_impianto is not null';	
	-- ACQ_RETE_DISTRIB
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_ACQ(impianto, id_ubic_contatore, codice)
		SELECT DISTINCT ON(uc.idgis) uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, acq_rete_distrib g
		WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND uc.id_impianto is not null';
	-- FGN_RETE_RACC
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_FGN(impianto, id_ubic_contatore, codice)
		SELECT DISTINCT ON(uc.idgis) uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, fgn_rete_racc g
		WHERE (g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom))
		AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND uc.id_impianto is not null';
	-- FGN_BACINO + FGN_TRATTAMENTO/FGN_PNT_SCARICO
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_BAC(impianto, id_ubic_contatore, codice)
		SELECT DISTINCT ON(uc.idgis) uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, (
			select t.codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
			from FGN_BACINO b, FGN_TRATTAMENTO t
			WHERE b.SUB_FUNZIONE = 3 AND b.idgis = t.id_bacino
			AND ((t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3'' AND t.D_GESTORE in (''PUBLIACQUA'',''GIDA'') ) OR t.CODICE_ATO in (''DE00213'',''DE00214''))
			UNION ALL
			select t.codice as codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
			from FGN_BACINO b, FGN_PNT_SCARICO t
			WHERE b.SUB_FUNZIONE = 1 AND b.idgis = t.id_bacino
			AND ((t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3'' AND t.D_GESTORE in (''PUBLIACQUA'',''GIDA'') ) OR t.CODICE in (''DE00213'',''DE00214''))
		) g WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND uc.id_impianto is not null';

	-- initialize table UTENZA_SERVIZIO.id_ubic_contatore with data from ACQ_UBIC_CONTATORE.idgis
	EXECUTE '
	INSERT INTO utenza_servizio(impianto, id_ubic_contatore)
	SELECT DISTINCT u.id_impianto, u.idgis 
	FROM ACQ_UBIC_CONTATORE u, ACQ_CONTATORE c 
	WHERE u.id_impianto is not NULL AND c.D_STATO=''ATT'' AND u.idgis=c.id_ubic_contatore';
	-- update field ids_codice_orig_acq
	EXECUTE '
		UPDATE utenza_servizio 
		SET ids_codice_orig_acq = t.codice 
		FROM (
			SELECT MIN(impianto) as imp, id_ubic_contatore as id_cont, MIN(codice) as codice
			FROM UTENZA_SERVIZIO_ACQ
			GROUP BY id_ubic_contatore
			HAVING COUNT(id_ubic_contatore)=1
		) t
		WHERE id_ubic_contatore = t.id_cont AND (impianto IS NULL OR impianto = t.imp)';
	-- update field id_localita_istat
	EXECUTE '
		UPDATE utenza_servizio 
		SET id_localita_istat = t.codice 
		FROM (
			SELECT MIN(impianto) as imp, id_ubic_contatore as id_cont, MIN(codice) as codice
			FROM UTENZA_SERVIZIO_LOC
			GROUP BY id_ubic_contatore
			HAVING COUNT(id_ubic_contatore)=1
		) t
		WHERE id_ubic_contatore = t.id_cont AND (impianto IS NULL OR impianto = t.imp)';
	-- update field ids_codice_orig_fgn
	EXECUTE '
		UPDATE utenza_servizio 
		SET ids_codice_orig_fgn = t.codice 
		FROM (
			SELECT MIN(impianto) as imp, id_ubic_contatore as id_cont, MIN(codice) as codice
			FROM UTENZA_SERVIZIO_FGN
			GROUP BY id_ubic_contatore
			HAVING COUNT(id_ubic_contatore)=1
		) t
		WHERE id_ubic_contatore = t.id_cont AND (impianto IS NULL OR impianto = t.imp)';
	-- update field ids_codice_orig_dep_sca
	EXECUTE '
		UPDATE utenza_servizio 
		SET ids_codice_orig_dep_sca = t.codice 
		FROM (
			SELECT MIN(impianto) as imp, id_ubic_contatore as id_cont, MIN(codice) as codice
			FROM UTENZA_SERVIZIO_BAC
			GROUP BY id_ubic_contatore
			HAVING COUNT(id_ubic_contatore)=1
		) t
		WHERE id_ubic_contatore = t.id_cont AND (impianto IS NULL OR impianto = t.imp)';


	-- Log duplicated items

	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in localita''
	FROM (
		SELECT uc.id_impianto, uc.idgis as id_ubic_contatore, g.id_localita_istat
		FROM acq_ubic_contatore uc, localita g
		where g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND uc.id_impianto is not null
	) t group by t.id_ubic_contatore having count(0)>1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in acquedotto''
	FROM(
		SELECT uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, acq_rete_distrib g
		WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND uc.id_impianto is not null
	)t group by t.id_ubic_contatore having count(0)>1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in fognatura''
	FROM(
		SELECT uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, fgn_rete_racc g
		WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND uc.id_impianto is not null
	)t group by t.id_ubic_contatore having count(0)>1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in bacino''
	FROM(
		SELECT uc.id_impianto, uc.idgis as id_ubic_contatore, g.codice_ato as codice
		from acq_ubic_contatore uc, (
			select t.codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
			from FGN_BACINO b, FGN_TRATTAMENTO t
			WHERE b.SUB_FUNZIONE = 3 AND b.idgis = t.id_bacino
			AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
			UNION ALL
			select t.codice as codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
			from FGN_BACINO b, FGN_PNT_SCARICO t
			WHERE b.SUB_FUNZIONE = 1 AND b.idgis = t.id_bacino
			AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
		) g WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
		AND uc.id_impianto is not null
	)t group by t.id_ubic_contatore having count(0)>1';
	
	v_result:= TRUE;
    RETURN v_result;
--EXCEPTION WHEN OTHERS THEN
--	RAISE NOTICE 'Exception: %', SQLERRM;
--	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data into the ABITANTI_TRATTATI table 
-- (Ref. 4.3. ABITANTI EQUIVALENTI TRATTATI DA DEPURATORI O SCARICO DIRETTO)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_abitanti_trattati();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_abitanti_trattati(
) RETURNS BOOLEAN AS $$
BEGIN
	
	DELETE FROM ABITANTI_TRATTATI;
	
	EXECUTE '
	INSERT INTO ABITANTI_TRATTATI(codice,idgis,denom,vol_civ,vol_ind,anno,ae_civ,ae_ind,ae_tot,tipo)
	--DEPURATORE
	SELECT distinct b.codice, b.idgis, b.denom,0,0,0,0,0,0,''DEP''
	FROM 
		utenza_servizio us,
		(select t.codice_ato as codice, t.idgis, t.denom  
		FROM FGN_BACINO b, FGN_TRATTAMENTO t
		WHERE b.SUB_FUNZIONE = 3 AND b.idgis = t.id_bacino
		AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
		) b
	WHERE b.codice = us.ids_codice_orig_dep_sca
	UNION ALL
	--SCARICO
	SELECT distinct b.codice, b.idgis, b.denom,0,0,0,0,0,0,''SCA''
	FROM 
		utenza_servizio us,
		(select t.codice, t.idgis, t.denom  
		from FGN_BACINO b, FGN_PNT_SCARICO t
		WHERE b.SUB_FUNZIONE = 1 AND b.idgis = t.id_bacino
		AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
		) b
	WHERE b.codice = us.ids_codice_orig_dep_sca;
	';
	-- Volume industriale ------------------------------------
	-- (SCA)
	UPDATE ABITANTI_TRATTATI 
	SET vol_ind = t.volume, anno = t.anno_rif
	FROM (
		SELECT srv.ids_codice_orig_dep_sca, sap.anno_rif, sum(vol_fgn_ero) as volume
		FROM utenza_servizio srv, utenza_sap sap
		WHERE srv.impianto = sap.impianto and sap.cattariffa in ('APB_REFIND', 'APBLREFIND')
		GROUP BY srv.ids_codice_orig_dep_sca, sap.anno_rif
	) t WHERE t.ids_codice_orig_dep_sca = ABITANTI_TRATTATI.codice AND ABITANTI_TRATTATI.tipo='SCA';
	-- (DEP)
	UPDATE ABITANTI_TRATTATI 
	SET vol_ind = t.volume, anno = t.anno_rif
	FROM (
		SELECT srv.ids_codice_orig_dep_sca, sap.anno_rif, sum(vol_dep_ero) as volume
		FROM utenza_servizio srv, utenza_sap sap
		WHERE srv.impianto = sap.impianto and sap.cattariffa in ('APB_REFIND', 'APBLREFIND')
		GROUP BY srv.ids_codice_orig_dep_sca, sap.anno_rif
	) t WHERE t.ids_codice_orig_dep_sca = ABITANTI_TRATTATI.codice AND ABITANTI_TRATTATI.tipo='DEP';
	---------------------------------------------------------
	-- Volume civile ------------------------------------
	-- (SCA)
	UPDATE ABITANTI_TRATTATI 
	SET vol_civ = t.volume, anno = t.anno_rif
	FROM (
		SELECT srv.ids_codice_orig_dep_sca, sap.anno_rif, sum(vol_fgn_ero) as volume
		FROM utenza_servizio srv, utenza_sap sap
		WHERE srv.impianto = sap.impianto and sap.cattariffa NOT IN ('APB_REFIND', 'APBLREFIND')
		GROUP BY srv.ids_codice_orig_dep_sca, sap.anno_rif
	) t WHERE t.ids_codice_orig_dep_sca = ABITANTI_TRATTATI.codice AND ABITANTI_TRATTATI.tipo='SCA';
	-- (DEP)
	UPDATE ABITANTI_TRATTATI 
	SET vol_civ = t.volume, anno = t.anno_rif
	FROM (
		SELECT srv.ids_codice_orig_dep_sca, sap.anno_rif, sum(vol_dep_ero) as volume
		FROM utenza_servizio srv, utenza_sap sap
		WHERE srv.impianto = sap.impianto and sap.cattariffa NOT IN ('APB_REFIND', 'APBLREFIND')
		GROUP BY srv.ids_codice_orig_dep_sca, sap.anno_rif
	) t WHERE t.ids_codice_orig_dep_sca = ABITANTI_TRATTATI.codice AND ABITANTI_TRATTATI.tipo='DEP';
	
	
	UPDATE ABITANTI_TRATTATI SET ae_civ = vol_civ*1000/365/200, ae_ind = vol_ind*1000/220/200;
	UPDATE ABITANTI_TRATTATI SET ae_tot = ae_civ + ae_ind;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data into the DISTRIB_TRONCHI/ADDUT_TRONCHI table 
-- (Ref. 5.1/5.2. Tronchi)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_tronchi_acq('DISTRIB_TRONCHI');
--  select DBIAIT_ANALYSIS.populate_tronchi_acq('ADDUT_TRONCHI');
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_tronchi_acq(
	v_table VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
	v_sub_funzione INTEGER := 0;
	v_field VARCHAR(32);
	v_column VARCHAR(200);
	v_join_table VARCHAR(32);
	v_tipo_infr VARCHAR(32);
BEGIN

	IF v_table = 'DISTRIB_TRONCHI' THEN
		v_tipo_infr := 'DISTRIBUZIONI';
		v_sub_funzione := 4;
		v_field := 'pressione';
		v_join_table := 'ACQ_RETE_DISTRIB';
		v_column := '0::BIT';
	ELSIF v_table = 'ADDUT_TRONCHI' THEN
		v_tipo_infr := 'ADDUZIONI';
		v_sub_funzione := 1;
		v_field := 'pressione, protezione_catodica';
		v_join_table := 'ACQ_ADDUTTRICE';
		v_column := '
			0::BIT,
			CASE 
				WHEN a.id_sist_prot_cat IS NULL THEN 0::BIT 
				ELSE 1::BIT
			END
		';
	else
		return false;
	--	RAISE EXCEPTION 'Table ' || v_table || ' is not supported'; 
	end IF;	

	EXECUTE 'DELETE FROM ' || v_table || ';';
	
	EXECUTE '
		INSERT INTO ' || v_table || '(
			 geom
			,codice_ato
			,idgis	
			,idgis_rete	
			,id_tipo_telecon			
			,id_materiale	
			,id_conservazione
			,diametro		
			,anno			
			,lunghezza		
			,idx_materiale	
			,idx_diametro	
			,idx_anno		
			,idx_lunghezza	
			,' || v_field || '		
		)
		SELECT 
			a.geom,
			r.codice_ato, 
			a.idgis as idgis, 
			r.idgis as idgis_rete,
			1,
			a.d_materiale as d_materiale_idr, -- da all_domains
			a.d_stato_cons,
			a.d_diametro,
			CASE 
				WHEN a.data_esercizio IS NULL THEN 9999 
				ELSE TO_CHAR(a.data_esercizio, ''YYYY'')::INTEGER 
			END anno_messa_opera,
			ST_LENGTH(a.geom)/1000.0 LUNGHEZZA,
			CASE 
				WHEN a.d_tipo_rilievo in (''ASB'',''DIN'') THEN ''A''
				ELSE ''B''
			END idx_materiale,
			CASE 
				WHEN a.d_diametro IS NULL THEN ''X''
				WHEN a.d_diametro IS NOT NULL AND (a.d_tipo_rilievo in (''ASB'',''DIN'')) THEN ''A''
				ELSE ''B''
			END idx_diametro, 
			CASE 
				WHEN a.data_esercizio IS NULL THEN ''X''
				WHEN a.data_esercizio IS NOT NULL AND (a.d_tipo_rilievo in (''ASB'',''DIN'')) THEN ''A''
				ELSE ''B''
			END idx_anno, 
			a.d_tipo_rilievo,
			' || v_column || '
		FROM 
			ACQ_CONDOTTA a,  
			' || v_join_table || ' r
		WHERE 
			(a.D_AMBITO = ''AT3'' OR a.D_AMBITO IS null) AND (a.D_STATO = ''ATT'' OR a.D_STATO = ''FIP'' OR
			a.D_STATO IS NULL) AND (a.SN_FITTIZIA = ''NO'' OR a.SN_FITTIZIA IS null) AND (a.D_GESTORE
			= ''PUBLIACQUA'') AND a.SUB_FUNZIONE = ' || v_sub_funzione || '
			AND a.id_rete=r.idgis;
		';

		-- (TIPO_RILIEVO = ASB or TIPO_RILIEVO  DIN) allora l'indice assume valore A, altrimenti B


	--D_MATERIALE convertito in D_MATERIALE_IDR
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_materiale = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.id_materiale,''NULL'') AND d.dominio_gis = ''D_MATERIALE_IDR''
	';
	
	EXECUTE '
		UPDATE ' || v_table || ' SET idx_materiale = ''X'' WHERE id_materiale = ''1''
	';

	--D_STATO_CONS convertito in id_conserva
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_conservazione = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.id_conservazione,''SCO'') AND d.dominio_gis = ''D_STATO_CONS'';
	';
	
	-- valorizzazione idx_lunghezza (da chiarire)
	EXECUTE '
		UPDATE ' || v_table || '
		SET idx_lunghezza = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.idx_lunghezza,''SCO'') AND d.dominio_gis = ''D_T_RILIEVO'';
	';
	
	-- valorizzazione rete con gestione delle pressioni
	EXECUTE '
		UPDATE ' || v_table || '
		SET pressione = 1::BIT WHERE EXISTS(
			SELECT * FROM (
				SELECT a.idgis 
				FROM 
					acq_distretto d,
					acq_condotta a
				WHERE d_tipo = ''MIS'' 
				AND a.geom&&d.geom AND ST_INTERSECTS(a.geom,d.geom)
				GROUP by a.idgis having count(*)>0
			) t WHERE t.idgis = ' || v_table || '.idgis		
		);
	';
	
	-- Aggiornamento tipo telecontrollo
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_tipo_telecon = 2 WHERE EXISTS(
			SELECT * FROM (
				SELECT a.idgis 
				FROM 
					acq_distretto d,
					acq_condotta a
				WHERE d_tipo = ''MIS'' 
				AND a.geom&&d.geom AND ST_INTERSECTS(a.geom,d.geom)
				AND d.d_tipo = ''MIS''
			) t WHERE t.idgis = ' || v_table || '.idgis		
		);
	';
	
	-- ACQ_COND_ALTRO
	DELETE FROM ACQ_COND_ALTRO WHERE tipo_infr = v_tipo_infr;
	EXECUTE '
		INSERT INTO ACQ_COND_ALTRO (idgis, id_rete, codice_ato, tipo_infr)
		SELECT idgis, idgis_rete, codice_ato, $1
		FROM ' || v_table || ';
	' using v_tipo_infr;
	
	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data into the DISTRIB_TRONCHI table 
-- (Ref. 5.1 DISTRIB_TRONCHI)
-- OUT: BOOLEAN
-- Example:
--  select DBIAIT_ANALYSIS.populate_distrib_tronchi();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_distrib_tronchi(
) RETURNS BOOLEAN AS $$
BEGIN
	RETURN populate_tronchi_acq('DISTRIB_TRONCHI');
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data into the ADDUT_TRONCHI table 
-- (Ref. 5.2. ADDUT_TRONCHI)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_addut_tronchi();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_addut_tronchi(
) RETURNS BOOLEAN AS $$
BEGIN
	RETURN populate_tronchi_acq('ADDUT_TRONCHI');
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;		
--------------------------------------------------------------------
-- Populate data into the ACQ_LUNGHEZZA_RETE table 
-- (Ref. 5.5. LUNGHEZZA RETE ACQUEDOTTO)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_lung_rete_acq();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_lung_rete_acq(
) RETURNS BOOLEAN AS $$
BEGIN

	DELETE FROM ACQ_LUNGHEZZA_RETE;
	
	INSERT INTO ACQ_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_tlc
	)
	SELECT 
		idgis_rete, codice_ato, 'DISTRIBUZIONE', sum(lunghezza) lung, 
		sum(case when id_tipo_telecon=1 then lunghezza else 0 end) lung_tlc 
	FROM distrib_tronchi
	GROUP BY codice_ato, idgis_rete;

	INSERT INTO ACQ_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_tlc
	)
	SELECT 
		idgis_rete, codice_ato, 'ADDUZIONE', sum(lunghezza) lung, 
		sum(case when id_tipo_telecon=1 then lunghezza else 0 end) lung_tlc 
	FROM addut_tronchi
	GROUP BY codice_ato, idgis_rete;
	
	RETURN TRUE;
	
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;		
--------------------------------------------------------------------
-- Populate data into the FOGNAT_TRONCHI/COLLET_TRONCHI table 
-- (Ref. 6.1/6.2. Tronchi)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_tronchi_fgn('FOGNAT_TRONCHI');
--  select DBIAIT_ANALYSIS.populate_tronchi_fgn('COLLETT_TRONCHI');
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_tronchi_fgn(
	v_table VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
	v_sub_funzione INTEGER := 0;
	v_join_table VARCHAR(32):='';
	v_tipo_infr VARCHAR(32);
BEGIN

	IF v_table = 'FOGNAT_TRONCHI' THEN
		v_sub_funzione := 1;
		v_join_table := 'FGN_RETE_RACC';
		v_tipo_infr := 'RETE RACCOLTA';
	ELSIF v_table = 'COLLETT_TRONCHI' THEN
		v_sub_funzione := 2;
		v_join_table := 'FGN_COLLETTORE';
		v_tipo_infr := 'COLLETTORI';
	ELSE
		return FALSE;
	END IF;	

	EXECUTE 'DELETE FROM ' || v_table || ';';
		
	EXECUTE '
		INSERT INTO ' || v_table || '(
			 geom
			,codice_ato
			,idgis	
			,idgis_rete
			,recapito			
			,id_materiale	
			,id_conservazione
			,diametro		
			,anno			
			,lunghezza		
			,idx_materiale	
			,idx_diametro	
			,idx_anno		
			,idx_lunghezza	
			,id_refluo_trasportato
			,funziona_gravita
			,depurazione		
		)
		SELECT 
			a.geom,
			r.codice_ato, 
			a.idgis,
			r.idgis as idgis_rete,	
			NULL,
			a.d_materiale as d_materiale_idr, -- da all_domains
			a.d_stato_cons,
			coalesce( a.d_diametro, GREATEST(a.dim_l_min, a.dim_l_max, a.dim_h_min, a.dim_h_max) ),
			CASE 
				WHEN a.data_esercizio IS NULL THEN 9999 
				ELSE TO_CHAR(a.data_esercizio, ''YYYY'')::INTEGER 
			END anno_messa_opera,
			ST_LENGTH(a.geom)/1000.0 LUNGHEZZA,
			CASE 
				WHEN a.d_tipo_rilievo in (''ASB'',''DIN'') THEN ''A''
				ELSE ''B''
			END idx_materiale,
			CASE 
				WHEN coalesce( a.d_diametro, GREATEST(a.dim_l_min, a.dim_l_max, a.dim_h_min, a.dim_h_max) ) IS NULL THEN ''X''
				WHEN a.d_diametro IS NOT NULL AND (a.d_tipo_rilievo in (''ASB'',''DIN'')) THEN ''A''
				ELSE ''B''
			END idx_diametro, 
			CASE 
				WHEN a.data_esercizio IS NULL THEN ''X''
				WHEN a.data_esercizio IS NOT NULL AND (a.d_tipo_rilievo in (''ASB'',''DIN'')) THEN ''A''
				ELSE ''B''
			END idx_anno, 
			a.d_tipo_rilievo as idx_lunghezza,
			a.d_tipo_acqua as id_refluo_trasportato,
			0::BIT,
			0::BIT
		FROM 
			FGN_CONDOTTA a
			LEFT JOIN
			' || v_join_table || ' r
			ON a.id_rete = r.idgis
		WHERE 
			(a.D_AMBITO = ''AT3'' OR a.D_AMBITO IS null) 
			AND (a.D_STATO = ''ATT'' OR a.D_STATO = ''FIP'' OR a.D_STATO IS NULL) 
			AND (a.SN_FITTIZIA = ''NO'' OR a.SN_FITTIZIA IS null) 
			AND (a.D_TIPO_ACQUA in (''MIS'',''NER'',''SCA'') or a.D_TIPO_ACQUA IS NULL)
			AND (a.D_GESTORE = ''PUBLIACQUA'') AND a.SUB_FUNZIONE = $1;' USING v_sub_funzione;

	--D_MATERIALE convertito in D_MATERIALE_IDR
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_materiale = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.id_materiale,''NULL'') AND d.dominio_gis = ''D_MATERIALE_IDR''
	';

	--D_STATO_CONS convertito in id_conserva
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_conservazione = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.id_conservazione,''SCO'') AND d.dominio_gis = ''D_STATO_CONS'';
	';
	
	-- valorizzazione idx_lunghezza
	EXECUTE '
		UPDATE ' || v_table || '
		SET idx_lunghezza = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.idx_lunghezza,''SCO'') AND d.dominio_gis = ''D_T_RILIEVO'';
	';
	
	-- valorizzazione id_refluo_trasportato
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_refluo_trasportato = d.valore_netsic
		FROM ALL_DOMAINS d
		WHERE d.valore_gis = COALESCE(' || v_table || '.id_refluo_trasportato,''MIS'') AND d.dominio_netsic = ''id_refluo_trasportato'';
	';
	EXECUTE '
		UPDATE ' || v_table || '
		SET id_refluo_trasportato = ''1''
		WHERE to_integer(id_refluo_trasportato, -9) = -9;
	';
	
	-- valorizzazione funziona_gravita
	EXECUTE '
		UPDATE ' || v_table || '
		SET funziona_gravita = 1::BIT WHERE EXISTS(
			SELECT * FROM (
				SELECT c.idgis
				FROM fgn_condotta c, all_domains d
				WHERE c.d_funzionam = d.valore_gis AND d.dominio_gis=''D_F_FUNZIONAM_COND''
				AND d.valore_netsic = ''1''
			) t WHERE t.idgis = ' || v_table || '.idgis	
		);
	';
	
	-- valorizzazione pressione di depurazione
	EXECUTE '
		UPDATE ' || v_table || '
		SET depurazione = 1::BIT WHERE EXISTS(
			SELECT * FROM (
				select c.idgis 
				FROM 
					fgn_condotta c,
					fgn_trattamento d
				where c.sist_acq_dep = d.business_id
			) t WHERE t.idgis = ' || v_table || '.idgis		
		);
	';
	
	--aggiornamento campo idx_materiale ad X nel caso che il campo id_materiale sia 1
	EXECUTE '
		UPDATE ' || v_table || '
		SET idx_materiale = ''X'' WHERE id_materiale=''1'';
	';

	-- Aggiornamento recapito (step 1)
	EXECUTE '
		UPDATE ' || v_table || '
		SET recapito = t.codice_ato 
		FROM (
			SELECT fc.idgis, ft.codice_ato 
			FROM fgn_condotta fc, fgn_trattamento ft
			WHERE fc.sist_acq_dep = ft.business_id  
		) t WHERE ' || v_table || '.recapito IS NULL AND t.idgis = ' || v_table || '.idgis
		;
	';
	-- Aggiornamento recapito (step 2)
	EXECUTE '
		UPDATE ' || v_table || '
		SET recapito = t.recapito_prossimale 
		FROM (
			SELECT fc.idgis, ft.recapito_prossimale 
			FROM fgn_condotta fc, fgn_pnt_scarico ft
			WHERE fc.sist_acq_dep = ft.business_id  
		) t WHERE ' || v_table || '.recapito IS NULL AND t.idgis = ' || v_table || '.idgis
		;
	';
	
	-- FGN_COND_ALTRO
	DELETE FROM FGN_COND_ALTRO WHERE tipo_infr = v_tipo_infr;
	EXECUTE '
		INSERT INTO FGN_COND_ALTRO (idgis, id_rete, codice_ato, tipo_infr)
		SELECT idgis, idgis_rete, codice_ato, $1
		FROM ' || v_table || ';
	' using v_tipo_infr;
	
	-- LOG
	DELETE FROM LOG_STANDALONE WHERE alg_name = v_table;
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT idgis, ''' || v_table || ''', ''Campo recapito non valorizzato'' 
	FROM ' || v_table || ' WHERE recapito is NULL';

	-- LOG
	DELETE FROM LOG_STANDALONE WHERE alg_name = v_table;
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT idgis, ''' || v_table || ''', ''Campo idgis_rete vuoto''
	FROM ' || v_table || ' WHERE idgis_rete is NULL';
	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data into the FOGNAT_TRONCHI table 
-- (Ref. 6.1. FOGNAT_TRONCHI)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_fognat_tronchi();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_fognat_tronchi(
) RETURNS BOOLEAN AS $$
BEGIN
	RETURN populate_tronchi_fgn('FOGNAT_TRONCHI');
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data into the COLLETT_TRONCHI table 
-- (Ref. 6.2. COLLETT_TRONCHI)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_collett_tronchi();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_collett_tronchi(
) RETURNS BOOLEAN AS $$
BEGIN
	RETURN populate_tronchi_fgn('COLLETT_TRONCHI');
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data into the FGN_LUNGHEZZA_RETE table 
-- (Ref. 6.5. LUNGHEZZA RETE FOGNATURA)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_lung_rete_fgn();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_lung_rete_fgn(
) RETURNS BOOLEAN AS $$
BEGIN

	DELETE FROM FGN_LUNGHEZZA_RETE;

	-- FOGNATURA
	INSERT INTO FGN_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_dep,
		id_refluo_trasportato,
		lung_rete_mista,
		lung_rete_nera
	)
    with lunghezza_reti as (
	    select
            idgis,
            sum(lu_mista) as lu_mista,
            sum(lu_nera) as lu_nera
        from
            tab_ispezioni
        group by
            1)
	select
		idgis_rete, codice_ato, 'FOGNATURA',
		sum(lunghezza) lung,
		sum(lung_dep) lung_tlc,
		case when sum(id_rt) = 0 then 2 else 1 end,
		lu_mista,
		lu_nera
	from (
		select
			distinct on (ft.idgis, codice_ato) ft.idgis, ft.recapito, bc.idgis as idgis_bac, idgis_rete, codice_ato, lunghezza, case when id_refluo_trasportato <> '1' then 0 else 1 end id_rt,
			case when
                ft.depurazione::integer = 1
                then lunghezza
            else 0 end lung_dep
		FROM
		  FOGNAT_TRONCHI as ft
		LEFT JOIN
		  FGN_BACINO as bc ON (ft.geom&&bc.geom and ST_INTERSECTS(ft.geom, bc.geom) and bc.sub_funzione=3)
		WHERE idgis_rete is not NULL
	) t  left join lunghezza_reti as lr on t.idgis_rete = lr.idgis
	GROUP BY t.codice_ato, t.idgis_rete, lu_mista, lu_nera;

	-- COLLETTORE
	INSERT INTO FGN_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_dep,
		id_refluo_trasportato,
		lung_rete_mista,
		lung_rete_nera
	)
	select
		idgis_rete, codice_ato, 'COLLETTORE', sum(lunghezza) lung,
		sum(lung_dep) lung_tlc,
		case when sum(id_rt) = 0 then 2 else 1 end,
		null as lung_rete_mista,
		null as lung_rete_nera
	from (
		 select
            distinct on (ft.idgis, codice_ato) ft.idgis, ft.recapito, bc.idgis as idgis_bac, idgis_rete, codice_ato, lunghezza, case when id_refluo_trasportato <> '1' then 0 else 1 end id_rt,
            case when
                ft.depurazione::integer = 1
                then lunghezza
            else 0 end lung_dep
        FROM
		  COLLETT_TRONCHI as ft
		LEFT JOIN
		  FGN_BACINO as bc ON (ft.geom&&bc.geom and ST_INTERSECTS(ft.geom, bc.geom) and bc.sub_funzione=3)
		WHERE idgis_rete is not NULL
	) t
	GROUP BY t.codice_ato, t.idgis_rete;

	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- determine number and length of allacci ACQ
-- (Ref. 7.1. ACQUEDOTTO)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.determine_acq_allacci();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.determine_acq_allacci(
) RETURNS BOOLEAN AS $$
DECLARE
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN

	DELETE FROM LOG_STANDALONE WHERE alg_name = 'ACQUEDOTTO';
	DELETE FROM ACQ_ALLACCIO;
	DELETE FROM ACQ_LUNGHEZZA_ALLACCI;
    DELETE FROM SUPPORT_ACQ_ALLACCI;

    -- CREAZIONE TABELLA DI SUPPORTO IN COMUNE FRA ACQ_COND_ALTRO E ACQ_ALLACCI.
    -- CI SONO I DATI GREZZI E NON LE AGGREGAZIONI
    INSERT INTO support_acq_allacci
    SELECT z.id_cassetta, z.id_condotta, z.id_derivazione, ac.sub_funzione,
        0 nr_allacci, 0 lung_alla, z.cnt nr_allacci_ril, z.leng lung_alla_ril
    FROM acq_condotta ac,
    (
        SELECT id_cass_cont id_cassetta, id_condotta,id_derivazione, count(0) cnt, sum(leng) leng
        FROM (
            SELECT d.id_condotta,id_derivazione,id_cass_cont, st_length(c.geom) leng
            FROM acq_derivazione d, acq_condotta c,
            (
                select distinct on(cc.idgis) cc.id_derivazione, uc.id_cass_cont
                from acq_cass_cont cc, acq_ubic_contatore uc
                where uc.ID_IMPIANTO is not null
                and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                and uc.id_cass_cont = cc.idgis
            ) cc
            WHERE
                d.idgis = cc.id_derivazione
                and c.sub_funzione = 3
                and c.geom&&st_buffer(d.geom, v_tol)
                and st_intersects(c.geom, st_buffer(d.geom, v_tol))
        ) t GROUP BY t.id_condotta, t.id_derivazione, t.id_cass_cont
        union ALL
        SELECT id_cass_cont id_cassetta, id_condotta,id_derivazione, count(0) cnt, 0 leng
        FROM (
            SELECT d.id_condotta,id_derivazione,id_cass_cont, 0 leng
            FROM acq_derivazione d, acq_condotta c,
            (
                select distinct on(cc.idgis) cc.id_derivazione, uc.id_cass_cont
                from acq_cass_cont cc, acq_ubic_contatore uc
                where uc.ID_IMPIANTO is not null
                and EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                and uc.id_cass_cont = cc.idgis
            ) cc
            WHERE
                d.idgis = cc.id_derivazione
                and c.sub_funzione = 3
                and c.geom&&st_buffer(d.geom, v_tol)
                and st_intersects(c.geom, st_buffer(d.geom, v_tol))
        ) t GROUP BY t.id_condotta, t.id_derivazione, t.id_cass_cont
    ) z
    WHERE ac.idgis = z.id_condotta
    AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
    AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
    AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
    AND (ac.D_GESTORE = 'PUBLIACQUA')
    AND ac.SUB_FUNZIONE in (1, 4)
    UNION ALL
    -- 2) SIMULAZIONE ALLACCIO
    SELECT z.id_cassetta, z.id_condotta, z.id_derivazione, ac.sub_funzione,
        sum(z.cnt) nr_allacci, sum(z.leng) lung_alla, 0 nr_allacci_ril, 0 lung_alla_ril
    FROM acq_condotta ac,
    (
        SELECT cc.id_cass_cont id_cassetta, d.id_condotta, cc.id_derivazione, count(0) cnt, sum(CASE WHEN st_length(l.geom)>50 THEN 50 ELSE st_length(l.geom) END) leng
        FROM acq_deriv_auto d, acq_link_deriv l,
        (
                select distinct on(cc.idgis) cc.idgis, cc.id_derivazione , uc.id_cass_cont
                from acq_cass_cont_auto cc, acq_ubic_contatore uc
                where uc.ID_IMPIANTO is not null
                and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                and uc.id_cass_cont = cc.idgis
        ) cc
        WHERE
            d.idgis=cc.id_derivazione
            and l.id_derivazione = d.idgis
            and l.id_cass_cont = cc.idgis
        group by d.id_condotta, cc.id_derivazione, cc.id_cass_cont
        UNION ALL
        SELECT ss.id_cass_cont id_cassetta, d.id_condotta, ss.id_derivazione, count(0) cnt, 0 leng
        FROM acq_deriv_auto d, acq_link_deriv l,
        (
                select distinct on(cc.idgis) cc.idgis, cc.id_derivazione , uc.id_cass_cont
                from acq_cass_cont_auto cc, acq_ubic_contatore uc
                where uc.ID_IMPIANTO is not null
                and EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                and uc.id_cass_cont = cc.idgis
        ) ss
        WHERE
            d.idgis=ss.id_derivazione
            and l.id_derivazione = d.idgis
            and l.id_cass_cont = ss.idgis
        group by d.id_condotta, ss.id_derivazione, ss.id_cass_cont
    ) z
    WHERE ac.idgis = z.id_condotta
    AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
    AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
    AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
    AND (ac.D_GESTORE = 'PUBLIACQUA')
    AND ac.SUB_FUNZIONE in (1, 4)
	group by z.id_cassetta,z.id_condotta, z.id_derivazione, ac.sub_funzione;
   -- PAACCA00000001630138


    UPDATE ACQ_COND_ALTRO
    SET
        nr_allacci_sim = w.nr_allacci_sim,
        lu_allacci_sim = w.lu_allacci_sim,
        nr_allacci_ril = w.nr_allacci_ril,
        lu_allacci_ril = w.lu_allacci_ril
    FROM (
        SELECT id_condotta as idgis, NULL,
                CASE
                    WHEN sub_funzione = 4 THEN 'DISTRIBUZIONI'
                    WHEN sub_funzione = 1 THEN 'ADDUZIONI'
                    ELSE '?'
                END tipo_infr,
            sum(nr_allacci) nr_allacci_sim, sum(lung_alla) lu_allacci_sim,
            sum(nr_allacci_ril) nr_allacci_ril, sum(lung_alla_ril) lu_allacci_ril
        FROM (select * from support_acq_allacci) xx group by id_condotta,sub_funzione
    ) w WHERE w.idgis = ACQ_COND_ALTRO.idgis;

	--Aggiornamento codice ATO (su DISTRIBUZIONI)
	UPDATE ACQ_COND_ALTRO
	SET codice_ato = t.codice_ato
	FROM ACQ_RETE_DISTRIB t
	WHERE t.idgis = ACQ_COND_ALTRO.id_rete
	AND ACQ_COND_ALTRO.tipo_infr = 'DISTRIBUZIONI';
	--Aggiornamento codice ATO (su ADDUZIONI)
	UPDATE ACQ_COND_ALTRO
	SET codice_ato = t.codice_ato
	FROM ACQ_ADDUTTRICE t
	WHERE t.idgis = ACQ_COND_ALTRO.id_rete
	AND ACQ_COND_ALTRO.tipo_infr = 'ADDUZIONI';

	--GROUP BY x ACQ_LUNGHEZZA_ALLACCI
	INSERT INTO acq_lunghezza_allacci(idgis, codice_ato, tipo_infr, nr_allacci_ril, lung_alla_ril, nr_allacci, lung_alla)
	SELECT id_rete, codice_ato, tipo_infr, sum(nr_allacci_ril), sum(lu_allacci_ril)/1000, sum(nr_allacci_sim), sum(lu_allacci_sim)/1000
	FROM ACQ_COND_ALTRO
	WHERE id_rete is NOT NULL
	GROUP BY id_rete, codice_ato, tipo_infr;

	-- INSERT INTO ACQ_ALLACCIO CON AGGREGAZIONE NECESSARIA
	INSERT INTO acq_allaccio
    SELECT id_cassetta,id_condotta, id_derivazione,
        (sum(lung_alla) + sum(lung_alla_ril)) lungh_all, case when sum(lung_alla) > 0 then 'SIMULATO' else 'RILEVATO' end as tipo,
        (sum(nr_allacci_ril) + sum(nr_allacci)) nr_cassette
    FROM support_acq_allacci
    WHERE id_cassetta in (select distinct on(cc.idgis) uc.id_cass_cont
        from acq_cass_cont_auto cc join acq_ubic_contatore uc on uc.id_cass_cont = cc.idgis
        join acq_contatore ac on uc.idgis =ac.id_ubic_contatore
        where uc.ID_IMPIANTO is not null
        and not EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
        and ac.tariffa not in ('APB_REFIND', 'APBLREFIND', 'APBNREFCIV', 'APBHSUBDIS', 'COPDCI0000', 'COPDIN0000'))
    GROUP BY id_cassetta, id_condotta, id_derivazione, sub_funzione;

	--ANOMALIES 1
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT idgis, 'ACQUEDOTTO', 'Derivazione che non interseca la condotta di rete indicata nella derivazione (per problemi di snap)'
	FROM acq_derivazione ad
	where EXISTS(
		SELECT c.idgis
		from acq_condotta c
		WHERE (c.D_AMBITO = 'AT3' OR c.D_AMBITO IS null)
		AND (c.D_STATO = 'ATT' OR c.D_STATO = 'FIP' OR c.D_STATO IS NULL)
		AND (c.SN_FITTIZIA = 'NO' OR c.SN_FITTIZIA IS null)
		AND (c.D_GESTORE = 'PUBLIACQUA')
		AND c.SUB_FUNZIONE in (1, 4)
		AND c.idgis=ad.id_condotta
	)
	AND NOT EXISTS(
		SELECT d.idgis
		FROM acq_derivazione d, acq_condotta c
		WHERE d.id_condotta = c.idgis

		AND (c.D_AMBITO = 'AT3' OR c.D_AMBITO IS null)
		AND (c.D_STATO = 'ATT' OR c.D_STATO = 'FIP' OR c.D_STATO IS NULL)
		AND (c.SN_FITTIZIA = 'NO' OR c.SN_FITTIZIA IS null)
		AND (c.D_GESTORE = 'PUBLIACQUA')
		AND c.SUB_FUNZIONE in (1, 4)

		AND c.geom&&ST_BUFFER(d.geom, v_tol)
		AND st_intersects(c.geom,ST_BUFFER(d.geom, v_tol))
		AND d.idgis=ad.idgis
	);
	-- ANOMALIES 2
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT idgis, 'ACQUEDOTTO', 'Derivazione che interseca la condotta di rete ma non interseca alcuna condotta di allacciamento'
	FROM acq_derivazione ad
	WHERE NOT EXISTS(
		SELECT d.idgis
		FROM acq_derivazione d, acq_condotta c
		WHERE c.sub_funzione = 3
		AND c.geom&&ST_BUFFER(d.geom, v_tol)
		AND st_intersects(c.geom,ST_BUFFER(d.geom, v_tol))
		AND d.idgis = ad.idgis
	);

	--ANOMALIES 3
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	select idgis, 'ACQUEDOTTO', 'Utenza che non risulta allacciata a nessuna rete'
	from acq_ubic_contatore uc
	where uc.ID_IMPIANTO is not null and
	uc.sorgente IS null and EXISTS(
		select idgis from (
			SELECT idgis FROM acq_cass_cont acc where id_derivazione is null
			union ALL
			SELECT idgis FROM acq_cass_cont_auto acc where id_derivazione is null
		) t where t.idgis = uc.id_cass_cont
	);

	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- determine number and length of allacci FGN
-- (Ref. 7.2. FOGNATURA)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.determine_fgn_allacci();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.determine_fgn_allacci(
) RETURNS BOOLEAN AS $$
DECLARE
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN

	DELETE FROM LOG_STANDALONE WHERE alg_name = 'FOGNATURA';
	DELETE FROM FGN_ALLACCIO;
	DELETE FROM SUPPORT_FGN_ALLACCI;
	DELETE FROM FGN_LUNGHEZZA_ALLACCI_id_rete;
	DELETE FROM FGN_LUNGHEZZA_ALLACCI;

    INSERT INTO support_fgn_allacci
    SELECT
        x.id_fossa_settica,
        x.id_condotta,
        x.id_immissione,
        tipo,
        sum(lu_allacci_c) as lu_allacci_c,
        sum(lu_allacci_c_ril) as lu_allacci_c_ril,
        sum(lu_allacci_i) as lu_allacci_i,
        sum(lu_allacci_i_ril) as lu_allacci_i_ril,
        sum(nr_allacci_c) as nr_allacci_c,
        sum(nr_allacci_c_ril) as nr_allacci_c_ril,
        sum(nr_allacci_i) as nr_allacci_i,
        sum(nr_allacci_i_ril) as nr_allacci_i_ril
    FROM (
        -- 1) Realmente mappati (utenze civili)
        SELECT ac.idgis as id_condotta, z.id_fossa_settica, z.id_immissione, ac.id_rete, 'RILEVATO' tipo,  ac.sub_funzione,
            0 lu_allacci_c, z.leng lu_allacci_c_ril,
            0 lu_allacci_i, 0 lu_allacci_i_ril,
            0 nr_allacci_c, z.cnt nr_allacci_c_ril,
            0 nr_allacci_i, 0 nr_allacci_i_ril
        FROM fgn_condotta ac,
        (
            SELECT id_condotta, id_fossa_settica, id_immissione, count(0) cnt, sum(leng) leng
            FROM (
                SELECT d.id_condotta, id_fossa_settica, id_immissione, st_length(c.geom) leng
                FROM fgn_immissione d, fgn_condotta c,
                (
                    select distinct on(fs.idgis) fs.id_immissione, id_fossa_settica
                    from fgn_fossa_settica fs,
                    (
                        select uct.id_fossa_settica, uct.id_impianto, uct.idgis
                        from utenza_sap us, acq_ubic_contatore uct
                        where us.id_ubic_contatore = uct.idgis
                        AND us.cattariffa NOT IN ('APB_REFIND','APBLREFIND')
                    ) uc
                    where
                    uc.ID_IMPIANTO is not null and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale) and
                    uc.id_fossa_settica = fs.idgis
                ) cc
                WHERE
                    d.idgis = cc.id_immissione
                    and c.sub_funzione = 0
                    and c.geom&&st_buffer(d.geom, v_tol)
                    and st_intersects(c.geom, st_buffer(d.geom, v_tol))
            ) t GROUP BY t.id_condotta, t.id_fossa_settica, t.id_immissione
        ) z
        WHERE ac.idgis = z.id_condotta
        AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
        AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
        AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
        AND (ac.D_GESTORE = 'PUBLIACQUA') AND ac.SUB_FUNZIONE in (1,2)
        AND (ac.D_TIPO_ACQUA in ('MIS','NER','SCA') or ac.D_TIPO_ACQUA IS NULL)

        UNION ALL
        -- 2) Realmente mappati (utenze industriali)

        SELECT ac.idgis id_condotta, id_immissione, id_fossa_settica, ac.id_rete, 'RILEVATO' tipo, ac.sub_funzione,
            0 lu_allacci_c, 0 lu_allacci_c_ril,
            0 lu_allacci_i, z.leng lu_allacci_i_ril,
            0 nr_allacci_c, 0 nr_allacci_c_ril,
            0 nr_allacci_i, z.cnt nr_allacci_i_ril
        FROM fgn_condotta ac,
        (
            SELECT id_condotta, id_immissione, id_fossa_settica, count(0) cnt, sum(leng) leng
            FROM (
                SELECT d.id_condotta, id_immissione, id_fossa_settica, st_length(c.geom) leng
                FROM fgn_immissione d, fgn_condotta c,
                (
                    select distinct prod_imm.id_immissione, id_fossa_settica
                    from (
                        select * from acq_contatore c,
                        (
                            select * from rel_prod_cont
                            union all
                            select * from a_rel_prod_cont
                        ) pc
                        where c.idgis = pc.idgis_contatore
                    ) prod_cont,
                    (
                        select uct.id_fossa_settica, uct.id_impianto, uct.idgis
                        from utenza_sap us, acq_ubic_contatore uct
                        where us.id_ubic_contatore = uct.idgis
                        AND us.cattariffa IN ('APB_REFIND','APBLREFIND')
                        --and uct.idgis = 'PAAUCO00000002082051'
                    ) uc,
                    (
                        select id_produttivo, id_immissione from fgn_rel_prod_imm
                        union all
                        select id_produttivo, id_immissione from a_fgn_rel_prod_imm
                    ) prod_imm
                    where
                    uc.ID_IMPIANTO is not null and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale) and
                    prod_cont.id_ubic_contatore = uc.idgis and
                    prod_imm.id_produttivo = prod_cont.idgis_produttivo
                ) cc
                WHERE
                    d.idgis = cc.id_immissione
                    and c.sub_funzione = 0
                    and c.geom&&st_buffer(d.geom, v_tol)
                    and st_intersects(c.geom, st_buffer(d.geom, v_tol))
            ) t GROUP BY t.id_condotta, id_immissione, id_fossa_settica
        ) z
        WHERE ac.idgis = z.id_condotta
        AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
        AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
        AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
        AND (ac.D_GESTORE = 'PUBLIACQUA') AND ac.SUB_FUNZIONE in (1,2)
        AND (ac.D_TIPO_ACQUA in ('MIS','NER','SCA') or ac.D_TIPO_ACQUA IS NULL)

        UNION ALL
        -- 3) SIMULAZIONE ALLACCIO (CIVILI)
        SELECT ac.idgis id_condotta, z.id_fossa_settica, z.id_immissione, ac.id_rete,'SIMULATO' as tipo, ac.sub_funzione
            ,z.leng lu_allacci_c, 0 lu_allacci_c_ril,
            0 lu_allacci_i, 0 lu_allacci_i_ril,
            z.cnt nr_allacci_c, 0 nr_allacci_c_ril,
            0 nr_allacci_i, 0 nr_allacci_i_ril
        FROM fgn_condotta ac,
        (
            SELECT d.id_condotta, cc.id_fossa_settica, cc.id_immissione, count(0) cnt, sum(ST_LENGTH(i.geom)) leng
            FROM fgn_immiss_auto d, fgn_link_imm i,
            (
                    select distinct on(fs.idgis) fs.idgis, uc.id_fossa_settica, fs.id_immissione
                    from fgn_fossa_settica fs,
                    (
                        select uct.id_fossa_settica, uct.id_impianto, uct.idgis
                        from utenza_sap us, acq_ubic_contatore uct
                        where us.id_ubic_contatore = uct.idgis
                        AND us.cattariffa NOT IN ('APB_REFIND','APBLREFIND')
                    ) uc
                    where uc.ID_IMPIANTO is not null and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                    and uc.id_fossa_settica = fs.idgis
                    --and uc.idgis='PAAUCO00000002095624'
            ) cc
            WHERE
                d.idgis = cc.id_immissione
                and i.id_fossa_settica = cc.idgis
                and i.id_immissione = cc.id_immissione
            group by d.id_condotta, cc.id_fossa_settica, cc.id_immissione

        ) z
        WHERE ac.idgis = z.id_condotta
        AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
        AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
        AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
        AND (ac.D_GESTORE = 'PUBLIACQUA') AND ac.SUB_FUNZIONE in (1,2)
        AND (ac.D_TIPO_ACQUA in ('MIS','NER','SCA') or ac.D_TIPO_ACQUA IS NULL)

        UNION ALL

        -- 3) SIMULAZIONE ALLACCIO (INDUSTRIALI)
        SELECT ac.idgis id_condotta,z.id_fossa_settica, z.id_immissione, ac.id_rete, 'SIMULATO' as tipo, ac.sub_funzione
            ,0 lu_allacci_c, 0 lu_allacci_c_ril,
            z.leng lu_allacci_i, 0 lu_allacci_i_ril,
            0 nr_allacci_c, 0 nr_allacci_c_ril,
            z.cnt nr_allacci_i, 0 nr_allacci_i_ril
        FROM fgn_condotta ac,
        (
            SELECT d.id_condotta,cc.id_fossa_settica, cc.id_immissione, count(0) cnt, sum(ST_LENGTH(i.geom)) leng
            FROM fgn_immiss_auto d, fgn_link_imm i,
            (
                    select distinct on(fs.idgis) fs.idgis, uc.id_fossa_settica id_fossa_settica, fs.id_immissione
                    from fgn_fossa_settica fs,
                    (
                        select uct.id_fossa_settica, uct.id_impianto, uct.idgis
                        from utenza_sap us, acq_ubic_contatore uct
                        where us.id_ubic_contatore = uct.idgis
                        AND us.cattariffa IN ('APB_REFIND','APBLREFIND')
                    ) uc
                    where uc.ID_IMPIANTO is not null and NOT EXISTS (select distinct idgis_divisionale from utenza_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY') and uc.idgis=idgis_divisionale)
                    and uc.id_fossa_settica = fs.idgis
                    --and uc.idgis='PAAUCO00000002095624'
            ) cc
            WHERE
                d.idgis = cc.id_immissione
                and i.id_fossa_settica = cc.idgis
                and i.id_immissione = cc.id_immissione
            group by d.id_condotta, cc.id_fossa_settica, cc.id_immissione

        ) z
        WHERE ac.idgis = z.id_condotta
        AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null)
        AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL)
        AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null)
        AND (ac.D_GESTORE = 'PUBLIACQUA') AND ac.SUB_FUNZIONE in (1,2)
        AND (ac.D_TIPO_ACQUA in ('MIS','NER','SCA') or ac.D_TIPO_ACQUA IS NULL)
    ) x GROUP BY x.id_condotta, x.id_fossa_settica, id_immissione, tipo;

    UPDATE FGN_COND_ALTRO
    SET
         lu_allacci_c	  = w.lu_allacci_c
        ,lu_allacci_c_ril = w.lu_allacci_c_ril
        ,lu_allacci_i	  = w.lu_allacci_i
        ,lu_allacci_i_ril = w.lu_allacci_i_ril
        ,nr_allacci_c	  = w.nr_allacci_c
        ,nr_allacci_c_ril = w.nr_allacci_c_ril
        ,nr_allacci_i	  = w.nr_allacci_i
        ,nr_allacci_i_ril = w.nr_allacci_i_ril
    FROM (
        SELECT
             sum(lu_allacci_c) as lu_allacci_c
            ,sum(lu_allacci_c_ril) as lu_allacci_c_ril
            ,sum(lu_allacci_i) as lu_allacci_i
            ,sum(lu_allacci_i_ril) as lu_allacci_i_ril
            ,sum(nr_allacci_c) as nr_allacci_c
            ,sum(nr_allacci_c_ril) as nr_allacci_c_ril
            ,sum(nr_allacci_i) as nr_allacci_i
            ,sum(nr_allacci_i_ril) as nr_allacci_i_ril
            ,x.idgis
        FROM (
            select
                lu_allacci_c,
                lu_allacci_c_ril,
                lu_allacci_i,
                lu_allacci_i_ril,
                nr_allacci_c,
                nr_allacci_c_ril,
                nr_allacci_i,
                nr_allacci_i_ril,
                id_condotta as idgis
            from
                support_fgn_allacci) x GROUP BY x.idgis
    ) w WHERE w.idgis = FGN_COND_ALTRO.idgis;


	--
	INSERT INTO FGN_LUNGHEZZA_ALLACCI(
		idgis, codice_ato, tipo_infr, 
		nr_allacci_c, lung_alla_c, 
		nr_allacci_i, lung_alla_i, 
		nr_allacci_c_ril, lung_alla_c_ril, 
		nr_allacci_i_ril, lung_alla_i_ril
	)
	SELECT id_rete, codice_ato, tipo_infr, 
		sum(nr_allacci_c), sum(lu_allacci_c)/1000, 
		sum(nr_allacci_i), sum(lu_allacci_i)/1000, 
		sum(nr_allacci_c_ril), sum(lu_allacci_c_ril)/1000,
		sum(nr_allacci_i_ril), sum(lu_allacci_i_ril)/1000
	FROM FGN_COND_ALTRO 
	WHERE id_rete is NOT NULL
	GROUP BY id_rete, codice_ato, tipo_infr;

    INSERT INTO fgn_allaccio
    with is_industriale as (
    select
        distinct on
        (cc.idgis) uc.id_fossa_settica,
        ac.d_tipo_utenza
    from
        fgn_fossa_settica cc
    join acq_ubic_contatore uc on
        cc.idgis = uc.id_fossa_settica
    join acq_contatore ac on
        ac.id_ubic_contatore = uc.idgis
    where
        uc.ID_IMPIANTO is not null
        and not exists (
        select
            distinct idgis_divisionale
        from
            utenza_defalco
        where
            dt_fine_val = to_date('31-12-9999', 'DD-MM-YYYY')
            and uc.idgis = idgis_divisionale))
    select
        sfa.id_fossa_settica id_fossa,
        id_condotta,
        id_immissione,
        (sum(lu_allacci_c) + sum(lu_allacci_c_ril) + sum(lu_allacci_i)+ sum(lu_allacci_i_ril)) lung_all,
        tipo,
        case when d_tipo_utenza = 'IND' then 'SI' else 'NO' end as industriale
    from
        support_fgn_allacci sfa
        join is_industriale is_ind on sfa.id_fossa_settica = is_ind.id_fossa_settica
    where
        exists (
        select
            id_fossa_settica
        from
            is_industriale ind
        where
            ind.id_fossa_settica = sfa.id_fossa_settica )
    group by
        sfa.id_fossa_settica,
        id_condotta,
        id_immissione,
        tipo,
        d_tipo_utenza;

    --- AGGREGAZIONE PER IDRETE ATO PER LA LUNGHEZZA TOTALE ALLACCI
    INSERT INTO FGN_LUNGHEZZA_ALLACCI_id_rete(
        id_rete, lunghezza_allaccio
    )
    select fca.id_rete, sum(fa.lungh_all)/1000 lungh
    from fgn_allaccio fa
    join fgn_cond_altro fca
    on fa.id_condotta = fca.idgis
    join fgn_rete_racc frc
    on frc.idgis =fca.id_rete
    WHERE frc.d_gestore = 'PUBLIACQUA' AND frc.d_ambito IN ('AT3', NULL) AND frc.d_stato NOT IN ('IPR','IAC')
    group by id_rete;

	--
	-- LOG ANOMALIES
	-- TODO: insert into LOG_STANDALONE
	-- 
	
	RETURN TRUE;
	
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data for shape acquedotto
-- (Ref. 5.4. SHAPE ACQUEDOTTO)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_acq_shape();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_acq_shape(
) RETURNS BOOLEAN AS $$
BEGIN
	
	DELETE FROM ACQ_SHAPE;

    INSERT INTO ACQ_SHAPE(
         tipo_rete
        ,ids_codice
        ,ids_codi_1
        ,id_materia
        ,idx_materi
        ,diametro
        ,idx_diamet
        ,anno
        ,idx_anno
        ,lunghez_1
        ,idx_lunghe
        ,id_conserv
        ,gestione_p
        ,id_tipo_te
        ,protezione
        ,geom
    )
    SELECT
        'ADDUZIONE', codice_ato, idgis, id_materiale, idx_materiale, diametro, idx_diametro, anno, idx_anno, lunghezza*1000, idx_lunghezza, id_conservazione, pressione, id_tipo_telecon, protezione_catodica, geom
    FROM addut_tronchi
    UNION ALL
    SELECT
        'DISTRIBUZIONE', codice_ato, idgis, id_materiale, idx_materiale, diametro, idx_diametro, anno, idx_anno, lunghezza*1000, idx_lunghezza, id_conservazione, pressione, id_tipo_telecon, 0::BIT protezione_catodica, geom
    FROM distrib_tronchi;

    -- (comune_nome, id_comune_istat)
    UPDATE ACQ_SHAPE
    SET comune_nom = t.denom, id_comune_ = t.cod_istat
    FROM (
        --SELECT c.idgis, cc.denom, cc.cod_istat
        --FROM acq_condotta c, confine_comunale cc
        --WHERE c.cod_comune = cc.pro_com_tx
        SELECT c.idgis, cc.denom, cc.cod_istat
        FROM (
            select c.idgis, coalesce(d.pro_com_acc, c.cod_comune::INTEGER) cod_comune
            from acq_condotta c left JOIN decod_com d on c.cod_comune::INTEGER = d.pro_com
        ) c, confine_comunale cc
        WHERE c.cod_comune = cc.pro_com
        --LIMIT 10
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
    -- (tipo_acqua)
    UPDATE ACQ_SHAPE
    SET tipo_acqua = t.valore_netsic
    FROM (
        SELECT c.idgis, d.valore_netsic
        FROM acq_condotta c, all_domains d
        WHERE d.dominio_gis = 'D_T_ACQUA_IDR'
        AND d.valore_gis = c.d_tipo_acqua
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
    --(funziona_gravita)
    UPDATE ACQ_SHAPE
    SET funziona_g = t.valore_netsic
    FROM (
        SELECT c.idgis, d.valore_netsic
        FROM acq_condotta c, all_domains d
        WHERE d.dominio_gis = 'D_T_SCORRIM'
        AND d.valore_gis = c.d_funzionam
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
    --(copertura)
    UPDATE ACQ_SHAPE
    SET copertura = t.valore_netsic
    FROM (
        SELECT c.idgis, d.valore_netsic
        FROM acq_condotta c, all_domains d
        WHERE COALESCE(c.d_pavimentaz,'SCO') in ('SCO','ALT')
        AND d.dominio_gis = 'D_UBICAZIONE'
        AND d.valore_gis = COALESCE(c.d_ubicazione,'SCO')
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
    UPDATE ACQ_SHAPE
    SET copertura = t.valore_netsic
    FROM (
        SELECT c.idgis, d.valore_netsic
        FROM acq_condotta c, all_domains d
        WHERE COALESCE(c.d_pavimentaz,'SCO') NOT IN ('SCO','ALT')
        AND d.dominio_gis = 'D_MAT_PAVIMENT'
        AND d.valore_gis = c.d_pavimentaz
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
    --(profondita, idx_profon)
    UPDATE ACQ_SHAPE
    SET
        profondita = c.prof_media,
        idx_profon = case when c.prof_media <> 0 THEN 'A' ELSE 'X' END
    FROM acq_condotta c
    WHERE c.idgis = ACQ_SHAPE.ids_codi_1;
    --(press_med_eserc, riparazioni_allacci, riparazioni_rete, allacci, lunghezza_allacci)

    -- AGGIUNTA COUNTER E LUGHEZZE
    UPDATE ACQ_SHAPE
    SET
        allacci = counter,
        lunghezza_ = lung*1000
    FROM (select id_condotta, sum(nr_cassette) as counter,sum(lungh_all) as lung from acq_allaccio group by 1) c
    WHERE c.id_condotta = ACQ_SHAPE.ids_codi_1;

    UPDATE ACQ_SHAPE
    SET
        press_med_ = c.pr_avg,
        RIPARAZION = c.rip_alla,
        RIPARAZI_1 = c.rip_rete
    FROM acq_cond_ext c
    WHERE c.idgis = ACQ_SHAPE.ids_codi_1;

    --(protezione_catodica)-> solo DISTRIBUZIONE (ADDUZIONE precedentemente calcolato)
    UPDATE ACQ_SHAPE
    SET protezione = 1::BIT
    FROM acq_condotta c
    WHERE c.id_sist_prot_cat is not null
        AND c.idgis = ACQ_SHAPE.ids_codi_1;

    --(utenze_misuratore)
    update
        acq_shape
    set
        UTENZE_MIS = counter
    from
        (
        select
            ids_codi_1,
            count(*) as counter
        from
            acq_shape as2
        join acq_allaccio aa on
            as2.ids_codi_1 = aa.id_condotta
        join acq_ubic_contatore auc on
            auc.id_cass_cont = aa.id_cassetta
        join utenza_sap us on
            auc.idgis = us.id_ubic_contatore
        where
            COALESCE(us.cattariffa,'?') not in (
				'APB_REFIND',
				'APBLREFIND',
				'APBNREFCIV',
				'APBHSUBDIS',
				'COPDCI0000',
				'COPDIN0000')
            and nr_contat >1
        group by
            1) g
    where
        acq_shape.ids_codi_1 = g.ids_codi_1;


    --(id_opera_stato)
    UPDATE ACQ_SHAPE
    SET id_opera_s = t.valore_netsic
    FROM (
        SELECT c.idgis, d.valore_netsic
        FROM acq_condotta c, all_domains d
        WHERE d.dominio_gis = 'D_STATO'
        AND d.valore_gis = c.d_stato
    ) t WHERE t.idgis = ACQ_SHAPE.ids_codi_1;
	
	-- LOG ANOMALIES
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'ACQ_SHAPE';
	
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT ids_codi_1, 'ACQ_SHAPE', 'Tratto con tipo_acqua diverso da GREZZA o TRATTATA' 
	FROM ACQ_SHAPE WHERE tipo_acqua NOT IN ('GREZZA','TRATTATA');
	
	RETURN TRUE;
	
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
--------------------------------------------------------------------
-- Populate data for ACQUEDOTTO
-- (Ref. 5.1, 5.2,5.3,5.4,5.5,7.1)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_acquedotto();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_acquedotto(
) RETURNS BOOLEAN AS $$
begin
	RETURN 
		populate_distrib_tronchi() 
	AND populate_addut_tronchi() 
	AND populate_lung_rete_acq()
	AND determine_acq_allacci()
	AND populate_acq_vol_utenze()
	AND populate_acq_shape()
	AND populate_ubic_allaccio()
	and populate_utenze_distribuzioni_adduttrici()
    and populate_stats_cloratore()
	and populate_schema_acq()
	and populate_codice_capt_accorp();
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;

--------------------------------------------------------------------
-- Populate data for volumes for Acquedotto
-- (Ref. 4.2)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_acq_vol_utenze();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_acq_vol_utenze(
) RETURNS BOOLEAN AS $$
BEGIN

	DELETE FROM ACQ_VOL_UTENZE;

	WITH utenze_per_tipo as (
        SELECT
            us2.ids_codice_orig_acq,
            nr_contat_diam_min,
            us.tipo_uso,
            us.nr_contat,
            count(*) as countUtenze,
            sum(vol_acq_ero) as sumVolAcqEro,
            sum(vol_acq_fatt) as sumVolAcqFatt
        FROM
            utenza_sap us
        LEFT JOIN utenza_servizio us2 on
            us.id_ubic_contatore = us2.id_ubic_contatore
        WHERE
            us.CATTARIFFA not in ('APB_REFIND',
            'APBLREFIND',
            'APBNREFCIV')
        GROUP BY
            us2.ids_codice_orig_acq,
            nr_contat_diam_min,
            us.tipo_uso,
            us.nr_contat),
        utenze_totali as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as totalCount
        FROM
            utenze_per_tipo
        GROUP BY
            ids_codice_orig_acq),
        utenze_domestiche as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countDomestiche,
            sum(sumVolAcqFatt) as sumDomesticheVolFatt
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'DOMESTICO'
        GROUP BY
            ids_codice_orig_acq),
        utenze_domestiche_residente as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countDomesticheResidente,
            sum(sumVolAcqFatt) as sumDomesticheResidenteVolFatt
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'DOMESTICO RESIDENTE'
        GROUP BY
            ids_codice_orig_acq),
        utenze_domestiche_diam_min as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countDomesticheDiamMin
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'DOMESTICO'
            and nr_contat_diam_min >= 1
        GROUP BY
            ids_codice_orig_acq),
        utenze_commerciali as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countCommerciali
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'COMMERCIALE'
        GROUP BY
            ids_codice_orig_acq),
        utenze_pubblico as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countPubblico,
            sum(sumVolAcqFatt) as sumPubblicoeVolFatt
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'PUBBLICO'
        GROUP BY
            ids_codice_orig_acq),
        utenze_industriale as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countIndustriale
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'INDUSTRIALE'
        GROUP BY
            ids_codice_orig_acq),
        utenze_con_misuratore as (
        SELECT
            ids_codice_orig_acq,
            sum(countUtenze) as countUtenzeConMisuratore
        FROM
            utenze_per_tipo
        WHERE
            nr_contat >= 1
        GROUP BY
            ids_codice_orig_acq),
        volume_erogato as (
        SELECT
            ids_codice_orig_acq,
            sum(sumVolAcqEro) as sumVolAcqEro
        FROM
            utenze_per_tipo
        GROUP BY
            ids_codice_orig_acq),
        volume_fatturato as (
        SELECT
            ids_codice_orig_acq,
            sum(sumVolAcqFatt) as sumVolAcqFatt
        FROM
            utenze_per_tipo
        GROUP BY
            ids_codice_orig_acq),
        volume_fatturato_altro as (
        SELECT
            ids_codice_orig_acq,
            sum(sumVolAcqFatt) as sumAltroVolFatt
        FROM
            utenze_per_tipo
        WHERE
            tipo_uso = 'ALTRO'
        GROUP BY
            ids_codice_orig_acq)
        INSERT INTO
            ACQ_VOL_UTENZE(ids_codice_orig_acq,
            totalCount,
            countDomestiche,
            countDomesticheResidente,
            countDomesticheDiamMin,
            countCommerciali,
            countPubblico,
            countIndustriale,
            countutenzeconmisuratore,
            sumVolAcqEro,
            sumVolAcqFatt,
            sumDomesticheVolFatt,
            sumDomesticheResidenteVolFatt,
            sumPubblicoeVolFatt,
            sumAltroVolFatt)
        SELECT
            ut.ids_codice_orig_acq,
            totalCount,
            countDomestiche,
            countDomesticheResidente,
            countDomesticheDiamMin,
            countCommerciali,
            countPubblico,
            countIndustriale,
            countutenzeconmisuratore,
            ve.sumVolAcqEro,
            vf.sumVolAcqFatt,
            sumDomesticheVolFatt,
            sumDomesticheResidenteVolFatt,
            sumPubblicoeVolFatt,
            sumAltroVolFatt
        FROM
            utenze_totali ut
        LEFT JOIN utenze_domestiche ud on
            ut.ids_codice_orig_acq = ud.ids_codice_orig_acq
        LEFT JOIN utenze_domestiche_residente udr on
            ut.ids_codice_orig_acq = udr.ids_codice_orig_acq
        LEFT JOIN utenze_domestiche_diam_min udm on
            ut.ids_codice_orig_acq = udm.ids_codice_orig_acq
        LEFT JOIN utenze_commerciali uc on
            ut.ids_codice_orig_acq = uc.ids_codice_orig_acq
        LEFT JOIN utenze_pubblico up on
            ut.ids_codice_orig_acq = up.ids_codice_orig_acq
        LEFT JOIN utenze_industriale ui on
            ut.ids_codice_orig_acq = ui.ids_codice_orig_acq
        LEFT JOIN utenze_con_misuratore ucm on
            ut.ids_codice_orig_acq = ucm.ids_codice_orig_acq
        LEFT JOIN volume_erogato ve on
            ut.ids_codice_orig_acq = ve.ids_codice_orig_acq
        LEFT JOIN volume_fatturato vf on
            ut.ids_codice_orig_acq = vf.ids_codice_orig_acq
        LEFT JOIN volume_fatturato_altro vfa on
            ut.ids_codice_orig_acq = vfa.ids_codice_orig_acq;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data for shape fognatura
-- (Ref. 6.4. SHAPE FOGNATURA)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_fgn_shape();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_fgn_shape(
) RETURNS BOOLEAN AS $$
BEGIN

	DELETE FROM FGN_SHAPE;

	INSERT INTO FGN_SHAPE(
		 tipo_rete
		,ids_codice
		,ids_codi_1
		,id_materia
		,idx_materi
		,diametro
		,idx_diamet
		,anno
		,idx_anno
		,lunghez_1
		,idx_lunghe
		,id_conserv
		,id_refluo_
		,funziona_g
		,recapito
		,geom
	)
	SELECT
		'FOGNATURA', codice_ato, idgis, to_integer(id_materiale), idx_materiale,
		to_integer(diametro), idx_diametro, anno, idx_anno, lunghezza*1000 , idx_lunghezza, to_integer(id_conservazione),
		id_refluo_trasportato::INTEGER, funziona_gravita, recapito, geom
	FROM FOGNAT_TRONCHI
	UNION ALL
	SELECT
		'COLLETTORE', codice_ato, idgis, to_integer(id_materiale), idx_materiale,
		to_integer(diametro), idx_diametro, anno, idx_anno, lunghezza*1000, idx_lunghezza, to_integer(id_conservazione),
		id_refluo_trasportato::INTEGER, funziona_gravita, recapito, geom
	FROM COLLETT_TRONCHI;

	-- (comune_nome, id_comune_istat)
	UPDATE FGN_SHAPE
	SET comune_nom = t.denom, id_comune_ = t.pro_com
	FROM (
		--SELECT c.idgis, cc.denom, cc.cod_istat
		--FROM fgn_condotta c, confine_comunale cc
		--WHERE c.cod_comune = cc.pro_com_tx
		SELECT c.idgis, cc.denom, cc.pro_com
		FROM (
			select c.idgis, coalesce(d.pro_com_acc, c.cod_comune::INTEGER) cod_comune
			from fgn_condotta c left JOIN decod_com d on c.cod_comune::INTEGER = d.pro_com
		) c, confine_comunale cc
		WHERE c.cod_comune = cc.pro_com
		--LIMIT 10
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;

	--(funziona_gravita)
	UPDATE FGN_SHAPE
	SET funziona_g = t.valore_netsic
	FROM (
		SELECT c.idgis, d.valore_netsic
		FROM fgn_condotta c, all_domains d
		WHERE d.dominio_gis = 'D_T_SCORRIM'
		AND d.valore_gis = c.d_funzionam
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;
	--(sezione)
	UPDATE FGN_SHAPE
	SET  sezione = t.sezione
		,prof_inizi = t.quota_in_rel
		,prof_final = t.quota_fn_rel
		,idx_profon = CASE WHEN t.quota_in_rel = 0 and t.quota_fn_rel = 0 THEN 'X' ELSE 'A' END
	FROM (
		SELECT
			c.idgis,
			CASE
				WHEN c.d_tipo_sezione IN ('RET','REC') THEN
					'RETTANGOLARE'
				WHEN c.d_tipo_sezione IN ('CIR','CIS','CIC') THEN
					'CIRCOLARE'
				WHEN c.d_tipo_sezione IN ('OVO','VIG','OVS','OVC') THEN
					'OVOIDALE'
				WHEN c.d_tipo_sezione IN ('VOL','TRA','SCO','CAT','CAA','ALT') THEN
					'ALTRO'
				ELSE null
			END sezione,
			coalesce(quota_in_rel,0) quota_in_rel,
			coalesce(quota_fn_rel,0) quota_fn_rel
		FROM fgn_condotta c
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;
	--(copertura)
	UPDATE FGN_SHAPE
	SET copertura = t.valore_netsic
	FROM (
		SELECT c.idgis, d.valore_netsic
		FROM fgn_condotta c, all_domains d
		WHERE COALESCE(c.d_pavimentaz,'SCO') in ('SCO','ALT')
		AND d.dominio_gis = 'D_UBICAZIONE'
		AND d.valore_gis = COALESCE(c.d_ubicazione,'SCO')
		--and d.valore_netsic !='ASFALTO SIMILI'
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;
	UPDATE FGN_SHAPE
	SET copertura = t.valore_netsic
	FROM (
		SELECT c.idgis, d.valore_netsic
		FROM fgn_condotta c, all_domains d
		WHERE COALESCE(c.d_pavimentaz,'SCO') NOT IN ('SCO','ALT')
		AND d.dominio_gis = 'D_MAT_PAVIMENT'
		AND d.valore_gis = c.d_pavimentaz
		--and d.valore_netsic !='ASFALTO SIMILI'
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;

	--(allacci, allacci_industriali, lunghezza_allaci)
	UPDATE FGN_SHAPE
	SET
		allacci    = nr_allacci,
		allacci_in = nr_allacci_ind,
		lunghezza_ = lunghezza
	FROM (select
        ids_codi_1,
        sum(lungh_all) as lunghezza,
        sum(case when fa.industriale = 'NO' then 1 else 0 end) as nr_allacci,
        sum(case when fa.industriale = 'SI' then 1 else 0 end) as nr_allacci_ind
    from
        fgn_shape fs
    join fgn_allaccio fa on
        fs.ids_codi_1 = fa.id_condotta
    group by
        1) c
    WHERE c.ids_codi_1 = FGN_SHAPE.ids_codi_1;

	--(riparazioni_allacci, riparazioni_rete)
	UPDATE FGN_SHAPE
	SET
		RIPARAZION = c.rip_alla,
		RIPARAZI_1 = c.rip_rete
	FROM FGN_COND_EXT c
	WHERE c.idgis = FGN_SHAPE.ids_codi_1;

	--(id_opera_stato)
	UPDATE FGN_SHAPE
	SET id_opera_s = t.valore_netsic
	FROM (
		SELECT c.idgis, d.valore_netsic
		FROM fgn_condotta c, all_domains d
		WHERE d.dominio_gis = 'D_STATO'
		AND d.valore_gis = c.d_stato
	) t WHERE t.idgis = FGN_SHAPE.ids_codi_1;


	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;

--------------------------------------------------------------------
-- Populate data for FOGNATURA
-- (Ref. 6.1, 6.2, 6.3, 6.4, 6.5, 7.2)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_fognatura();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_fognatura(
) RETURNS BOOLEAN AS $$
BEGIN
	RETURN 
		populate_fognat_tronchi() 
	AND populate_collett_tronchi() 
	AND populate_lung_rete_fgn()
	AND determine_fgn_allacci()
	AND populate_fgn_volumi_utenze()
	AND populate_ubic_f_allaccio()
	and populate_utenze_fognature_collettori()
	AND populate_fgn_shape();
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;

--------------------------------------------------------------------
-- Populate data for volumes for FOGNATURA
-- (Ref. 4.2)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_fgn_volumi_utenze();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_fgn_volumi_utenze(
) RETURNS BOOLEAN AS $$
BEGIN

	DELETE FROM FGN_VOL_UTENZE;

	with utenze_autorizzate as (
        SELECT
            us2.ids_codice_orig_FGN,
            count(*) as utenze_prod_auth
        FROM
            utenza_sap us
        LEFT JOIN utenza_servizio us2 ON
            us.id_ubic_contatore = us2.id_ubic_contatore
        WHERE
            us.CATTARIFFA in ('APB_REFIN',
            'APBLREFIND')
        GROUP BY
            us2.ids_codice_orig_FGN),
        volume_fatturato as (
        SELECT
            us2.ids_codice_orig_FGN,
            sum(VOL_FGN_FATT) as vol_fatturato
        FROM
            utenza_sap us
        LEFT JOIN utenza_servizio us2 ON
            us.id_ubic_contatore = us2.id_ubic_contatore
        GROUP BY
            us2.ids_codice_orig_FGN),
        volume_utenze_autorizzate as (
        SELECT
            us2.ids_codice_orig_FGN,
            sum(vol_fgn_fatt) as vol_utenze_auth
        FROM
            utenza_sap us
        LEFT JOIN utenza_servizio us2 ON
            us.id_ubic_contatore = us2.id_ubic_contatore
        WHERE
            us.CATTARIFFA in ('APB_REFIN',
            'APBLREFIND')
        GROUP BY
            us2.ids_codice_orig_FGN)
        INSERT INTO FGN_VOL_UTENZE (ids_codice_orig_FGN, utenze_prod_auth, vol_fatturato, vol_utenze_auth)
        SELECT
            volume_fatturato.ids_codice_orig_FGN,
            utenze_prod_auth,
            vol_fatturato,
            vol_utenze_auth
        FROM
            volume_fatturato
        LEFT JOIN utenze_autorizzate ON
            volume_fatturato.ids_codice_orig_fgn = utenze_autorizzate.ids_codice_orig_fgn
        LEFT JOIN volume_utenze_autorizzate ON
            volume_fatturato.ids_codice_orig_fgn = volume_utenze_autorizzate.ids_codice_orig_fgn;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
--------------------------------------------------------------------
-- Populate data into the XXX_POMPE tables 
--  * POZZI_POMPE
--  * POTAB_POMPE
--  * POMPAGGI_POMPE
--  * SOLLEV_POMPE
--  * DEPURATO_POMPE
-- using information from ARCHIVIO_POMPE table
-- (Ref. 8. Archivio POMPE)
-- OUT: BOOLEAN
-- Example:
-- 	select DBIAIT_ANALYSIS.populate_archivi_pompe();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_archivi_pompe(
) RETURNS BOOLEAN AS $$
DECLARE 
	v_t INTEGER;
	v_f INTEGER;
	v_in_tables VARCHAR[] := ARRAY['acq_captazione', 'acq_potabiliz', 'acq_pompaggio', 'fgn_imp_sollev', 'fgn_trattamento'];
	v_tables VARCHAR[] := ARRAY['POZZI_POMPE', 'POTAB_POMPE', 'POMPAGGI_POMPE', 'SOLLEV_POMPE', 'DEPURATO_POMPE'];
	v_filters VARCHAR[] := ARRAY[
		'p.tipo_oggetto = ''ACQ_CAPTAZIONE'' AND c.sub_funzione = 3', 
		'p.tipo_oggetto = ''ACQ_POTABILIZ''',  
		'p.tipo_oggetto = ''ACQ_POMPAGGIO''', 
		'p.tipo_oggetto = ''FGN_IMP_SOLLEV''', 
		'p.tipo_oggetto = ''FGN_TRATTAMENTO'''
	];
	--POZZI_POMPE -> 	[acq_captazione]  -> tipo_oggetto = 'ACQ_CAPTAZIONE' AND c.sub_funzione = 3
	--POTAB_POMPE -> 	[acq_potabiliz]   -> tipo_oggetto = 'ACQ_POTABILIZ'
	--POMPAGGI_POMPE -> [acq_pompaggio]   -> tipo_oggetto = 'ACQ_POMPAGGIO'
	--SOLLEV_POMPE -> 	[fgn_imp_sollev]  -> tipo_oggetto = 'FGN_IMP_SOLLEV' 
	--DEPURATO_POMPE -> [fgn_trattamento] -> tipo_oggetto = 'FGN_TRATTAMENTO'
	v_fields VARCHAR[] := ARRAY['IDX_ANNO_INSTAL', 'IDX_ANNO_RISTR', 'IDX_POTENZA', 'IDX_PORTATA', 'IDX_PREVALENZA'];
BEGIN

	DELETE FROM STATS_POMPE;
	DELETE FROM SUPPORT_POZZI_INPOTAB;
	
    FOR v_t IN array_lower(v_tables,1) .. array_upper(v_tables,1)
	LOOP
	
		EXECUTE 'DELETE FROM ' || v_tables[v_t];
		EXECUTE '
			INSERT INTO ' || v_tables[v_t] || '(
				CODICE_ATO, D_STATO_CONS, ANNO_INSTAL,
				ANNO_RISTR, POTENZA, PORTATA, 
				PREVALENZA, SN_RISERVA,
				IDX_ANNO_INSTAL, IDX_ANNO_RISTR,
				IDX_POTENZA, IDX_PORTATA, IDX_PREVALENZA
			)
			SELECT 
				c.CODICE_ATO, p.D_STATO_CONS, p.ANNO_INSTAL, 
				p.ANNO_RISTR, p.POTENZA, p.PORTATA, 
				p.PREVALENZA, 
				CASE WHEN 
					p.SN_RISERVA = ''NO'' THEN 0::BIT
				ELSE 1::BIT END,
				p.A_ANNO_INSTAL, p.A_ANNO_RISTR,
				p.A_POTENZA, p.A_PORTATA, p.A_PREVALENZA				
			FROM ' || v_in_tables[v_t] || ' c, ARCHIVIO_POMPE p
			WHERE 
				' || v_filters[v_t] ||' 
				and c.d_gestore = ''PUBLIACQUA'' AND c.d_ambito IN (''AT3'', NULL) 
				and c.d_stato in (''ATT'',''FIP'')
				and c.idgis = p.id_oggetto';

			FOR v_f IN array_lower(v_fields,1) .. array_upper(v_fields,1)
			LOOP
				EXECUTE '
					UPDATE ' || v_tables[v_t] || '
					SET ' || v_fields[v_f] || ' = d.valore_netsic
					FROM all_domains d
					WHERE d.dominio_netsic = ''id_indice_idx'' 
					AND d.valore_gis = ' || v_tables[v_t] || '.' || v_fields[v_f] || ';';
			END LOOP;		
		
		-- Statistics
		EXECUTE '
		INSERT INTO STATS_POMPE(codice_ato, sum_potenza, avg_idx_potenza)
		SELECT t1.codice_ato, t1.sum_potenza, t2.idx_potenza 
		FROM 
		(
			SELECT 
				codice_ato,
				SUM(
					CASE WHEN sn_riserva=0::BIT THEN potenza ELSE 0 END
				) sum_potenza
			FROM ' || v_tables[v_t] || ' 
			GROUP BY codice_ato
		) t1,
		(
			select codice_ato,
			   case when t.avg_potenza = 1 then ''A''
					when t.avg_potenza = 2 then ''C''
					when t.avg_potenza = 3 then ''D''
					else ''B''
			   end idx_potenza
			FROM(
				SELECT codice_ato, 
				ROUND(avg(
					case when idx_potenza=''A'' then 1
						 when idx_potenza=''C'' then 2
						 when idx_potenza=''D'' then 3
						 else 0
					end
				)) avg_potenza
				FROM ' || v_tables[v_t] || '
				GROUP BY codice_ato
			) t
		) t2
		WHERE t1.codice_ato = t2.codice_ato;';
		
	END LOOP;	
	
	--
	INSERT INTO SUPPORT_POZZI_INPOTAB(ids_codice, volume_medio_prel)
	select 
		ac.codice_ato, 
		case when pz.ids_codice is null then ac.volume_medio_prel
		else 0 
		end volume_medio_prel 
	from a_acq_captazione ac 
	left join pozzi_inpotab pz
	on ac.codice_ato = pz.ids_codice
	union ALL
	select 
		ac.codice_ato, 
		case when pz.ids_codice is null then ac.volume_medio_prel
		else 0 
		end volume_medio_prel 
	from acq_captazione ac 
	left join pozzi_inpotab pz
	on ac.codice_ato = pz.ids_codice;
	
	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;		
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- Partition table by year, note
--
-- select DBIAIT_FREEZE.initialize_freeze_table('acq_accumulo',2021,'example for 2021');
-- 
CREATE OR REPLACE FUNCTION DBIAIT_FREEZE.initialize_freeze_table(
	v_table_name VARCHAR,
	v_year INTEGER,
	v_note TEXT
) RETURNS BOOLEAN AS $$
DECLARE 
	v_count INTEGER:=0;
	v_schema_anl VARCHAR(16) := 'dbiait_analysis';	
	v_schema_frz VARCHAR(16) := 'dbiait_freeze';
	v_def_note VARCHAR(4000) := '';
BEGIN
	-- check if the child table (by year) already exists
	SELECT count(0) INTO v_count
	FROM information_schema.tables 
	WHERE UPPER(table_schema) = UPPER(v_schema_frz) 
	and UPPER(table_name) = UPPER(v_table_name || '_' || v_year)
	and table_type = 'BASE TABLE';
	
	IF v_count = 0 THEN
		-- create a child table with 2 additional fields (_year, _note)
		EXECUTE 'CREATE TABLE ' || v_schema_frz || '.' || v_table_name || '_' || v_year || ' AS TABLE ' || v_schema_anl || '.' || v_table_name;
		
		EXECUTE 'ALTER TABLE ' || v_schema_frz || '.' || v_table_name || '_' || v_year || ' ADD COLUMN _year INTEGER NOT NULL DEFAULT ' || v_year;
		
		EXECUTE 'ALTER TABLE ' || v_schema_frz || '.' || v_table_name || '_' || v_year || ' ADD COLUMN _note TEXT';
		
		IF v_note IS NOT NULL THEN
			EXECUTE 'UPDATE ' || v_schema_frz || '.' || v_table_name || '_' || v_year || ' SET _note = $1 ' using v_note;
		END IF;
	ELSE
		-- Raise an exception: we cannot initialize an existing partition
		RAISE EXCEPTION 'Table % is already partitioned for year %', v_table_name, v_year; 
	END IF;
	
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_FREEZE;
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- Remove all tables by year
--
-- select DBIAIT_FREEZE.freeze_tables(2021);
-- 
CREATE OR REPLACE FUNCTION DBIAIT_FREEZE.freeze_tables(
	v_year INTEGER,
	v_note TEXT
) RETURNS BOOLEAN AS $$
DECLARE
	v_schema_frz VARCHAR(16) := 'dbiait_freeze';
	v_schema_anl VARCHAR(16) := 'dbiait_analysis';
	v_rec RECORD;
	v_res BOOLEAN;
BEGIN
	FOR v_rec IN 
		SELECT table_name::VARCHAR
		FROM information_schema.tables 
		WHERE UPPER(table_schema) = UPPER(v_schema_anl) 
		and table_type = 'BASE TABLE'
	LOOP	
		v_res := initialize_freeze_table(v_rec.table_name, v_year, v_note);
	END LOOP;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_FREEZE;
--------------------------------------------------------------------------------------------
-- Remove a table by name, year
--
-- select DBIAIT_FREEZE.remove_freeze_table('acq_accumulo', 2021);
-- 
CREATE OR REPLACE FUNCTION DBIAIT_FREEZE.remove_freeze_table(
	v_table_name VARCHAR,
	v_year INTEGER
) RETURNS BOOLEAN AS $$
DECLARE
	v_schema_frz VARCHAR(16) := 'dbiait_freeze';
BEGIN
	EXECUTE 'DROP TABLE IF EXISTS ' || v_schema_frz || '.' || v_table_name || '_' || v_year;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_FREEZE;
--------------------------------------------------------------------------------------------
-- Remove all tables by year
--
-- select DBIAIT_FREEZE.remove_freeze_tables(2021);
-- 
CREATE OR REPLACE FUNCTION DBIAIT_FREEZE.remove_freeze_tables(
	v_year INTEGER
) RETURNS BOOLEAN AS $$
DECLARE
	v_schema_frz VARCHAR(16) := 'dbiait_freeze';
	v_rec RECORD;
BEGIN
	FOR v_rec IN 
		SELECT table_name
		FROM information_schema.tables 
		WHERE UPPER(table_schema) = UPPER(v_schema_frz) 
		and table_name like '%_' || v_year
		and table_type = 'BASE TABLE'
	LOOP	
		EXECUTE 'DROP TABLE IF EXISTS ' || v_schema_frz || '.' || v_rec.table_name;
	END LOOP;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_FREEZE;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
--WAITING GRAFO (Fase 2.5 - punto 7)
-- Populate table ADDUT_COM_SERV
-- Example:
-- select DBIAIT_ANALYSIS.populate_addut_com_serv();
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_addut_com_serv(
) RETURNS BOOLEAN AS $$
BEGIN
	
	DELETE FROM addut_com_serv;
	
	INSERT into addut_com_serv(ids_codice, id_comune_istat)
	select DISTINCT t.codice_ato, (c.pro_com::INTEGER)::VARCHAR from (
		select aa.codice_ato, ac.geom
		from acq_adduttrice aa, acq_condotta ac 
		where aa.d_gestore='PUBLIACQUA' 
		and aa.d_ambito IN ('AT3', NULL) 
		--and aa.d_stato IN ('ATT', 'FIP', 'PIF', 'RIS')
		AND ac.d_stato IN ('ATT', 'FIP', NULL) 
		and ac.sn_fittizia in ('NO', NULL)
		and ac.id_rete = aa.idgis 
	) t, confine_comunale c
	where t.geom && c.geom and st_intersects(t.geom, c.geom);
	
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'ADDUT_COM_SERV';
	
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	select codice_ato, 'ADDUT_COM_SERV', 'Tratto intersecante piu'' (' || count(0) || ') di un comune'  from (
	select distinct t.codice_ato, c.pro_com from (
		select aa.codice_ato, ac.geom
		from acq_adduttrice aa, acq_condotta ac 
		where aa.d_gestore='PUBLIACQUA' 
		and aa.d_ambito IN ('AT3', NULL) 
		--and aa.d_stato IN ('ATT', 'FIP', 'PIF', 'RIS')
		AND ac.d_stato IN ('ATT', 'FIP', NULL) 
		and ac.sn_fittizia in ('NO', NULL)
		and ac.id_rete = aa.idgis 
	) t, confine_comunale c
	where t.geom && c.geom and st_intersects(t.geom, c.geom)
	) t2 group by t2.codice_ato having count(0) > 1;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------------------------------------
-- Populate table COLLET_COM_SERV
--
-- Example:
-- select DBIAIT_ANALYSIS.populate_collet_com_serv();
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_collet_com_serv(
) RETURNS BOOLEAN AS $$
BEGIN
	
	DELETE FROM collet_com_serv;
	
	INSERT into collet_com_serv(ids_codice, id_comune_istat)
	SELECT distinct t.codice_ato, c.pro_com FROM (
		SELECT fc.codice_ato, fc2.geom 
		FROM fgn_collettore fc, fgn_condotta fc2 
		WHERE fc.d_gestore='PUBLIACQUA' 
		and fc.d_ambito IN ('AT3', NULL) 
		and fc.d_stato IN ('ATT', 'FIP', 'PIF', 'RIS')
		AND fc2.d_stato IN ('ATT', 'FIP', NULL) 
		AND fc2.sn_fittizia in ('NO', NULL)
		AND fc.idgis=fc2.id_rete
	) t, confine_comunale c
	WHERE t.geom && c.geom and st_intersects(t.geom, c.geom);
	
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'COLLET_COM_SERV';
	
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT codice_ato, 'COLLET_COM_SERV', 'Tratto intersecante piu'' (' || count(0) || ') di un comune'  from (
		select distinct t.codice_ato, c.pro_com from (
		select fc.codice_ato, fc2.geom 
		from fgn_collettore fc, fgn_condotta fc2 
		where fc.d_gestore='PUBLIACQUA' 
		and fc.d_ambito IN ('AT3', NULL) 
		and fc.d_stato IN ('ATT', 'FIP', 'PIF', 'RIS')
		AND fc2.d_stato IN ('ATT', 'FIP', NULL) 
		AND fc2.sn_fittizia in ('NO', NULL)
		and fc.idgis=fc2.id_rete
		) t, confine_comunale c
		where t.geom && c.geom and st_intersects(t.geom, c.geom)
	) t2 group by t2.codice_ato having count(0) > 1;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------------------------------------
-- Populate tables:
--    FIUMI_INRETI 
--    LAGHI_INRETI
--    POZZI_INRETI
--    SORGENTI_INRETI
--    POTAB_INRETI
--    ADDUT_INRETI
--    ACCUMULI_INRETI
--
-- Example:
-- select DBIAIT_ANALYSIS.populate_impianti_inreti();
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_impianti_inreti(
) RETURNS BOOLEAN AS $$
DECLARE
	v_in_tables VARCHAR[] := ARRAY[
		'acq_captazione', 
		'acq_captazione',
		'acq_captazione',
		'acq_captazione',
		'acq_potabiliz',
		'(select aa.codice_ato, aa.idgis, ac.geom from acq_adduttrice aa, acq_condotta ac where aa.d_gestore = ''PUBLIACQUA'' AND aa.d_ambito IN (''AT3'', NULL) AND aa.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'') and ac.d_gestore = ''PUBLIACQUA'' AND ac.d_ambito IN (''AT3'', NULL) AND ac.d_stato IN (''ATT'', ''FIP'', NULL) and ac.sn_fittizia in (''NO'', NULL) and aa.idgis=ac.id_rete)',
		'acq_accumulo'
	];
	v_tables VARCHAR[] := ARRAY[
		'FIUMI_INRETI', 
		'LAGHI_INRETI',
		'POZZI_INRETI',
		'SORGENTI_INRETI',
		'POTAB_INRETI',
		'ADDUT_INRETI',
		'ACCUMULI_INRETI'
	];

	v_touch_flt VARCHAR[] := ARRAY[
		'',
		'',
		'',
		'',
		'',
		' AND ST_TOUCHES(r.geom,t.geom)=FALSE ',
		''
	];

	v_in_fields VARCHAR[] := ARRAY[
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3',
		't.codice_ato, r.codice_ato, 3'
	];
	v_out_fields VARCHAR[] := ARRAY[
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete',
		'ids_codice, ids_codice_rete, id_gestore_rete'
	];
	v_filters VARCHAR[] := ARRAY[
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'') AND t.SUB_FUNZIONE=0',
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'') AND t.SUB_FUNZIONE=1',
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'') AND t.SUB_FUNZIONE=3',
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'') AND t.SUB_FUNZIONE=4',
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'')',
		'',
		'AND t.d_gestore = ''PUBLIACQUA'' AND t.d_ambito IN (''AT3'', NULL) AND t.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'')'
	];
BEGIN
	
	FOR v_t IN array_lower(v_tables,1) .. array_upper(v_tables,1)
	LOOP
		-- Cleanup destination table
		EXECUTE 'DELETE FROM ' || v_tables[v_t] || ';';

		--Populate destination table
		EXECUTE '
		INSERT INTO ' || v_tables[v_t] || '(' || v_out_fields[v_t] || ')
		SELECT DISTINCT ' || v_in_fields[v_t] || ' 
		FROM ' || v_in_tables[v_t] || ' t
		LEFT join acq_rete_distrib r
		  ON r.geom&&t.geom AND ST_INTERSECTS(r.geom,t.geom) ' || v_touch_flt[v_t] || '
		WHERE r.codice_ato is NOT NULL AND r.d_gestore=''PUBLIACQUA'' and r.d_ambito IN (''AT3'', NULL) and r.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'')
		' || v_filters[v_t];
		
		--LOG ANOMALIE
		DELETE FROM LOG_STANDALONE WHERE alg_name = v_tables[v_t];
	
		-- Elementi che intersecano piu' di una rete
		EXECUTE '
		INSERT INTO LOG_STANDALONE (id, alg_name, description)
		SELECT idgis, $1, ''Elemento intersecante piu'''' ('' || count(0) || '') di una rete'' 
		FROM (
			SELECT t.idgis
			FROM ' || v_in_tables[v_t] || ' t
			LEFT JOIN acq_rete_distrib r 
				ON r.geom&&t.geom AND ST_INTERSECTS(r.geom, t.geom) ' || v_touch_flt[v_t] || '
			WHERE r.codice_ato is NOT NULL AND r.d_gestore=''PUBLIACQUA'' and r.d_ambito IN (''AT3'', NULL) and r.d_stato IN (''ATT'', ''FIP'', ''PIF'', ''RIS'')
			' || v_filters[v_t] || '
		) t2 group by t2.idgis having count(0) > 1;' USING v_tables[v_t];
		
		-- Elementi che non intersecano alcuna rete
		EXECUTE '
		INSERT INTO LOG_STANDALONE (id, alg_name, description)
		SELECT t.idgis, $1, ''Elemento non intersecante alcuna rete''
		FROM ' || v_in_tables[v_t] || ' t
		WHERE NOT EXISTS(
		    SELECT TRUE from ' || v_tables[v_t] || ' ai
            where t.codice_ato = ai.ids_codice
		)
		' || v_filters[v_t] USING v_tables[v_t];

	END LOOP;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------------------------------------
-- Populate table ACCUMULI_INADD
-- Example:
-- select DBIAIT_ANALYSIS.populate_accumuli_inadd();
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_accumuli_inadd(
) RETURNS BOOLEAN AS $$
DECLARE
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN
	
	DELETE FROM ACCUMULI_INADD;
	
	INSERT into ACCUMULI_INADD(ids_codice, ids_codice_adduzione, id_gestore_adduzione)
	SELECT DISTINCT t.codice_ato, ad.codice_ato, 3 from (
		SELECT distinct aa.codice_ato, ac.id_rete
		from acq_accumulo aa, acq_condotta ac
		WHERE aa.d_gestore = 'PUBLIACQUA' 
		AND aa.d_ambito IN ('AT3', NULL) 
		AND aa.d_stato IN ('ATT','FIP','PIF','RIS')
		AND ac.d_stato IN ('ATT', 'FIP', NULL) 
		AND ac.sn_fittizia in ('NO', NULL)
		and st_buffer(aa.geom, v_tol)&&ac.geom and st_intersects(ac.geom, st_buffer(aa.geom, v_tol))
	) t, acq_adduttrice ad
	WHERE t.id_rete=ad.idgis
	AND ad.d_gestore = 'PUBLIACQUA' AND ad.d_ambito IN ('AT3', NULL) AND ad.d_stato IN ('ATT','FIP','PIF','RIS');
	
	--INSERT into ACCUMULI_INADD(ids_codice, ids_codice_adduzione, id_gestore_adduzione)
	--select a.codice_ato, c.codice_ato, NULL 
	--from acq_accumulo a left JOIN addut_tronchi c
	--on c.geom&&ST_BUFFER(a.geom, v_tol) and ST_INTERSECTS(c.geom, ST_BUFFER(a.geom, v_tol))
	--WHERE a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	--AND c.codice_ato is not NULL;
		
	--LOG ANOMALIE
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'ACCUMULI_INADD';
	
	-- Elementi che intersecano piu' tronchi di adduzione	
	INSERT INTO LOG_STANDALONE (id, alg_name, description) 
	select a.idgis, 'ACCUMULI_INADD', 'Elemento intersecante piu'' (' || t.cnt || ') di un tronco di adduzione'  
	FROM
	(
		select ids_codice, count(0) cnt
		from ACCUMULI_INADD
		group by ids_codice having count(0) > 1
	) t, acq_accumulo a
	where a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	AND t.ids_codice = a.codice_ato;  
	
	-- Elementi che non intersecano alcun tronco di adduzione
	INSERT INTO LOG_STANDALONE (id, alg_name, description)	
	select idgis, 'ACCUMULI_INADD', 'Elemento non intersecante alcun tronco di adduzione'
	from acq_accumulo a 
	where a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	AND NOT EXISTS(
		select ids_codice
		from ACCUMULI_INADD t
		where t.ids_codice = a.codice_ato 
	);
	
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------------------------------------
-- Populate table DEPURATO_INCOLL
-- Example:
-- select DBIAIT_ANALYSIS.populate_depurato_incoll();
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_depurato_incoll(
) RETURNS BOOLEAN AS $$
DECLARE
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN
	
	DELETE FROM DEPURATO_INCOLL;
	
	INSERT into DEPURATO_INCOLL(ids_codice, ids_codice_collettore, id_gestore_collettore)
	SELECT DISTINCT t.codice_ato, ad.codice_ato, 3 from (
		SELECT distinct aa.codice_ato, ac.id_rete
		from fgn_trattamento aa, fgn_condotta ac
		WHERE aa.d_gestore = 'PUBLIACQUA' AND aa.d_ambito IN ('AT3', NULL) AND aa.d_stato IN ('ATT','FIP','PIF','RIS')
		and st_buffer(aa.geom, v_tol)&&ac.geom and st_intersects(ac.geom, st_buffer(aa.geom, v_tol))
	) t, fgn_collettore ad
	WHERE t.id_rete=ad.idgis
	AND ad.d_gestore = 'PUBLIACQUA' AND ad.d_ambito IN ('AT3', NULL) AND ad.d_stato IN ('ATT','FIP','PIF','RIS');
	
	--INSERT into DEPURATO_INCOLL(ids_codice, ids_codice_collettore, id_gestore_collettore)
	--select a.codice_ato, c.codice_ato, NULL 
	--from fgn_trattamento a left JOIN collett_tronchi c
	--on c.geom&&ST_BUFFER(a.geom, v_tol) and ST_INTERSECTS(c.geom, ST_BUFFER(a.geom, v_tol))
	--WHERE a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	--AND c.codice_ato is not NULL;
		
	--LOG ANOMALIE
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'DEPURATO_INCOLL';
	
	-- Elementi che intersecano piu' tronchi di collettore	
	INSERT INTO LOG_STANDALONE (id, alg_name, description) 
	select a.idgis, 'DEPURATO_INCOLL', 'Elemento intersecante piu'' (' || t.cnt || ') di un tronco di collettore'  
	FROM
	(
		select ids_codice, count(0) cnt
		from DEPURATO_INCOLL
		group by ids_codice having count(0) > 1
	) t, fgn_trattamento a
	WHERE a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	AND t.ids_codice = a.codice_ato;  

	-- Elementi che non intersecano alcun tronco di collettore
	INSERT INTO LOG_STANDALONE (id, alg_name, description)	
	select idgis, 'DEPURATO_INCOLL', 'Elemento non intersecante alcun tronco di collettore'
	from fgn_trattamento a 
	WHERE a.d_gestore = 'PUBLIACQUA' AND a.d_ambito IN ('AT3', NULL) AND a.d_stato IN ('ATT','FIP','PIF','RIS')
	AND not exists(
		select ids_codice
		from DEPURATO_INCOLL t
		where t.ids_codice = a.codice_ato 
	);
	
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
------------------------------------------------------------------------------------------------
-- Populate table SCARICATO_INFOG
--
-- Example:
-- select DBIAIT_ANALYSIS.populate_scaricato_infog();
--
-- TODO: vericare se campi e tabelle sono quelli attesi
-- Che valori mettere per il campo id_gestore_fognatura?
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_scaricato_infog(
) RETURNS BOOLEAN AS $$
BEGIN
	
	DELETE FROM SCARICATO_INFOG;
	
	INSERT into SCARICATO_INFOG(ids_codice, ids_codice_fognatura, id_gestore_fognatura)
	SELECT DISTINCT sf.codice_ato, rr.codice_ato, 3
	FROM fgn_sfioro sf
	LEFT JOIN fgn_rete_racc rr 
		ON rr.geom&&sf.geom AND st_INTERSECTS(rr.geom,sf.geom)
	WHERE sf.d_gestore = 'PUBLIACQUA' AND sf.d_ambito IN ('AT3', NULL) AND sf.d_stato IN ('ATT','FIP','PIF','RIS')
	AND rr.d_gestore = 'PUBLIACQUA' AND rr.d_ambito IN ('AT3', NULL) AND rr.d_stato IN ('ATT','FIP','PIF','RIS')
	AND rr.codice_ato is not NULL;
		
	--LOG ANOMALIE
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'SCARICATO_INFOG';
	
	-- Elementi che intersecano piu' di una rete
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT idgis, 'SCARICATO_INFOG', 'Elemento intersecante piu'' (' || count(0) || ') di una rete' 
	FROM (
		SELECT sf.idgis
		FROM fgn_sfioro sf
		LEFT JOIN fgn_rete_racc rr 
			ON rr.geom&&sf.geom AND st_INTERSECTS(rr.geom,sf.geom)
		WHERE sf.d_gestore = 'PUBLIACQUA' AND sf.d_ambito IN ('AT3', NULL) AND sf.d_stato IN ('ATT','FIP','PIF','RIS')
		AND rr.d_gestore = 'PUBLIACQUA' AND rr.d_ambito IN ('AT3', NULL) AND rr.d_stato IN ('ATT','FIP','PIF','RIS')
		AND rr.codice_ato is not NULL
	) t group by t.idgis having count(0) > 1;

	-- Elementi che non intersecano alcuna rete
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT sf.idgis, 'SCARICATO_INFOG', 'Elemento non intersecante alcuna rete' 
	FROM fgn_sfioro sf
	LEFT JOIN fgn_rete_racc rr 
		ON rr.geom&&sf.geom AND st_INTERSECTS(rr.geom,sf.geom)
	WHERE sf.d_gestore = 'PUBLIACQUA' AND sf.d_ambito IN ('AT3', NULL) AND sf.d_stato IN ('ATT','FIP','PIF','RIS')
		AND rr.d_gestore = 'PUBLIACQUA' AND rr.d_ambito IN ('AT3', NULL) AND rr.d_stato IN ('ATT','FIP','PIF','RIS')
		AND	rr.codice_ato is null;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-- Populate temporary tables (waiting for official graphs algorithms)
--   * FIUMI_INRETI, LAGHI_INRETI, POZZI_INRETI, SORGENTI_INRETI, POTAB_INRETI, ADDUT_INRETI, ACCUMULI_INRETI
--   * ADDUT_COM_SERV, COLLETT_COM_SERV
--   * ACCUMULI_INADD, DEPURATO_INCOLL
--   * SCARICAT_INFOG
-- Example:
-- 	SELECT DBIAIT_ANALYSIS.populate_temp_graph_tables()
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_temp_graph_tables(
) RETURNS BOOLEAN AS $$
BEGIN
	PERFORM populate_impianti_inreti();
	PERFORM populate_addut_com_serv();
	PERFORM populate_collet_com_serv();
	PERFORM populate_accumuli_inadd();
	PERFORM populate_depurato_incoll();
	PERFORM populate_scaricato_infog();
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-- Create the network node for a specified table
-- Example:
-- SELECT DBIAIT_ANALYSIS.create_network_nodes('acq_condotta')
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.create_network_nodes(
	v_table_name VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
	v_node_table VARCHAR;
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN
	v_node_table := v_table_name || '_nodes';
	EXECUTE 'DROP TABLE IF EXISTS dbiait_analysis.' || v_node_table;
	EXECUTE 'CREATE TABLE dbiait_analysis.' || v_node_table || '(id INTEGER)';
	PERFORM AddGeometryColumn ('dbiait_analysis', v_node_table, 'geom', 25832, 'POINT', 2); 
	
	EXECUTE '
		INSERT INTO ' || v_node_table || ' (geom, id)
		select geom, ROW_NUMBER () OVER () id from (

			select distinct on (ST_ASTEXT(geom)) geom
			from (
				
				select 
				   st_snaptogrid(st_startpoint(t.geom), $1) geom
				from (
				   select st_geometryn(geom, 1) geom
				   from ' || v_table_name || '
				   where st_numgeometries(geom)=1
				) t
				
				UNION ALL
				
				select 
				   st_snaptogrid(st_endpoint(t.geom), $2) geom
				from (
				   select st_geometryn(geom, 1) geom
				   from ' || v_table_name || '
				   where st_numgeometries(geom)=1
				) t
				
			) t2
			
		) t3
	' USING v_tol, v_tol;
	
	EXECUTE 'CREATE INDEX ' || v_node_table || '_geom_idx ON ' || v_node_table || ' USING GIST(geom)';
	
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;	
-------------------------------------------------------------------------------------------------------
-- Create the network edges for a specified table
-- Example:
-- SELECT DBIAIT_ANALYSIS.create_network_edges('acq_condotta')
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.create_network_edges(
	v_table_name VARCHAR
) RETURNS BOOLEAN AS $$
DECLARE
	v_edge_table VARCHAR;
	v_tol DOUBLE PRECISION;
BEGIN
	v_tol := snap_tolerance();
	v_edge_table := v_table_name || '_edges';
	EXECUTE 'DROP TABLE IF EXISTS dbiait_analysis.' || v_edge_table;
	EXECUTE 'CREATE TABLE dbiait_analysis.' || v_edge_table || '(id INTEGER, idgis VARCHAR(32), source INTEGER, target INTEGER)';
	PERFORM AddGeometryColumn ('dbiait_analysis', v_edge_table, 'geom', 25832, 'LINESTRING', 2); 
	
	EXECUTE '
		INSERT INTO ' || v_edge_table || ' (id, idgis, geom)
		SELECT ROW_NUMBER () OVER () id, idgis, st_geometryn(geom, 1) geom
		FROM ' || v_table_name || '
		WHERE st_numgeometries(geom) = 1
	';
	
	EXECUTE 'CREATE INDEX ' || v_edge_table || '_geom_idx ON ' || v_edge_table || ' USING GIST(geom)';
	
	--Update source field
	EXECUTE '
	update ' || v_edge_table || '
	set source = n.id
	from acq_condotta_nodes n
	where st_buffer(st_startpoint(' || v_edge_table || '.geom),$1)&&n.geom
	and st_intersects(n.geom,st_buffer(st_startpoint(' || v_edge_table || '.geom),$2))
	' USING v_tol, v_tol;
	--Update target field
	EXECUTE '
	update ' || v_edge_table || '
	set target = n.id
	from acq_condotta_nodes n
	where st_buffer(st_endpoint(' || v_edge_table || '.geom),$1)&&n.geom
	and st_intersects(n.geom,st_buffer(st_endpoint(' || v_edge_table || '.geom),$2))
	' USING v_tol, v_tol;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql 
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-- Create the networks for acq_condotta/fgn_condotta
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.create_networks()
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.create_networks(
) RETURNS BOOLEAN AS $$
DECLARE
	v_result BOOLEAN;
BEGIN
	-- Rete Idrica
	PERFORM create_network_nodes('acq_condotta');
	PERFORM create_network_edges('acq_condotta');
	-- Rete Fognaria
	PERFORM create_network_nodes('fgn_condotta');
	PERFORM create_network_edges('fgn_condotta');
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;

--------
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_stats_cloratore(
) RETURNS BOOLEAN AS $$
BEGIN
    -- truncate old table
	DELETE FROM stats_cloratore;

    -- run procedure
	INSERT INTO stats_cloratore (id_rete, counter)
	SELECT
		id_rete,
		count(*) as cc
	FROM
		acq_condotta ac,
		acq_cloratore ac2
	WHERE
		st_intersects(ac.geom,
		ac2.geom)
	GROUP BY
		id_rete;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;

-------------------------------------------------------------------------------------------------------
-- Create the schema acquedottistico for impianti/reti
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.populate_schema_acq()
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_schema_acq(
) RETURNS BOOLEAN AS $$
DECLARE
	v_tol DOUBLE PRECISION := snap_tolerance();
BEGIN

	DELETE FROM schema_acq;

	WITH
	    all_reti as(
            SELECT idgis, geom FROM acq_rete_distrib ard),
        all_impianti as(
            SELECT idgis, geom FROM acq_captazione fis
            UNION ALL
            SELECT idgis, geom FROM acq_accumulo acq
            UNION ALL
            SELECT idgis, geom FROM acq_potabiliz ap
            UNION ALL
            SELECT idgis, geom FROM acq_pompaggio acp)
    INSERT INTO schema_acq(idgis, codice_schema_acq, denominazione_schema_acq)
    SELECT
        ot.idgis idgis,
        string_agg(ot.codice_schema_acq, ';') codice_schema_acq,
        string_agg(ot.denominazione_schema_acq, ';') denominazione_schema_acq
    FROM (
        SELECT
            ap.codice_schema_acq,
            ap.denominazione_schema_acq,
            ar.idgis
        FROM
            area_poe ap
        JOIN (
            SELECT idgis, geom FROM all_reti
            UNION ALL
            SELECT idgis, geom FROM all_impianti
            ORDER BY idgis desc
         ) ar ON
         ap.geom && ar.geom
         AND ST_INTERSECTS(ST_BUFFER(ap.geom, -1*v_tol), ar.geom)
		 AND ST_TOUCHES(ap.geom, ar.geom) = FALSE
	) ot
    GROUP BY
        idgis;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;


-------------------------------------------------------------------------------------------------------
-- Calcola la tabella ubic_allaccio partendo da acq_allacci #221
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.populate_ubic_allaccio()
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_ubic_allaccio(
) RETURNS BOOLEAN AS $$
begin

	-- PULIZIA PRECENTI ELABORAZIONI E LOG STAND-ALONE
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'UBIC_ALLACCIO';
	DELETE FROM ubic_contatori_cass_cont;
	DELETE FROM ubic_allaccio;


	-- AGGIUNTA TUTTI GLI UBIC_CONTATORI DISPONIBILI SECONDO I FILTRI FORNITI
	-- UTILIZZO DI UNA TABELLA DI SISTEMA PER EVITARE DI RIFARE LA SELECT CON I FILTRI
	-- PER LE QUERY SUCCESSIVE, COSI' ACCELLERIAMO L'ESECUZIONE DEL PROCESSO
	insert into ubic_contatori_cass_cont
	select
		distinct id_ubic_contatore, id_cass_cont
	from
		acq_ubic_contatore as auc
	join acq_contatore ac on
		auc.idgis = ac.id_ubic_contatore
	where
		auc.id_impianto is not null
		AND COALESCE(ac.tariffa, '?') NOT IN ('APB_REFIND',
		'APBLREFIND',
		'APBNREFCIV',
		'APBHSUBDIS',
		'COPDCI0000',
		'COPDIN0000');

	-- INSERIMENTO DGLI ALLACCI NELLA TABELLA FINALE DI UBIC_ALLACCIO
	insert
		into
		ubic_allaccio(id_ubic_contatore,
		acq_sn_alla,
		acq_idrete)
	select
		id_ubic_contatore, null, null
	from
		ubic_contatori_cass_cont;

	-- UPDATE ID_RETE E ACQ_SN_ALLA PER GLI ID_UBIC_CONTATORE
	-- CHE MATCHANO LA TABELLA ACQ_ALLACCIO E TUTTI PARENT DEFALCO ATTIVI
	update ubic_allaccio
	set acq_sn_alla = xx.acq_sn_alla, acq_idrete=xx.id_rete
	from (
	with defalco_parent as (select
		distinct id_ubic_contatore,
		id_cass_cont
	from
		ubic_contatori_cass_cont
	join utenza_defalco on
		id_ubic_contatore = utenza_defalco.idgis_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY'))
	select
		c.id_ubic_contatore,
		case
			when aa.id_cassetta is not null then 'SI'
			else null
		end acq_sn_alla,
		case
			when aca.id_rete is not null then aca.id_rete
			else null
		end id_rete
	from
		(select id_ubic_contatore, id_cass_cont from ubic_contatori_cass_cont
		union all
		select * from defalco_parent) c
	join acq_allaccio aa on
		c.id_cass_cont = aa.id_cassetta
	join acq_cond_altro aca on
		aca.idgis = aa.id_condotta) xx where ubic_allaccio.id_ubic_contatore =xx.id_ubic_contatore;

	-- UPDATE ID_RETE E ACQ_SN_ALLA PER GLI ID_UBIC_CONTATORE
	-- PER TUTTI I DIVISIONALI (CHILD) DEFALCO ASSEGNANDO ID_RETE DEL PARENT
	update ubic_allaccio
	set acq_sn_alla = xx.acq_sn_alla, acq_idrete=xx.acq_idrete
	from (
        with defalco_parent as (select
            distinct id_ubic_contatore id_defalco
        from
            ubic_contatori_cass_cont
        join utenza_defalco on
            id_ubic_contatore = utenza_defalco.idgis_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY')),
        defalco_divisionali as (
        select
            distinct ud.idgis_divisionale,
            ud.idgis_defalco
        from utenza_defalco ud
        join defalco_parent dp on ud.idgis_defalco =dp.id_defalco)
        select dd.idgis_divisionale id_ubic_contatore,
        case
                when aa.acq_idrete is not null then 'SI'
                else null
            end acq_sn_alla,
            case
                when aa.acq_idrete is not null then aa.acq_idrete
            else null
            end  acq_idrete
        from defalco_divisionali dd
        inner join ubic_allaccio aa on aa.id_ubic_contatore = dd.idgis_defalco
    ) xx where ubic_allaccio.id_ubic_contatore =xx.id_ubic_contatore;

	-- AGGIUNTO NEL LOG STAND_ALONE TUTTI GLI ID_UBIC_CONTATORE CHE HANNO ID_RETE VUORO
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, 'UBIC_ALLACCIO', 'Contatore non allacciato alla rete acquedotto'
	FROM ubic_allaccio where acq_idrete is null;


	-- RECUPERO INFORMAZIONE ID_RETE DELLA RETE DI DISTRIBUZIONE PER GLI
	-- ID_UBIC_CONTATORE I QUALI NON HANNO UNA RELAZIONE DIRETTA CON I CONTATORI
	update
		ubic_allaccio
	set
		acq_sn_alla = 'NO',
		acq_idrete = yy.acq_idrete
	from
		(select
			distinct auc.idgis id_ubic_contatore,
			ard.idgis acq_idrete
		from
			acq_ubic_contatore auc
		join acq_rete_distrib ard on
			st_INTERSECTS(ard.geom,
			auc.geom)
		where
			auc.idgis in (
			select
				id_ubic_contatore
				from
					ubic_allaccio ua
				where
					acq_idrete is null
			)) yy
	where
		ubic_allaccio.id_ubic_contatore = yy.id_ubic_contatore;

	-- INSERIMENTO NEL LOG STAND-ALONE TUTTI GLI ID_UBIC_CONTATORE CHE HANNO
	-- ID_RETE NULLO
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, 'UBIC_ALLACCIO', 'Contatore non allacciato ad acquedotto e fuori rete distribuzione'
	FROM ubic_allaccio where acq_idrete is null;


	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;


CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_utenze_distribuzioni_adduttrici(
) RETURNS BOOLEAN AS $$
begin

	delete from utenze_distribuzioni_adduttrici;
    insert into utenze_distribuzioni_adduttrici
    with utenze_dirette as (
        SELECT
            id_ubic_contatore,
            tipo_uso
        FROM
            utenza_sap us
        where
            cattariffa not in ('APB_REFIND',
            'APBLREFIND',
            'APBNREFCIV',
            'APBHSUBDIS')),
        utenze_condominiali as (
        SELECT
            id_ubic_contatore,
            tipo_uso
        FROM
            utenza_sap us
        where
            cattariffa not in ('APB_CONDOM','APB_CONMIS')),
        utenze_indirette as (
        SELECT
            id_ubic_contatore,
            tipo_uso,
            u_ab
        FROM
            utenza_sap us
        where
            cattariffa not in ('APB_REFIND',
            'APBLREFIND',
            'APBNREFCIV',
            'APBHSUBDIS')),
        utenze_misuratore as (
        SELECT
            id_ubic_contatore,
            nr_contat
        FROM
            utenza_sap us
        where
            nr_contat > 1 and cattariffa not in ('APB_REFIND',
            'APBLREFIND',
            'APBNREFCIV',
            'APBHSUBDIS')),
        volume_utenze as (
        SELECT
            id_ubic_contatore,
            vol_acq_ero,
            vol_acq_fatt
        FROM
            utenza_sap us
        where
            cattariffa not in ('APB_REFIND',
            'APBLREFIND',
            'APBNREFCIV',
            'APBHSUBDIS')),
        n_allacci as (select id_rete, count(*) as nr_allacci
            FROM acq_allaccio aa
            join acq_cond_altro aca
            on aa.id_condotta =aca.idgis
            join acq_rete_distrib ard
            on ard.idgis=aca.id_rete
            WHERE ard.d_gestore = 'PUBLIACQUA' AND ard.d_ambito IN ('AT3', NULL) AND ard.d_stato NOT IN ('IPR','IAC') group by 1
        ),
        distrib_and_addr as (
            SELECT distinct ua.acq_idrete, idgis
            FROM (
                SELECT distinct idgis, d_gestore,d_ambito, d_stato FROM acq_rete_distrib
                UNION ALL
                SELECT distinct idgis, d_gestore,d_ambito, d_stato from acq_adduttrice
            ) ard
            JOIN ubic_allaccio ua ON
                ard.idgis = ua.acq_idrete
            WHERE
                ard.d_gestore = 'PUBLIACQUA'
                AND ard.d_ambito IN ('AT3',
                null)
                AND ard.d_stato NOT IN ('IPR',
                'IAC')
        )
	SELECT
		ua.acq_idrete,
		count(ud.id_ubic_contatore) as nr_utenze_dirette,
		sum(case when ud.tipo_uso in ('DOMESTICO', 'DOMESTICO RESIDENTE') then 1 else 0 end) nr_utenze_dir_dom_e_residente,
		sum(case when ud.tipo_uso = 'DOMESTICO RESIDENTE' then 1 else 0 end) nr_utenze_dir_residente,
		count(uc.id_ubic_contatore) as nr_utenze_condominiali,
		sum(uind.u_ab) as nr_utenze_indir_indirette,
		sum(case when uind.tipo_uso in ('DOMESTICO', 'DOMESTICO RESIDENTE') then u_ab else 0 end) nr_utenze_indir_domestici,
		sum(case when uind.tipo_uso = 'DOMESTICO RESIDENTE' then u_ab else 0 end) nr_utenze_indir_residente,
		count(um.nr_contat) as nr_utenze_misuratore,
		sum(vu.vol_acq_ero) volume_erogato,
		sum(vu.vol_acq_fatt) volume_fatturato,
		nal.nr_allacci
	FROM
		distrib_and_addr ard
	JOIN ubic_allaccio ua on
		ard.idgis = ua.acq_idrete
	LEFT JOIN utenze_dirette ud on
		ud.id_ubic_contatore = ua.id_ubic_contatore
	LEFT JOIN utenze_condominiali uc on
		uc.id_ubic_contatore = ua.id_ubic_contatore
	LEFT JOIN utenze_indirette uind on
		uind.id_ubic_contatore = ua.id_ubic_contatore
	LEFT JOIN utenze_misuratore um on
		um.id_ubic_contatore = ua.id_ubic_contatore
	LEFT JOIN volume_utenze vu on
		vu.id_ubic_contatore = ua.id_ubic_contatore
	LEFT JOIN n_allacci nal on
		nal.id_rete = ard.idgis
	GROUP BY
		ua.acq_idrete, nr_allacci;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;


-------------------------------------------------------------------------------------------------------
-- Calcola la tabella ubic_f_allaccio partendo da acq_allacci #221
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.populate_ubic_f_allaccio()
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_ubic_f_allaccio(
) RETURNS BOOLEAN AS $$
begin

    -- PULIZIA PRECENTI CALCOLAZIONI E LOG STAND-ALONE
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'UBIC_F_ALLACCIO';
	DELETE FROM ubic_contatori_fgn;
	DELETE FROM ubic_f_allaccio;


	-- AGGIUNTA TUTTI GLI UBIC_CONTATORI DISPONIBILI SECONDO I FILTRI FORNITI
	-- UTILIZZO DI UNA TABELLA DI SISTEMA PER EVITARE DI RIFARE LA SELECT CON I FILTRI
	-- PER LE QUERY SUCCESSIVE, COSI' ACCELLERIAMO L'ESECUZIONE DEL PROCESSO
	insert into ubic_contatori_fgn
	select
		distinct idgis, id_fossa
	from
		acq_ubic_contatore as auc
	LEFT join fgn_allaccio fa on
		auc.id_fossa_settica = fa.id_fossa
	where
		auc.id_impianto is not null;

	-- INSERIMENTO DGLI ALLACCI NELLA TABELLA FINALE DI ubic_f_allaccio
	insert
		into
		ubic_f_allaccio(id_ubic_contatore,
		fgn_sn_alla,
		fgn_idrete)
	select
		id_ubic_contatore, null, null
	from
		ubic_contatori_fgn;

	-- UPDATE ID_RETE E ACQ_SN_ALLA PER GLI ID_UBIC_CONTATORE
	-- CHE MATCHANO LA TABELLA fgn_allaccio E TUTTI PARENT DEFALCO ATTIVI
	update ubic_f_allaccio
	set fgn_sn_alla = xx.fgn_sn_alla, fgn_idrete=xx.id_rete
	from (
	with defalco_parent as (select
		distinct id_ubic_contatore,
		id_fossa
	from
		ubic_contatori_fgn
	join utenza_defalco on
		id_ubic_contatore = utenza_defalco.idgis_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY'))
	select
		c.id_ubic_contatore,
		case
			when aa.id_fossa is not null then 'SI'
			else null
		end fgn_sn_alla,
		case
			when aca.id_rete is not null then aca.id_rete
			else null
		end id_rete
	from
		(select id_ubic_contatore, id_fossa from ubic_contatori_fgn
		union all
		select id_ubic_contatore, id_fossa from defalco_parent) c
	join fgn_allaccio aa on
		c.id_fossa = aa.id_fossa
	join fgn_cond_altro aca on
		aca.idgis = aa.id_condotta) xx where ubic_f_allaccio.id_ubic_contatore =xx.id_ubic_contatore;

	-- UPDATE ID_RETE E ACQ_SN_ALLA PER GLI ID_UBIC_CONTATORE
	-- PER TUTTI I DIVISIONALI (CHILD) DEFALCO ASSEGNANDO ID_RETE DEL PARENT
	update ubic_f_allaccio
	set fgn_sn_alla = xx.fgn_sn_alla, fgn_idrete=xx.id_rete
	from (
	with defalco_parent as (select
		distinct id_ubic_contatore id_defalco
	from
		ubic_contatori_fgn
	join utenza_defalco on
		id_ubic_contatore = utenza_defalco.idgis_defalco where dt_fine_val=to_date('31-12-9999', 'DD-MM-YYYY')),
	defalco_divisionali as (
	select
		distinct ud.idgis_divisionale,
		ud.idgis_defalco
	from utenza_defalco ud
	join defalco_parent dp on ud.idgis_defalco =dp.id_defalco)
	select dd.idgis_divisionale id_ubic_contatore,
	case
			when aa.fgn_idrete is not null then 'SI'
			else null
		end fgn_sn_alla,
		case
			when aa.fgn_idrete is not null then aa.fgn_idrete
		else null
		end  id_rete
	from defalco_divisionali dd
	inner join ubic_f_allaccio aa on aa.id_ubic_contatore = dd.idgis_defalco) xx where ubic_f_allaccio.id_ubic_contatore =xx.id_ubic_contatore;

	-- AGGIUNTO NEL LOG STAND_ALONE TUTTI GLI ID_UBIC_CONTATORE CHE HANNO ID_RETE VUORO
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT ufa.id_ubic_contatore, 'UBIC_F_ALLACCIO', 'Contatore servito da Fognatura non allacciato'
	from ubic_f_allaccio ufa
	join utenza_sap us on ufa.id_ubic_contatore = us.id_ubic_contatore
	where fgn_idrete is null and esente_fog = 0;


	-- RECUPERO INFORMAZIONE ID_RETE DELLA RETE DI DISTRIBUZIONE PER GLI
	-- ID_UBIC_CONTATORE I QUALI NON HANNO UNA RELAZIONE DIRETTA CON I CONTATORI
	update
		ubic_f_allaccio
	set
		fgn_sn_alla = 'NO',
		fgn_idrete = yy.fgn_idrete
	from
		(select
			distinct auc.idgis id_ubic_contatore,
			ard.idgis fgn_idrete
		from
			acq_ubic_contatore auc
		join fgn_rete_all ard on
			st_INTERSECTS(ard.geom,
			auc.geom)
		where
			auc.idgis in (
			select
				id_ubic_contatore
				from
					ubic_f_allaccio ua
				where
					fgn_idrete is null
			)) yy
	where
		ubic_f_allaccio.id_ubic_contatore = yy.id_ubic_contatore;

	-- INSERIMENTO NEL LOG STAND-ALONE TUTTI GLI ID_UBIC_CONTATORE CHE HANNO
	-- ID_RETE NULLO
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, 'UBIC_F_ALLACCIO', 'Contatore servito da Fognatura non allacciato e fuori rete di raccolta'
	FROM ubic_f_allaccio where fgn_idrete is null;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;

-------------------------------------------------------------------------------------------------------
-- Calcola la tabella utenze_fognature_collettori per il volume delle utenze industriali #222
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.populate_utenze_fognature_collettori()
--
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_utenze_fognature_collettori(
) RETURNS BOOLEAN AS $$
begin

	DELETE FROM utenze_fognature_collettori;

    INSERT INTO utenze_fognature_collettori
	WITH utenze AS (
		SELECT
			DISTINCT ufa.fgn_idrete,
			esente_fog,
			cattariffa,
			vol_fgn_fatt,
			vol_fgn_ero
		FROM
			(
			SELECT
				DISTINCT idgis
			FROM
				fgn_rete_racc frr
			WHERE frr.d_gestore = 'PUBLIACQUA' AND frr.d_ambito IN ('AT3', NULL) AND frr.d_stato NOT IN ('IPR','IAC')
		union all
			SELECT
				DISTINCT idgis
			FROM
				fgn_collettore fc
			WHERE fc.d_gestore = 'PUBLIACQUA' AND fc.d_ambito IN ('AT3', NULL) AND fc.d_stato NOT IN ('IPR','IAC')
		) f
		JOIN ubic_f_allaccio ufa ON
			f.idgis = ufa.fgn_idrete
		JOIN utenza_sap us ON
			ufa.id_ubic_contatore = us.id_ubic_contatore
	)
	SELECT
		fgn_idrete,
		SUM(CASE WHEN ut.esente_fog = 0 THEN 1 ELSE 0 END) nr_utenze_totali,
		SUM(CASE WHEN ut.cattariffa IN ('APB_REFIND', 'APBLREFIND') THEN 1 ELSE 0 END) utenze_industriali,
		SUM(CASE WHEN ut.cattariffa IN ('APB_REFIND', 'APBLREFIND') THEN vol_fgn_fatt ELSE 0 END) volume_utenze_industriali,
		SUM(CASE WHEN ut.esente_fog = 0 THEN vol_fgn_fatt ELSE 0 END) volume_utenze_totali
	FROM
		utenze ut
	GROUP BY
		fgn_idrete;

	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;

-------------------------------------------------------------------------------------------------------
-- Associa i codici di accorpamento per idgis sulle captazioni #222
--
-- Example:
-- SELECT DBIAIT_ANALYSIS.populate_codice_capt_accorp()

CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.populate_codice_capt_accorp(
) RETURNS BOOLEAN AS $$
begin
    DELETE FROM support_codice_capt_accorp;
    INSERT INTO support_codice_capt_accorp
    SELECT ac.idgis,codice_accorp_capt codice, acc2.denom FROM acq_capt_conces acc
    JOIN acq_captazione ac on ac.idgis=acc.id_captazione
    join ACQ_CAPT_ACCORPAM acc2 on codice_accorp_capt=codice_acc;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------
-- Esegue il reset (DELETE o UPDATE) della tabelle/colonne che sono aggiornate
-- dalle procedure standalone
-- SELECT DBIAIT_ANALYSIS.reset_proc_stda_tables()
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.reset_proc_stda_tables(
) RETURNS BOOLEAN AS $$
begin
	DELETE FROM LOG_STANDALONE;
    DELETE FROM POP_RES_LOC;
	DELETE FROM DISTRIB_LOC_SERV;
	
	UPDATE POP_RES_COMUNE
	SET 
		pop_ser_acq = NULL, 
		perc_acq = NULL,	
		perc_fgn = NULL,	
		pop_ser_fgn = NULL, 
		perc_dep = NULL,		
		pop_ser_dep = NULL;																									
	
	DELETE FROM DISTRIB_COM_SERV;
	DELETE FROM UTENZA_SERVIZIO;
	DELETE FROM UTENZA_SERVIZIO_LOC;
	DELETE FROM UTENZA_SERVIZIO_ACQ;
	DELETE FROM UTENZA_SERVIZIO_FGN;
	DELETE FROM UTENZA_SERVIZIO_BAC;
	DELETE FROM ABITANTI_TRATTATI;
	DELETE FROM DISTRIB_TRONCHI;
	DELETE FROM ADDUT_TRONCHI;
	DELETE FROM ACQ_COND_ALTRO;
	DELETE FROM ACQ_LUNGHEZZA_RETE;
	DELETE FROM FOGNAT_TRONCHI;
	DELETE FROM COLLETT_TRONCHI;
	DELETE FROM FGN_COND_ALTRO;
	DELETE FROM FGN_LUNGHEZZA_RETE;
	DELETE FROM ACQ_ALLACCIO;
	DELETE FROM ACQ_LUNGHEZZA_ALLACCI;
	DELETE FROM SUPPORT_ACQ_ALLACCI;
	DELETE FROM FGN_ALLACCIO;
	DELETE FROM FGN_LUNGHEZZA_ALLACCI;
	DELETE FROM FGN_LUNGHEZZA_ALLACCI_id_rete;
	DELETE FROM SUPPORT_FGN_ALLACCI;
	DELETE FROM ACQ_SHAPE;
	DELETE FROM ACQ_VOL_UTENZE;
	DELETE FROM FGN_SHAPE;
	DELETE FROM FGN_VOL_UTENZE;
	DELETE FROM STATS_POMPE;
	DELETE FROM POZZI_POMPE;
	DELETE FROM POTAB_POMPE;
	DELETE FROM POMPAGGI_POMPE;
	DELETE FROM SOLLEV_POMPE;
	DELETE FROM DEPURATO_POMPE;
	DELETE FROM ADDUT_COM_SERV;
	DELETE FROM COLLET_COM_SERV;
	DELETE FROM FIUMI_INRETI;
	DELETE FROM LAGHI_INRETI;
	DELETE FROM POZZI_INRETI;
	DELETE FROM SORGENTI_INRETI;
	DELETE FROM POTAB_INRETI;
	DELETE FROM ADDUT_INRETI;
	DELETE FROM ACCUMULI_INRETI;
	DELETE FROM ACCUMULI_INADD;
	DELETE FROM DEPURATO_INCOLL;
	DELETE FROM SCARICATO_INFOG;
	DROP TABLE IF EXISTS ACQ_CONDOTTA_NODES;
	DROP TABLE IF EXISTS ACQ_CONDOTTA_EDGES;
	DROP TABLE IF EXISTS FGN_CONDOTTA_NODES;
	DROP TABLE IF EXISTS FGN_CONDOTTA_EDGES;
	DELETE FROM STATS_CLORATORE;
	DELETE FROM SCHEMA_ACQ;
	DELETE FROM UBIC_ALLACCIO;
	DELETE FROM UBIC_CONTATORI_CASS_CONT;
	DELETE FROM UTENZE_DISTRIBUZIONI_ADDUTTRICI;
	DELETE FROM UBIC_CONTATORI_FGN;
	DELETE FROM UBIC_F_ALLACCIO;
	DELETE FROM UTENZE_FOGNATURE_COLLETTORI;
	DELETE FROM SUPPORT_CODICE_CAPT_ACCORP;
	RETURN TRUE;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-- Esegue la lista di tutte le procedure STANDALONE nell'ordine corretto
-- SELECT DBIAIT_ANALYSIS.run_all_procs();
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.run_all_procs(
	v_reset_before BOOLEAN DEFAULT TRUE
) RETURNS BOOLEAN AS $$
DECLARE
	v_result BOOLEAN := TRUE;
begin
	IF v_reset_before THEN
		v_result := reset_proc_stda_tables();
	END IF;
	
	IF v_result THEN
		v_result:= create_networks()
			and populate_temp_graph_tables()
			and populate_pop_res_loc()
			and populate_distrib_loc_serv()
			and populate_pop_res_comune()
			and populate_distr_com_serv()
			and populate_utenza_servizio()
			and populate_abitanti_trattati()
			and populate_archivi_pompe()
			and populate_acquedotto()
			and populate_fognatura();
	END IF;
	
	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public, DBIAIT_ANALYSIS;
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------