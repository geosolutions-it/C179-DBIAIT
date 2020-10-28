--------------------------------------------------------------------
-- Etract pro-com part from the localita ISTAT (removing last 5 characters)
-- Example:
--  select dbiait_analysis.locistat_2_procom('3701520011')
--  37015
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.locistat_2_procom(
	v_locistat VARCHAR
) RETURNS VARCHAR AS $$
DECLARE 
	v_result VARCHAR := NULL;
BEGIN
	v_result := substr(v_locistat, 1, length(v_locistat) - 5);
    RETURN v_result;
EXCEPTION WHEN OTHERS THEN
	RETURN v_result;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;	
------------------------------------------------------------------
-- Transform a Geometry from EPSG:25832 to EPSG:3003 using NTV2 nadgrids
--SELECT ST_X(geom), ST_Y(geom) FROM(
--	SELECT DBIAIT_ANALYSIS.ST_TRANSFORM_RM40_ETRS89(
--		ST_SETSRID(ST_POINT(705438.9186,4830672.536), 25832)
--	) geom
--) t
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.ST_TRANSFORM_RM40_ETRS89(
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

	INSERT INTO POP_RES_LOC(anno_rif, pro_com, id_localita_istat, popres)
	SELECT 
		p.anno as anno_rif, 
		l.pro_com,
		loc2011 as id_localita_istat, 
		--loc.popres as popres_before, 
		ROUND(loc.popres*(p.pop_res/l.popres)) popres 
	FROM LOCALITA loc,
	(
		SELECT anno, pro_com, pop_res 
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
			WHERE p.pro_com=l.pro_com
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
		sum(perc)
	FROM (
		SELECT codice_ato, loc2011 as id_localita_istat, popres, 100*ST_AREA(ST_INTERSECTION(r.geom,l.geom))/ST_AREA(l.geom) perc 
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
	SET pop_ser_acq = NULL, perc_acq = NULL;

	-- updating field pop_ser_acq
	UPDATE POP_RES_COMUNE
	SET pop_ser_acq = t2.ab_srv_com, perc_acq = 100*t2.ab_srv_com/POP_RES_COMUNE.pop_res
	FROM (
		SELECT t.pro_com, sum(t.ab_srv_loc) as ab_srv_com 
		FROM(
			SELECT 
				locistat_2_procom(loc_serv.id_localita_istat) as pro_com, 
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
	
	--LOCALITA
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_LOC(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, l.loc2011 as id_localita_istat
		FROM acq_ubic_contatore c, utenza_sap s, localita l
		WHERE c.id_impianto is not null AND l.geom && c.geom AND ST_INTERSECTS(l.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';
		
	-- ACQ_RETE_DISTRIB
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_ACQ(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, g.codice_ato as id_localita_istat
		FROM acq_ubic_contatore c, utenza_sap s, acq_rete_distrib g
		WHERE c.id_impianto is not null AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';

	-- FGN_RETE_RACC
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_FGN(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, g.codice_ato as id_localita_istat
		FROM acq_ubic_contatore c, utenza_sap s, fgn_rete_racc g
		WHERE c.id_impianto is not null AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';

	-- FGN_BACINO + FGN_TRATTAMENTO/FGN_PNT_SCARICO
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_BAC(impianto, id_ubic_contatore, codice)
		select s.impianto, s.id_ubic_contatore, g.codice_ato as id_localita_istat
		from acq_ubic_contatore c, utenza_sap s, (
			select t.codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
			from FGN_BACINO b, FGN_TRATTAMENTO t
			WHERE b.SUB_FUNZIONE = 3 AND b.idgis = t.id_bacino
			AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
		) g
		WHERE c.id_impianto is not null 
		AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_BAC(impianto, id_ubic_contatore, codice)
		select s.impianto, s.id_ubic_contatore, g.codice_ato as id_localita_istat
		from acq_ubic_contatore c, utenza_sap s, (
			select t.codice as codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO 
			from FGN_BACINO b, FGN_PNT_SCARICO t
			WHERE b.SUB_FUNZIONE = 1 AND b.idgis = t.id_bacino
			AND t.D_GESTORE=''PUBLIACQUA'' AND t.D_STATO=''ATT'' AND t.D_AMBITO=''AT3''
		) g
		WHERE c.id_impianto is not null AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';


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
