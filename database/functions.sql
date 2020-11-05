--------------------------------------------------------------------
-- Snap tolerance for the system to use in spatial queries
-- Example:
--  select dbiait_analysis.snap_tolerance()
CREATE OR REPLACE FUNCTION DBIAIT_ANALYSIS.snap_tolerance(
) RETURNS DOUBLE PRECISION AS $$
BEGIN
    RETURN 0.01;
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;
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
	
	DELETE FROM LOG_STANDALONE WHERE alg_name = 'UTENZA_SERVIZIO';
	
	
	--LOCALITA
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_LOC(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, l.loc2011 as codice
		FROM acq_ubic_contatore c, utenza_sap s, localita l
		WHERE c.id_impianto is not null AND l.geom && c.geom AND ST_INTERSECTS(l.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';
		
	-- ACQ_RETE_DISTRIB
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_ACQ(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, g.codice_ato as codice
		FROM acq_ubic_contatore c, utenza_sap s, acq_rete_distrib g
		WHERE c.id_impianto is not null AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';

	-- FGN_RETE_RACC
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_FGN(impianto, id_ubic_contatore, codice)
		SELECT s.impianto, s.id_ubic_contatore, g.codice_ato as codice
		FROM acq_ubic_contatore c, utenza_sap s, fgn_rete_racc g
		WHERE c.id_impianto is not null AND g.D_GESTORE=''PUBLIACQUA'' AND g.D_STATO=''ATT'' AND g.D_AMBITO=''AT3''
		AND g.geom && c.geom AND ST_INTERSECTS(g.geom, c.geom)
		AND s.id_ubic_contatore=c.idgis';

	-- FGN_BACINO + FGN_TRATTAMENTO/FGN_PNT_SCARICO
	EXECUTE '
		INSERT INTO UTENZA_SERVIZIO_BAC(impianto, id_ubic_contatore, codice)
		select s.impianto, s.id_ubic_contatore, g.codice_ato as codice
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
		select s.impianto, s.id_ubic_contatore, g.codice_ato as codice
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


	-- Log duplicated items
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in acquedotto''
	FROM UTENZA_SERVIZIO_ACQ
	GROUP BY id_ubic_contatore
	HAVING COUNT(id_ubic_contatore) > 1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in localita''
	FROM UTENZA_SERVIZIO_LOC
	GROUP BY id_ubic_contatore
	HAVING COUNT(id_ubic_contatore) > 1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in fognatura''
	FROM UTENZA_SERVIZIO_FGN
	GROUP BY id_ubic_contatore
	HAVING COUNT(id_ubic_contatore) > 1';
	
	EXECUTE '
	INSERT INTO LOG_STANDALONE (id, alg_name, description)
	SELECT id_ubic_contatore, ''UTENZA_SERVIZIO'', ''Duplicati: '' || count(0) || '' in bacino''
	FROM UTENZA_SERVIZIO_BAC
	GROUP BY id_ubic_contatore
	HAVING COUNT(id_ubic_contatore) > 1';
	
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
BEGIN

	IF v_table = 'DISTRIB_TRONCHI' THEN
		v_sub_funzione := 4;
		v_field := 'pressione';
		v_join_table := 'ACQ_RETE_DISTRIB';
		v_column := '0::BIT';
	ELSIF v_table = 'ADDUT_TRONCHI' THEN
		v_sub_funzione := 1;
		v_field := 'protezione_catodica';
		v_join_table := 'ACQ_ADDUTTRICE';
		v_column := '
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
	IF v_sub_funzione = 4 THEN
		EXECUTE '
			UPDATE ' || v_table || '
			SET ' || v_field || ' = 1::BIT WHERE EXISTS(
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
	END IF;
	
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
	v_col_refluo VARCHAR(32):='';
	v_exp_refluo VARCHAR(64):='';
BEGIN

	IF v_table = 'FOGNAT_TRONCHI' THEN
		v_sub_funzione := 1;
		v_join_table := 'FGN_RETE_RACC';
		v_col_refluo := ',id_refluo_trasportato';
		v_exp_refluo := 'a.d_tipo_acqua as id_refluo_trasportato,';
	ELSIF v_table = 'COLLETT_TRONCHI' THEN
		v_sub_funzione := 2;
		v_join_table := 'FGN_COLLETTORE';
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
			' || v_col_refluo || '
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
			a.d_tipo_rilievo as idx_lunghezza,
			' || v_exp_refluo || '
			0::BIT,
			0::BIT
		FROM 
			FGN_CONDOTTA a,  
			' || v_join_table || ' r
		WHERE 
			(a.D_AMBITO = ''AT3'' OR a.D_AMBITO IS null) 
			AND (a.D_STATO = ''ATT'' OR a.D_STATO = ''FIP'' OR a.D_STATO IS NULL) 
			AND (a.SN_FITTIZIA = ''NO'' OR a.SN_FITTIZIA IS null) 
			AND (a.D_TIPO_ACQUA in (''MIS'',''NER'',''SCA'') or a.D_TIPO_ACQUA IS NULL)
			AND (a.D_GESTORE = ''PUBLIACQUA'') AND a.SUB_FUNZIONE = ' || v_sub_funzione || '
			AND a.id_rete=r.idgis;
		';

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
	
	IF v_table = 'FOGNAT_TRONCHI' THEN	
		-- valorizzazione id_refluo_trasportato
		EXECUTE '
			UPDATE ' || v_table || '
			SET id_refluo_trasportato = d.valore_netsic
			FROM ALL_DOMAINS d
			WHERE d.valore_gis = COALESCE(' || v_table || '.id_refluo_trasportato,''MIS'') AND d.dominio_netsic = ''id_refluo_trasportato'';
		';
	END IF;
	
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
					fgn_bacino b, ' || v_join_table || ' r
				where b.sub_funzione = 3
				and c.id_rete = r.idgis
				and c.geom && b.geom 
				and st_intersects(c.geom, b.geom)
				and r.d_stato in (''ATT'',''FIP'')
				and (r.d_ambito is null or r.d_ambito = ''AT3'')
				and r.d_gestore = ''PUBLIACQUA''
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
	
	-- DISTRIBUZIONE
	INSERT INTO FGN_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_dep
	)
	select 
		idgis_rete, codice_ato, 'DISTRIBUZIONE', sum(lunghezza) lung, 
		sum(
			case when recapito IS NOT NULL 
				or t.idgis_bac is not NULL
			then lunghezza 
			else 0 end
		) lung_tlc 
	from (
		select 
			ft.idgis, ft.recapito, bc.idgis as idgis_bac, idgis_rete, codice_ato, lunghezza, 	  
			case when 
				recapito IS NOT NULL or bc.idgis is not NULL
				then lunghezza 
			else 0 end lung_tlc 
		FROM
		  FOGNAT_TRONCHI as ft
		LEFT OUTER JOIN
		  FGN_BACINO as bc ON (ft.geom&&bc.geom and ST_INTERSECTS(ft.geom, bc.geom) and bc.sub_funzione=3)
	) t 
	GROUP BY t.codice_ato, t.idgis_rete;
	
	-- ADDUZIONE
	INSERT INTO FGN_LUNGHEZZA_RETE(
		idgis,
		codice_ato,
		tipo_infr,
		lunghezza,
		lunghezza_dep
	)
	select 
		idgis_rete, codice_ato, 'ADDUZIONE', sum(lunghezza) lung, 
		sum(
			case when recapito IS NOT NULL 
				or t.idgis_bac is not NULL
			then lunghezza 
			else 0 end
		) lung_tlc 
	from (
		select 
			ft.idgis, ft.recapito, bc.idgis as idgis_bac, idgis_rete, codice_ato, lunghezza, 	  
			case when 
				recapito IS NOT NULL or bc.idgis is not NULL
				then lunghezza 
			else 0 end lung_tlc 
		FROM
		  COLLETT_TRONCHI as ft
		LEFT OUTER JOIN
		  FGN_BACINO as bc ON (ft.geom&&bc.geom and ST_INTERSECTS(ft.geom, bc.geom) and bc.sub_funzione=3)
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
	DELETE FROM ACQ_COND_ALTRO;
	DELETE FROM ACQ_ALLACCIO;

	INSERT INTO ACQ_COND_ALTRO(
		idgis, id_rete, codice_ato, tipo_infr, 
		nr_allacci_sim, lu_allacci_sim,
		nr_allacci_ril, lu_allacci_ril
	)
	SELECT idgis, id_rete, NULL, 
			CASE 
				WHEN sub_funzione = 4 THEN 'DISTRIBUZIONI'
				WHEN sub_funzione = 1 THEN 'ADDUZIONI'
				ELSE '?'
			END tipo_infr, 
		sum(nr_allacci), sum(lung_alla), 
		sum(nr_allacci_ril), sum(lung_alla_ril)  
	FROM (
		-- 1) Realmente mappati
		SELECT ac.idgis, ac.id_rete, ac.sub_funzione,
			0 nr_allacci, 0 lung_alla, z.cnt nr_allacci_ril, z.leng lung_alla_ril 
		FROM acq_condotta ac, 
		(
			SELECT id_condotta, count(0) cnt, sum(leng) leng 
			FROM (
				SELECT d.id_condotta, st_length(c.geom) leng 
				FROM acq_derivazione d, acq_condotta c,
				(
					select distinct on(cc.idgis) cc.id_derivazione 
					from acq_cass_cont cc, acq_ubic_contatore uc
					where uc.ID_IMPIANTO is not null and uc.sorgente IS null
					and uc.id_cass_cont = cc.idgis 
				) cc
				WHERE 
					d.idgis = cc.id_derivazione 
					and c.sub_funzione = 3
					and c.geom&&st_buffer(d.geom, v_tol)
					and st_intersects(c.geom, st_buffer(d.geom, v_tol))
			) t GROUP BY t.id_condotta
		) z
		WHERE ac.idgis = z.id_condotta
		AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null) 
		AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL) 
		AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null) 
		AND (ac.D_GESTORE = 'PUBLIACQUA') 
		AND ac.SUB_FUNZIONE in (1, 4)
		UNION ALL	
		-- 2) SIMULAZIONE ALLACCIO
		SELECT ac.idgis, ac.id_rete, ac.sub_funzione,
			z.cnt nr_allacci, z.leng lung_alla, 0 nr_allacci_ril, 0 lung_alla_ril
		FROM acq_condotta ac,
		(
			SELECT d.id_condotta, count(0) cnt, sum(CASE WHEN st_length(l.geom)>50 THEN 50 ELSE st_length(l.geom) END) leng 
			FROM acq_deriv_auto d, acq_link_deriv l,
			(
					select distinct on(cc.idgis) cc.idgis, cc.id_derivazione 
					from acq_cass_cont_auto cc, acq_ubic_contatore uc
					where uc.ID_IMPIANTO is not null and uc.sorgente IS null
					and uc.id_cass_cont = cc.idgis 
			) cc
			WHERE 
				d.idgis=cc.id_derivazione
				and l.id_derivazione = d.idgis 
				and l.id_cass_cont = cc.idgis
			group by d.id_condotta
		) z
		WHERE ac.idgis = z.id_condotta		
		AND (ac.D_AMBITO = 'AT3' OR ac.D_AMBITO IS null) 
		AND (ac.D_STATO = 'ATT' OR ac.D_STATO = 'FIP' OR ac.D_STATO IS NULL) 
		AND (ac.SN_FITTIZIA = 'NO' OR ac.SN_FITTIZIA IS null) 
		AND (ac.D_GESTORE = 'PUBLIACQUA') 
		AND ac.SUB_FUNZIONE in (1, 4)
	) x GROUP BY idgis, id_rete, sub_funzione;
	
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
	
	--GROUP BY x ACQ_ALLACCIO
	INSERT INTO ACQ_ALLACCIO(idgis, codice_ato, tipo_infr, nr_allacci, lung_alla, nr_allacci_ril, lung_alla_ril)
	SELECT id_rete, codice_ato, tipo_infr, sum(nr_allacci_ril), sum(lu_allacci_ril)/1000, sum(nr_allacci_sim), sum(lu_allacci_sim)/1000 
	FROM ACQ_COND_ALTRO 
	WHERE id_rete is NOT NULL
	GROUP BY id_rete, codice_ato, tipo_infr;

	
	-- --ANOMALIES (derivazioni che non intersecano le condotte indicate)
	-- INSERT INTO LOG_STANDALONE (id, alg_name, description)
	-- SELECT idgis, 'ACQUEDOTTO', 'Derivazione che non interseca la condotta di rete indicata nella derivazione (per problemi di snap)' 
	-- FROM acq_derivazione ad 
	-- where EXISTS(
	-- 	SELECT c.idgis 
	-- 	from acq_condotta c
	-- 	WHERE (c.D_AMBITO = 'AT3' OR c.D_AMBITO IS null) 
	-- 	AND (c.D_STATO = 'ATT' OR c.D_STATO = 'FIP' OR c.D_STATO IS NULL) 
	-- 	AND (c.SN_FITTIZIA = 'NO' OR c.SN_FITTIZIA IS null) 
	-- 	AND (c.D_GESTORE = 'PUBLIACQUA') 
	-- 	AND c.SUB_FUNZIONE in (1, 4)				
	-- 	AND c.idgis=ad.id_condotta
	-- )
	-- AND NOT EXISTS(
	-- 	SELECT d.idgis 
	-- 	FROM acq_derivazione d, acq_condotta c
	-- 	WHERE d.id_condotta = c.idgis
	-- 	
	-- 	AND (c.D_AMBITO = 'AT3' OR c.D_AMBITO IS null) 
	-- 	AND (c.D_STATO = 'ATT' OR c.D_STATO = 'FIP' OR c.D_STATO IS NULL) 
	-- 	AND (c.SN_FITTIZIA = 'NO' OR c.SN_FITTIZIA IS null) 
	-- 	AND (c.D_GESTORE = 'PUBLIACQUA') 
	-- 	AND c.SUB_FUNZIONE in (1, 4)
	-- 			
	-- 	AND c.geom&&ST_BUFFER(d.geom, v_tol)
	-- 	AND st_intersects(c.geom,ST_BUFFER(d.geom, v_tol))
	-- 	AND d.idgis=ad.idgis
	-- );
	-- 
	-- INSERT INTO LOG_STANDALONE (id, alg_name, description)
	-- SELECT idgis, 'ACQUEDOTTO', 'Derivazione che interseca la condotta di rete ma non interseca alcuna condotta di allacciamento' 
	-- FROM acq_derivazione ad 
	-- WHERE NOT EXISTS(
	-- 	SELECT d.idgis 
	-- 	FROM acq_derivazione d, acq_condotta c
	-- 	WHERE d.id_condotta = c.idgis
	-- 	AND c.sub_funzione = 3
	-- 	AND c.geom&&ST_BUFFER(d.geom, v_tol)
	-- 	AND st_intersects(c.geom,ST_BUFFER(d.geom, v_tol))
	-- 	AND d.idgis = ad.idgis
	-- );

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
	v_tables VARCHAR[] := ARRAY['POZZI_POMPE', 'POTAB_POMPE', 'POMPAGGI_POMPE', 'SOLLEV_POMPE', 'DEPURATO_POMPE'];
	v_fields VARCHAR[] := ARRAY['IDX_ANNO_INSTAL', 'IDX_ANNO_RISTR', 'IDX_POTENZA', 'IDX_PORTATA', 'IDX_PREVALENZA'];
BEGIN

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
				CODICE_ATO, D_STATO_CONS, ANNO_INSTAL, 
				ANNO_RISTR, POTENZA, PORTATA, 
				PREVALENZA, 
				CASE WHEN 
					SN_RISERVA = ''NO'' THEN 0::BIT
				ELSE 1::BIT END,
				A_ANNO_INSTAL, A_ANNO_RISTR,
				A_POTENZA, A_PORTATA, A_PREVALENZA				
			FROM ARCHIVIO_POMPE p, ALL_DOMAINS d
			WHERE TIPO_OGGETTO = ''ACQ_CAPTAZIONE'';'; -- TODO: add other condition (?)

			FOR v_f IN array_lower(v_fields,1) .. array_upper(v_fields,1)
			LOOP
				EXECUTE '
					UPDATE ' || v_tables[v_t] || '
					SET ' || v_fields[v_f] || ' = d.valore_netsic
					FROM all_domains d
					WHERE d.dominio_netsic = ''id_indice_idx'' 
					AND d.valore_gis = ' || v_tables[v_t] || '.' || v_fields[v_f] || ';';
			END LOOP;		
		
	END LOOP;	
	
	RETURN TRUE;

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'dbiait_analysis'
    SET search_path = public, DBIAIT_ANALYSIS;		