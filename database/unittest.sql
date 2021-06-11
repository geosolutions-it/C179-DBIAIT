---------------------------------------------------------------------------------------------
-- It is necessary to have a dedicated database for stub (dbiait_stub)
-- In this database:
-- CREATE SCHEMA pgunit;
-- CREATE EXTENSION DBLINK SCHEMA pgunit;
-- # GRANT EXECUTE ON FUNCTION dblink_connect_u(text) TO dbiait_stub;
-- # GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO dbiait_stub;
-- run PGUnit.sql (from https://github.com/adrianandrei-ca/pgunit)
---------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------
-- RUN ALL TESTS:
-- SELECT * FROM pgunit.test_run_all();
--
-- RUN A SPECIFIC SUITE
-- SELECT * FROM pgunit.test_run_suite('sqlexport');
-- ==========================================================================================
CREATE OR REPLACE function dbiait_analysis.test_case_denom_acq_shape(
) returns void as $$
DECLARE
  dummy_string varchar;
  dummy_numeric NUMERIC;
begin
    
	-- Barberino Tavarnelle	48054
	SELECT distinct comune_nom INTO dummy_string
	FROM dbiait_analysis.acq_shape where id_comune_  = 48054; 
	PERFORM test_assertTrue('Check comune_nom: expected Barberino Tavarnelle, got ' || dummy_string, dummy_string = 'Barberino Tavarnelle');
	
	-- San Marcello Piteglio	47024
	SELECT distinct comune_nom INTO dummy_string
	FROM dbiait_analysis.acq_shape where id_comune_  = 47024; 
	PERFORM test_assertTrue('Check comune_nom: expected San Marcello Piteglio, got ' || dummy_string, dummy_string = 'San Marcello Piteglio');
	
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;


-- ------------------------------------------------------------------------------------------
-- TEST log duplicati UTENZA_SERVIZIO_BAC
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_log_standalone_UTENZA_SERVIZIO_BAC(
) returns void as $$
DECLARE
    v_count INTEGER;
begin
    SELECT count(0) INTO v_count
    from dbiait_analysis.acq_ubic_contatore uc, (
        select t.codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
        from dbiait_analysis.FGN_BACINO b, dbiait_analysis.FGN_TRATTAMENTO t
        WHERE b.SUB_FUNZIONE = 3 AND b.idgis = t.id_bacino
        AND ((t.D_STATO='ATT' AND t.D_AMBITO='AT3' AND t.D_GESTORE in ('PUBLIACQUA','GIDA') ) OR t.CODICE_ATO in ('DE00213','DE00214'))
        UNION ALL
        select t.codice as codice_ato, b.geom, t.D_GESTORE, t.D_STATO, t.D_AMBITO
        from dbiait_analysis.FGN_BACINO b, dbiait_analysis.FGN_PNT_SCARICO t
        WHERE b.SUB_FUNZIONE = 1 AND b.idgis = t.id_bacino
        AND ((t.D_STATO='ATT' AND t.D_AMBITO='AT3' AND t.D_GESTORE in ('PUBLIACQUA','GIDA') ) OR t.CODICE in ('DE00213','DE00214'))
    ) g WHERE g.geom && uc.geom AND ST_INTERSECTS(g.geom, uc.geom)
        AND uc.id_impianto in (
        '4001762758', '4001760688', '4002068228'
    );

    PERFORM test_assertTrue('Check duplicati UTENZA_SERVIZIO_BAC: expected 6, got ' || v_count, v_count = 6);

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- TEST POPULATE LUNG RETE FGN
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_lung_rete_fgn(
    v_run_proc BOOLEAN DEFAULT FALSE
) returns void as $$
DECLARE
  dummy_int bigint;
  dummy_string varchar;
  dummy_decimal decimal;
  dummy_decimal2 decimal;
  dummy_decimal_expected decimal;
  dummy_decimal_actual decimal;
begin
    -- run the new version of the procedure
    IF v_run_proc THEN
    	perform test_assertTrue('Verifica esito procedura', dbiait_analysis.populate_lung_rete_fgn() );
    END IF;
	-- ASSERTION TESTS START FROM HERE

    --- given a selected idgis, the value should be the expected
    SELECT lung_rete_mista, lung_rete_nera into dummy_decimal, dummy_decimal2 FROM dbiait_analysis.fgn_lunghezza_rete flr WHERE idgis ='PAFRRC00000000001145';
	PERFORM test_assertTrue('Check rete mista sia 0.044926231', dummy_decimal BETWEEN 0.044926 and 0.044927 );
    PERFORM test_assertTrue('Check rete nera sia 0.005073769', dummy_decimal2 BETWEEN 0.005073 and 0.005074 );

    --- given a selected idgis, the value should be the expected
    SELECT lung_rete_mista, lung_rete_nera into dummy_decimal, dummy_decimal2 FROM dbiait_analysis.fgn_lunghezza_rete flr WHERE idgis ='PAFRRC00000000001142';
	PERFORM test_assertTrue('Check rete mista sia 0.1', dummy_decimal = 0.1 );
    PERFORM test_assertTrue('Check rete nera sia 0.0', dummy_decimal2 = 0.0 );

   --- for FOGNATURA the field 'lung_dep' should be the same as the sum of 'lunghezza' in fognat_tronchi with depurazione =1
    SELECT round(cast(sum(lunghezza) as numeric), 9) into dummy_decimal_expected from dbiait_analysis.fognat_tronchi where depurazione='1';
    SELECT round(cast(sum(lunghezza_dep) as numeric), 9) into dummy_decimal_actual from dbiait_analysis.FGN_LUNGHEZZA_RETE where tipo_infr='FOGNATURA';
    PERFORM test_assertTrue('Check: la lunghezza_dep è uguale alla lunghezza delle fognat_tronchi', dummy_decimal_expected = dummy_decimal_actual);

   --- for COLLETTORE the field 'lung_dep' should be the same as the sum of 'lunghezza' in fognat_tronchi with depurazione =1
    SELECT round(cast(sum(lunghezza) as numeric), 9) into dummy_decimal_expected  from dbiait_analysis.COLLETT_TRONCHI where depurazione='1' and idgis_rete is not null;
    SELECT round(cast(sum(lunghezza_dep) as numeric), 9) into dummy_decimal_actual  from dbiait_analysis.FGN_LUNGHEZZA_RETE where tipo_infr='COLLETTORE';
    PERFORM test_assertTrue('Check: la lunghezza_dep è uguale alla lunghezza dei collettori', dummy_decimal_expected = dummy_decimal_actual);

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- TEST POPULATE FGN SHAPE
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_fgn_shape(
    v_run_proc BOOLEAN DEFAULT FALSE
) returns void as $$
DECLARE
  dummy_int bigint;
  dummy_string varchar;
  dummy_decimal decimal;
begin
    -- run the new version of the procedure
    IF v_run_proc THEN
    	perform test_assertTrue('Verifica esito procedura', dbiait_analysis.populate_fgn_shape() );
    END IF;
	-- ASSERTION TESTS START FROM HERE

    --- given a selected ids_codi_1, the value of sezione should be null
    SELECT sezione into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000420230';
    PERFORM test_assertTrue('Check la sezione sia ALTRO', dummy_string = 'ALTRO' );

   --- given a selected ids_codi_1, the value of sezione should be circolare
    SELECT sezione into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000368712';
    PERFORM test_assertTrue('Check la sezione sia CIRCOLARE', dummy_string = 'CIRCOLARE' );

   --- given a selected ids_codi_1, the value of comune_nom should be the pro_com and not the codice_istat
    SELECT id_comune_ into dummy_int FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000375524';
    PERFORM test_assertTrue('Check comune sia pro_com e non id_istat', dummy_int = 48017 );

   --- given a selected ids_codi_1, the value of lunghz_1 should be rounded to the 6° decimal
    SELECT lunghez_1 into dummy_decimal FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000420850';
    PERFORM test_assertTrue('Check lunghez_ rounded al 6 decimale', dummy_decimal > 52.012974 and dummy_decimal < 52.012975 );

   --- given a selected ids_codi_1, the value of lunghz_1 should be rounded to the 6° decimal
    SELECT id_refluo_ into dummy_int FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000371107';
    PERFORM test_assertTrue('Check id_refluo_ is no longer 0', dummy_int = 1 );

   --- given a selected ids_codi_1, the value of copertura should be null in case of ASFALTO SIMILI
    SELECT copertura into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000400248';
    PERFORM test_assertTrue('Check copertura sia ASFALTO SIMILI', dummy_string = 'ASFALTO SIMILI' );

   --- given a selected ids_codi_1, the value of copertura should be TERRENO VEGETALE in case of NOT NULL
    SELECT copertura into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000400245';
    PERFORM test_assertTrue('Check id_refluo_ is no longer 0', dummy_string = 'TERRENO VEGETALE' );

   -- given the whole table, the total number of rows should be the same
    SELECT count(*) INTO dummy_int FROM dbiait_analysis.fgn_shape;
    PERFORM test_assertTrue('numero totale di righe è cambiato', 69246 = dummy_int );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;

-- ------------------------------------------------------------------------------------------
-- TEST POPULATE SCHEMA ACQ (ACQUEDOTTISTICO)
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_schema_acq(
    v_run_proc BOOLEAN DEFAULT FALSE
) returns void as $$
DECLARE
  cod_schema varchar;
  denom_schema varchar;
  error varchar;

begin
    -- run the new version of the procedure
    IF v_run_proc THEN
	    perform test_assertTrue('Verifica esito procedura', dbiait_analysis.populate_schema_acq() );
    END IF;
--- check if the output of the selected idgis is the expected
    SELECT codice_schema_acq,denominazione_schema_acq INTO cod_schema, denom_schema FROM dbiait_analysis.schema_acq sa WHERE idgis='PAARDI00000000001299';
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected DI01165 ma trovata ' || cod_schema , 'DI01165' = cod_schema );
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected CASOLE ma trovata ' || denom_schema , 'CASOLE' = denom_schema );

    --- check if the output of the selected idgis is the expected
    SELECT codice_schema_acq,denominazione_schema_acq INTO cod_schema, denom_schema FROM dbiait_analysis.schema_acq sa WHERE idgis='PAARDI00000000001402';
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected DI01215 ma trovata ' || cod_schema , 'DI01215' = cod_schema );
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected POGGIO DI LORO ma trovata ' || denom_schema , 'POGGIO DI LORO' = denom_schema );

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;

-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_ubic_allaccio(
    v_run_proc BOOLEAN DEFAULT FALSE
) returns void as $$
DECLARE
  sn_alla varchar;
  id_rete varchar;
  new_count bigint;
begin
    -- run the new version of the procedure
    IF v_run_proc THEN
    	perform test_assertTrue('Verifica esito procedura', dbiait_analysis.populate_ubic_allaccio() );
    END IF;
    --- check if the count of the selected id_rete is still the same
    SELECT acq_sn_alla,acq_idrete into sn_alla,id_rete FROM dbiait_analysis.ubic_allaccio ua WHERE id_ubic_contatore ='PAAUCO00000001907206';
    perform test_assertTrue('ID_rete wrong, expected PAARDI00000000001511 but found ' || id_rete, 'PAARDI00000000001511' = id_rete );
    perform test_assertTrue('sn_alla wrong, expected SI but found ' || sn_alla, 'SI' = sn_alla );
    --- check if the count of the selected id_rete is still the same
    SELECT acq_sn_alla,acq_idrete into sn_alla,id_rete FROM dbiait_analysis.ubic_allaccio ua WHERE id_ubic_contatore ='PAAUCO00000002073907';
    perform test_assertTrue('ID_rete wrong, expected PAARDI00000000001511 but found <' || id_rete || '>', 'PAARDI00000000001511' = id_rete );
    perform test_assertTrue('sn_alla wrong, expected NO but found ' || sn_alla, 'NO' = sn_alla );
    -- check if the total rows are the same

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_accumuli_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1017;
BEGIN
    --- check the count of the query for SHP of ACCUMULI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_accumulo WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR','IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_accumulo WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR','IAC')
    ) t;
    perform test_assertTrue('count SHP accumuli, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_acq_shape_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=119262;
BEGIN
    --- check the count of the query for SHP of ACQ_SHAPE
    SELECT count(0) INTO v_count FROM dbiait_analysis.acq_shape;
    perform test_assertTrue('count SHP acq_shape, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_depuratori_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=154;
BEGIN
    --- check the count of the query for SHP of DEPURATORI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.fgn_trattamento
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR','IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_fgn_trattamento
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR','IAC')
    ) t;
    perform test_assertTrue('count SHP depuratori, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_fgn_shape_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=69246;
BEGIN
    --- check the count of the query for SHP of FGN_SHAPE
    SELECT count(0) INTO v_count FROM dbiait_analysis.fgn_shape;
    perform test_assertTrue('count SHP fgn_shape, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_fiumi_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=74;
BEGIN
    --- check the count of the query for SHP of FIUMI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=0
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=0
    ) t;
    perform test_assertTrue('count SHP fiumi, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_laghi_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=36;
BEGIN
    --- check the count of the query for SHP of LAGHI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=1
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=1
    ) t;
    perform test_assertTrue('count SHP laghi, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_pompaggi_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=493;
BEGIN
    --- check the count of the query for SHP of POMPAGGI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_pompaggio
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_pompaggio
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
    ) t;
    perform test_assertTrue('count SHP pompaggi, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_potabilizzatori_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=142;
BEGIN
    --- check the count of the query for SHP of POTABILIZZATORI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_potabiliz
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_potabiliz
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
    ) t;
    perform test_assertTrue('count SHP potabilizzatori, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_pozzi_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=764;
BEGIN
    --- check the count of the query for SHP of POZZI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=3
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=3
    ) t;
    perform test_assertTrue('count SHP pozzi, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_scaricatori_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1118;
BEGIN
    --- check the count of the query for SHP of SCARICATORI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.fgn_sfioro
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_fgn_sfioro
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
    ) t;
    perform test_assertTrue('count SHP scaricatori, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_sollevamenti_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=250;
BEGIN
    --- check the count of the query for SHP of SOLLEVAMENTI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.fgn_imp_sollev
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
        union ALL
        select count(0) cnt from dbiait_analysis.a_fgn_imp_sollev
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC')
    ) t;
    perform test_assertTrue('count SHP sollevamenti, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_sqlexport_sorgenti_shp() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=934;
BEGIN
    --- check the count of the query for SHP of SORGENTI
    select sum(cnt) into v_count from (
        select count(0) cnt from dbiait_analysis.acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=4
        union ALL
        select count(0) cnt from dbiait_analysis.a_acq_captazione
        WHERE d_gestore = 'PUBLIACQUA' AND d_ambito IN ('AT3', NULL) AND d_stato NOT IN ('IPR', 'IAC') and SUB_FUNZIONE=4
    ) t;
    perform test_assertTrue('count SHP sorgenti, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_tolerance() returns void as $$
DECLARE
  v_tol         DOUBLE PRECISION:=0;
  v_expected    DOUBLE PRECISION:=0.001;
BEGIN
    v_tol := dbiait_analysis.snap_tolerance();
    perform test_assertTrue('check tolerance, expected ' || v_expected || ' but found ' || v_tol, v_tol = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_st_round_x() returns void as $$
DECLARE
  v_val         DOUBLE PRECISION:=0;
  v_expected    DOUBLE PRECISION:=1.2346;
BEGIN
    v_val := st_round_x('POINT(1.23456789 9.87654321)'::GEOMETRY, 4);
    perform test_assertTrue('check st_round_x, expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_st_round_y() returns void as $$
DECLARE
  v_val         DOUBLE PRECISION:=0;
  v_expected    DOUBLE PRECISION:=9.8765;
BEGIN
    v_val := st_round_y('POINT(1.23456789 9.87654321)'::GEOMETRY, 4);
    perform test_assertTrue('check st_round_y, expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_to_integer() returns void as $$
DECLARE
  v_val         INTEGER;
  v_expected    INTEGER:=123;
BEGIN
    v_val := to_integer('123');
    perform test_assertTrue('check to_integer(''123''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_to_integer_empty() returns void as $$
DECLARE
  v_val         INTEGER;
  v_expected    INTEGER:=0;
BEGIN
    v_val := to_integer('');
    perform test_assertTrue('check to_integer(''''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_to_integer_empty_defNull() returns void as $$
DECLARE
  v_val INTEGER;
BEGIN
    v_val := to_integer('', NULL);
    perform test_assertTrue('check to_integer('''', NULL), expected NULL but found ' || v_val, v_val is NULL );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_from_float_to_int() returns void as $$
DECLARE
  v_val INTEGER;
  v_expected    INTEGER:=9;
BEGIN
    v_val := from_float_to_int('9.4');
    perform test_assertTrue('check from_float_to_int(''9.4''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_st_transform_rm40_etrs89() returns void as $$
DECLARE
  v_x DOUBLE PRECISION;
  v_y DOUBLE PRECISION;
  v_x_exp DOUBLE PRECISION:=1705470.09;
  v_y_exp DOUBLE PRECISION:=4830688.67;
BEGIN
    SELECT ROUND(ST_X(geom)::NUMERIC,2) x, ROUND(ST_Y(geom)::NUMERIC,2) y
        into v_x, v_y
    FROM(
        SELECT ST_TRANSFORM_RM40_ETRS89(
            ST_SETSRID(ST_POINT(705438.9186,4830672.536), 25832)
        ) geom
    ) t;
    perform test_assertTrue('check st_transform_rm40_etrs89 (x), expected ' || v_x_exp || ' but found ' || v_x, v_x = v_x_exp );
    perform test_assertTrue('check st_transform_rm40_etrs89 (y), expected ' || v_y_exp || ' but found ' || v_y, v_y = v_y_exp );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_is_null() returns void as $$
DECLARE
  v_val INTEGER;
  v_expected    INTEGER:=1;
BEGIN
    v_val := is_null(null::INTEGER);
    perform test_assertTrue('check is_null(NULL), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_expected := 0;
    v_val := is_null(5::INTEGER);
    perform test_assertTrue('check is_null(5), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := is_null('A'::VARCHAR);
    perform test_assertTrue('check is_null(''A''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := is_null(''::VARCHAR);
    perform test_assertTrue('check is_null(''''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_to_bit() returns void as $$
DECLARE
  v_val INTEGER;
  v_expected    INTEGER:=1;
BEGIN
    v_val := to_bit('SI');
    perform test_assertTrue('check to_bit(''SI''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := to_bit('YES');
    perform test_assertTrue('check to_bit(''YES''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := to_bit('S');
    perform test_assertTrue('check to_bit(''S''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := to_bit('Y');
    perform test_assertTrue('check to_bit(''Y''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := to_bit('1');
    perform test_assertTrue('check to_bit(''1''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_expected := 0;
    v_val := to_bit('N');
    perform test_assertTrue('check to_bit(''N''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := to_bit('?');
    perform test_assertTrue('check to_bit(''N''), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_func_decode_municipal() returns void as $$
DECLARE
  v_val      INTEGER;
  v_expected INTEGER;
BEGIN
    v_expected:=48054;
    v_val := DBIAIT_ANALYSIS.decode_municipal(48003);
    perform test_assertTrue('check decode_municipal(48003), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := DBIAIT_ANALYSIS.decode_municipal(48045);
    perform test_assertTrue('check decode_municipal(48003), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_expected:=47024;
    v_val := DBIAIT_ANALYSIS.decode_municipal(47015);
    perform test_assertTrue('check decode_municipal(47015), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_val := DBIAIT_ANALYSIS.decode_municipal(47019);
    perform test_assertTrue('check decode_municipal(47019), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
    v_expected:=51015;
    v_val := DBIAIT_ANALYSIS.decode_municipal(51015);
    perform test_assertTrue('check decode_municipal(51015), expected ' || v_expected || ' but found ' || v_val, v_val = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis._test_expected_xls_count(v_sheet IN VARCHAR) returns INTEGER as $$
DECLARE
    v_count INTEGER;
BEGIN
    v_count := ('{
        "XLS_ACCUMULI":         1017,
        "XLS_ACCUMULI_INADD":   783,
        "XLS_ACCUMULI_INRETI":  914,
        "XLS_ADDUT_COM_SERV":   822,
        "XLS_ADDUT_INRETI":     819,
        "XLS_ADDUT_TRONCHI":    10443,
        "XLS_ADDUTTRICI":       751,
        "XLS_COLLETT_COM_SERV": 210,
        "XLS_COLLETT_TRONCHI":  6013,
        "XLS_COLLETTORI":       161,
        "XLS_CONDOTTEMARINE":   0,
        "XLS_DEPURAT_INCOLL":   43,
        "XLS_DEPURAT_POMPE":    429,
        "XLS_DEPURATORI":       154,
        "XLS_DISTRIB_COM_SERV": 451,
        "XLS_DISTRIB_LOC_SERV": 1430,
        "XLS_DISTRIB_QUALITA":  0,
        "XLS_DISTRIB_TRONCHI":  108819,
        "XLS_DISTRIBUZIONI":    244,
        "XLS_FIUMI":            74,
        "XLS_FIUMI_INPOTAB":    0,
        "XLS_FIUMI_INRETI":     60,
        "XLS_FOGNAT_COM_SERV":  0,
        "XLS_FOGNAT_LOC_SERV":  0,
        "XLS_FOGNAT_TRONCHI":   63233,
        "XLS_FOGNATURE":        1027,
        "XLS_LAGHI":            36,
        "XLS_LAGHI_INPOTAB":    0,
        "XLS_LAGHI_INRETI":     22,
        "XLS_POMPAGGI":         493,
        "XLS_POMPAGGI_INPOTAB": 0,
        "XLS_POMPAGGI_INSERBA": 0,
        "XLS_POMPAGGI_POMPE":   875,
        "XLS_POTAB_INCAPTAZ":   0,
        "XLS_POTAB_INRETI":     99,
        "XLS_POTAB_POMPE":      221,
        "XLS_POTABILIZZATORI":  142,
        "XLS_POZZI":            764,
        "XLS_POZZI_INPOTAB":    0,
        "XLS_POZZI_INRETI":     576,
        "XLS_POZZI_POMPE":      578,
        "XLS_POZZI_QUALITA":    0,
        "XLS_SCARICAT_INFOG":   1074,
        "XLS_SCARICATORI":      1118,
        "XLS_SOLLEV_POMPE":     537,
        "XLS_SOLLEVAMENTI":     250,
        "XLS_SORGENT_INPOTAB":  0,
        "XLS_SORGENTI":         934,
        "XLS_SORGENTI_INRETI":  838,
        "XLS_SORGENTI_QUALITA": 0
    }'::JSON)->v_sheet;
    RETURN COALESCE(v_count,0);
EXCEPTION WHEN OTHERS THEN
    RETURN 0;
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis._test_expected_count(v_table IN VARCHAR) returns INTEGER as $$
DECLARE
    v_count INTEGER;
BEGIN
    v_count := ('{
        "POP_RES_LOC":          			1072,
        "DISTRIB_LOC_SERV":     			1430,
        "POP_RES_COMUNE":       			54,
        "DISTRIB_COM_SERV":     			451,
        "UTENZA_SERVIZIO":      			425971,
        "UTENZA_SERVIZIO_LOC":  			425970,
        "UTENZA_SERVIZIO_ACQ":  			425916,
        "UTENZA_SERVIZIO_FGN":  			380435,
        "UTENZA_SERVIZIO_BAC":  			380184,
        "ABITANTI_TRATTATI":    			846,
        "DISTRIB_TRONCHI":      			108819,
        "ADDUT_TRONCHI":        			10443,
        "ACQ_COND_ALTRO":       			119262,
		"ACQ_COND_ALTRO_A":     			10443,
		"ACQ_COND_ALTRO_D":     			108819,
        "ACQ_LUNGHEZZA_RETE":   			971,
        "ACQ_LUNGHEZZA_RETE_A": 			727,
        "ACQ_LUNGHEZZA_RETE_D": 			244,
        "FOGNAT_TRONCHI":       			63233,
        "COLLETT_TRONCHI":      			6013,
        "FGN_COND_ALTRO":       			69246,
        "FGN_COND_ALTRO_C":     			6013,
        "FGN_COND_ALTRO_R":     			63233,
        "FGN_LUNGHEZZA_RETE": 				1092,
        "FGN_LUNGHEZZA_RETE_C": 			160,
        "FGN_LUNGHEZZA_RETE_F": 			932,
        "ACQ_ALLACCIO": 					230003,
        "ACQ_LUNGHEZZA_ALLACCI": 			971,
        "SUPPORT_ACQ_ALLACCI": 				231228,
        "FGN_ALLACCIO": 					203832,
        "FGN_LUNGHEZZA_ALLACCI": 			1092,
        "FGN_LUNGHEZZA_ALLACCI_id_rete":	923,
        "SUPPORT_FGN_ALLACCI": 				203833,
        "ACQ_SHAPE": 						119262,
        "ACQ_SHAPE_A": 						10443,
        "ACQ_SHAPE_D": 						108819,
        "ACQ_VOL_UTENZE": 					244,
        "FGN_SHAPE": 						69246,
        "FGN_SHAPE_F": 						63233,
        "FGN_SHAPE_C": 						6013,
        "FGN_VOL_UTENZE": 					935,
        "STATS_POMPE": 						1413,
        "POZZI_POMPE": 						578,
        "POTAB_POMPE": 						220,
        "POMPAGGI_POMPE": 					875,
        "SOLLEV_POMPE": 					536,
        "DEPURATO_POMPE": 					425,
        "ADDUT_COM_SERV":		 			822,
        "COLLET_COM_SERV": 					210,
        "FIUMI_INRETI": 					60,
        "LAGHI_INRETI": 					22,
        "POZZI_INRETI": 					576,
        "SORGENTI_INRETI": 					838,
        "POTAB_INRETI": 					99,
        "ADDUT_INRETI": 					819,
        "ACCUMULI_INRETI": 					914,
        "ACCUMULI_INADD": 					783,
        "DEPURATO_INCOLL": 					43,
        "SCARICATO_INFOG": 					1074,
        "ACQ_CONDOTTA_NODES": 				161569,
        "ACQ_CONDOTTA_EDGES": 				153193,
        "FGN_CONDOTTA_NODES": 				82198,
        "FGN_CONDOTTA_EDGES": 				80517,
        "STATS_CLORATORE": 					43,
        "SCHEMA_ACQ": 						1173,
        "UBIC_ALLACCIO": 					423768,
        "UBIC_CONTATORI_CASS_CONT": 		423768,
        "UTENZE_DISTRIBUZIONI_ADDUTTRICI": 	390,
        "UBIC_CONTATORI_FGN": 				425971,
        "UBIC_F_ALLACCIO": 					425971,
        "UTENZE_FOGNATURE_COLLETTORI": 		1075,
        "SUPPORT_CODICE_CAPT_ACCORP": 		1853,
        "SUPPORT_POZZI_INPOTAB":            764
    }'::JSON)->v_table;
    RETURN COALESCE(v_count,0);
EXCEPTION WHEN OTHERS THEN
    RETURN 0;
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POP_RES_LOC_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POP_RES_LOC');
BEGIN
    select count(0) into v_count from dbiait_analysis.POP_RES_LOC;
    perform test_assertTrue('count TAB POP_RES_LOC, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_LOC_SERV_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('DISTRIB_LOC_SERV');
BEGIN
    select count(0) into v_count from dbiait_analysis.DISTRIB_LOC_SERV;
    perform test_assertTrue('count TAB DISTRIB_LOC_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POP_RES_COMUNE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POP_RES_COMUNE');
BEGIN
    select count(0) into v_count from dbiait_analysis.POP_RES_COMUNE;
    perform test_assertTrue('count TAB POP_RES_COMUNE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_COM_SERV_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('DISTRIB_COM_SERV');
BEGIN
    select count(0) into v_count from dbiait_analysis.DISTRIB_COM_SERV;
    perform test_assertTrue('count TAB DISTRIB_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZA_SERVIZIO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZA_SERVIZIO');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZA_SERVIZIO;
    perform test_assertTrue('count TAB UTENZA_SERVIZIO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZA_SERVIZIO_LOC_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZA_SERVIZIO_LOC');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZA_SERVIZIO_LOC;
    perform test_assertTrue('count TAB UTENZA_SERVIZIO_LOC, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZA_SERVIZIO_ACQ_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZA_SERVIZIO_ACQ');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZA_SERVIZIO_ACQ;
    perform test_assertTrue('count TAB UTENZA_SERVIZIO_ACQ, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZA_SERVIZIO_FGN_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZA_SERVIZIO_FGN');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZA_SERVIZIO_FGN;
    perform test_assertTrue('count TAB UTENZA_SERVIZIO_FGN, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZA_SERVIZIO_BAC_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZA_SERVIZIO_BAC');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZA_SERVIZIO_BAC;
    perform test_assertTrue('count TAB UTENZA_SERVIZIO_BAC, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ABITANTI_TRATTATI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ABITANTI_TRATTATI');
BEGIN
    select count(0) into v_count from dbiait_analysis.ABITANTI_TRATTATI;
    perform test_assertTrue('count TAB ABITANTI_TRATTATI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DISTRIB_TRONCHI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('DISTRIB_TRONCHI');
BEGIN
    select count(0) into v_count from dbiait_analysis.DISTRIB_TRONCHI;
    perform test_assertTrue('count TAB DISTRIB_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_TRONCHI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ADDUT_TRONCHI');
BEGIN
    select count(0) into v_count from dbiait_analysis.ADDUT_TRONCHI;
    perform test_assertTrue('count TAB ADDUT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_COND_ALTRO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_COND_ALTRO');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_COND_ALTRO;
    perform test_assertTrue('count TAB ACQ_COND_ALTRO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('ACQ_COND_ALTRO_A');
    select count(0) into v_count from dbiait_analysis.ACQ_COND_ALTRO where tipo_infr='ADDUZIONI';
    perform test_assertTrue('count TAB ACQ_COND_ALTRO (tipo_infr ADDUZIONI), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('ACQ_COND_ALTRO_D');
    select count(0) into v_count from dbiait_analysis.ACQ_COND_ALTRO where tipo_infr='DISTRIBUZIONI';
    perform test_assertTrue('count TAB ACQ_COND_ALTRO (tipo_infr DISTRIBUZIONI), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_LUNGHEZZA_RETE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_LUNGHEZZA_RETE');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_LUNGHEZZA_RETE;
    perform test_assertTrue('count TAB ACQ_LUNGHEZZA_RETE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('ACQ_LUNGHEZZA_RETE_A');
    select count(0) into v_count from dbiait_analysis.ACQ_LUNGHEZZA_RETE where tipo_infr='ADDUZIONE';
    perform test_assertTrue('count TAB ACQ_LUNGHEZZA_RETE (tipo_infr ADDUZIONE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('ACQ_LUNGHEZZA_RETE_D');
    select count(0) into v_count from dbiait_analysis.ACQ_LUNGHEZZA_RETE where tipo_infr='DISTRIBUZIONE';
    perform test_assertTrue('count TAB ACQ_LUNGHEZZA_RETE (tipo_infr DISTRIBUZIONE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FOGNAT_TRONCHI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FOGNAT_TRONCHI');
BEGIN
    select count(0) into v_count from dbiait_analysis.FOGNAT_TRONCHI;
    perform test_assertTrue('count TAB FOGNAT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_COLLETT_TRONCHI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('COLLETT_TRONCHI');
BEGIN
    select count(0) into v_count from dbiait_analysis.COLLETT_TRONCHI;
    perform test_assertTrue('count TAB COLLETT_TRONCHI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_COND_ALTRO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_COND_ALTRO');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_COND_ALTRO;
    perform test_assertTrue('count TAB FGN_COND_ALTRO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('FGN_COND_ALTRO_C');
    select count(0) into v_count from dbiait_analysis.FGN_COND_ALTRO where tipo_infr='COLLETTORI';
    perform test_assertTrue('count TAB FGN_COND_ALTRO (tipo_infr COLLETTORI), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected :=dbiait_analysis._test_expected_count('FGN_COND_ALTRO_R');
    select count(0) into v_count from dbiait_analysis.FGN_COND_ALTRO where tipo_infr='RETE RACCOLTA';
    perform test_assertTrue('count TAB FGN_COND_ALTRO (tipo_infr RETE RACCOLTA), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_LUNGHEZZA_RETE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_LUNGHEZZA_RETE');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_LUNGHEZZA_RETE;
    perform test_assertTrue('count TAB FGN_LUNGHEZZA_RETE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('FGN_LUNGHEZZA_RETE_C');
    select count(0) into v_count from dbiait_analysis.FGN_LUNGHEZZA_RETE WHERE tipo_infr = 'COLLETTORE';
    perform test_assertTrue('count TAB FGN_LUNGHEZZA_RETE (tipo_infr COLLETTORE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('FGN_LUNGHEZZA_RETE_F');
    select count(0) into v_count from dbiait_analysis.FGN_LUNGHEZZA_RETE WHERE tipo_infr = 'FOGNATURA';
    perform test_assertTrue('count TAB FGN_LUNGHEZZA_RETE (tipo_infr FOGNATURA), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_ALLACCIO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_ALLACCIO');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_ALLACCIO;
    perform test_assertTrue('count TAB ACQ_ALLACCIO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_LUNGHEZZA_ALLACCI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_LUNGHEZZA_ALLACCI');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_LUNGHEZZA_ALLACCI;
    perform test_assertTrue('count TAB ACQ_LUNGHEZZA_ALLACCI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SUPPORT_ACQ_ALLACCI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SUPPORT_ACQ_ALLACCI');
BEGIN
    select count(0) into v_count from dbiait_analysis.SUPPORT_ACQ_ALLACCI;
    perform test_assertTrue('count TAB SUPPORT_ACQ_ALLACCI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_ALLACCIO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_ALLACCIO');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_ALLACCIO;
    perform test_assertTrue('count TAB FGN_ALLACCIO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_LUNGHEZZA_ALLACCI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_LUNGHEZZA_ALLACCI');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_LUNGHEZZA_ALLACCI;
    perform test_assertTrue('count TAB FGN_LUNGHEZZA_ALLACCI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_LUNGHEZZA_ALLACCI_id_rete_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_LUNGHEZZA_ALLACCI_id_rete');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_LUNGHEZZA_ALLACCI_id_rete;
    perform test_assertTrue('count TAB FGN_LUNGHEZZA_ALLACCI_id_rete, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SUPPORT_FGN_ALLACCI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SUPPORT_FGN_ALLACCI');
BEGIN
    select count(0) into v_count from dbiait_analysis.SUPPORT_FGN_ALLACCI;
    perform test_assertTrue('count TAB SUPPORT_FGN_ALLACCI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_SHAPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_SHAPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_SHAPE;
    perform test_assertTrue('count TAB ACQ_SHAPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('ACQ_SHAPE_A');
    select count(0) into v_count from dbiait_analysis.ACQ_SHAPE WHERE tipo_rete = 'ADDUZIONE';
    perform test_assertTrue('count TAB ACQ_SHAPE (tipo_rete=ADDUZIONE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('ACQ_SHAPE_D');
    select count(0) into v_count from dbiait_analysis.ACQ_SHAPE WHERE tipo_rete = 'DISTRIBUZIONE';
    perform test_assertTrue('count TAB ACQ_SHAPE (tipo_rete=DISTRIBUZIONE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACQ_VOL_UTENZE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_VOL_UTENZE');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_VOL_UTENZE;
    perform test_assertTrue('count TAB ACQ_VOL_UTENZE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_SHAPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_SHAPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_SHAPE;
    perform test_assertTrue('count TAB FGN_SHAPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('FGN_SHAPE_F');
    select count(0) into v_count from dbiait_analysis.FGN_SHAPE WHERE tipo_rete = 'FOGNATURA';
    perform test_assertTrue('count TAB FGN_SHAPE (tipo_rete=FOGNATURA), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('FGN_SHAPE_C');
    select count(0) into v_count from dbiait_analysis.FGN_SHAPE WHERE tipo_rete = 'COLLETTORE';
    perform test_assertTrue('count TAB FGN_SHAPE (tipo_rete=COLLETTORE), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FGN_VOL_UTENZE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_VOL_UTENZE');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_VOL_UTENZE;
    perform test_assertTrue('count TAB FGN_VOL_UTENZE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_STATS_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('STATS_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.STATS_POMPE;
    perform test_assertTrue('count TAB STATS_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POZZI_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.POZZI_POMPE;
    perform test_assertTrue('count TAB POZZI_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTAB_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POTAB_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.POTAB_POMPE;
    perform test_assertTrue('count TAB POTAB_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POMPAGGI_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POMPAGGI_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.POMPAGGI_POMPE;
    perform test_assertTrue('count TAB POMPAGGI_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SOLLEV_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SOLLEV_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.SOLLEV_POMPE;
    perform test_assertTrue('count TAB SOLLEV_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DEPURATO_POMPE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('DEPURATO_POMPE');
BEGIN
    select count(0) into v_count from dbiait_analysis.DEPURATO_POMPE;
    perform test_assertTrue('count TAB DEPURATO_POMPE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_COM_SERV_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ADDUT_COM_SERV');
BEGIN
    select count(0) into v_count from dbiait_analysis.ADDUT_COM_SERV;
    perform test_assertTrue('count TAB ADDUT_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_COLLET_COM_SERV_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('COLLET_COM_SERV');
BEGIN
    select count(0) into v_count from dbiait_analysis.COLLET_COM_SERV;
    perform test_assertTrue('count TAB COLLET_COM_SERV, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_FIUMI_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FIUMI_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.FIUMI_INRETI;
    perform test_assertTrue('count TAB FIUMI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_LAGHI_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('LAGHI_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.LAGHI_INRETI;
    perform test_assertTrue('count TAB LAGHI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POZZI_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POZZI_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.POZZI_INRETI;
    perform test_assertTrue('count TAB POZZI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SORGENTI_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SORGENTI_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.SORGENTI_INRETI;
    perform test_assertTrue('count TAB SORGENTI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_POTAB_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('POTAB_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.POTAB_INRETI;
    perform test_assertTrue('count TAB POTAB_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ADDUT_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ADDUT_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.ADDUT_INRETI;
    perform test_assertTrue('count TAB ADDUT_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACCUMULI_INRETI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACCUMULI_INRETI');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACCUMULI_INRETI;
    perform test_assertTrue('count TAB ACCUMULI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );

    v_expected := 3;
    select count(0) into v_count from DBIAIT_ANALYSIS.LOG_STANDALONE WHERE alg_name ='ACCUMULI_INRETI';
    perform test_assertTrue('count anomalies ACCUMULI_INRETI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );

END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_ACCUMULI_INADD_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACCUMULI_INADD');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACCUMULI_INADD;
    perform test_assertTrue('count TAB ACCUMULI_INADD, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_DEPURATO_INCOLL_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('DEPURATO_INCOLL');
BEGIN
    select count(0) into v_count from dbiait_analysis.DEPURATO_INCOLL;
    perform test_assertTrue('count TAB DEPURATO_INCOLL, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SCARICATO_INFOG_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SCARICATO_INFOG');
BEGIN
    select count(0) into v_count from dbiait_analysis.SCARICATO_INFOG;
    perform test_assertTrue('count TAB SCARICATO_INFOG, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_GRAFO_ACQUEDOTTO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('ACQ_CONDOTTA_NODES');
BEGIN
    select count(0) into v_count from dbiait_analysis.ACQ_CONDOTTA_NODES;
    perform test_assertTrue('count TAB ACQ_CONDOTTA_NODES, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('ACQ_CONDOTTA_EDGES');
    select count(0) into v_count from dbiait_analysis.ACQ_CONDOTTA_EDGES;
    perform test_assertTrue('count TAB ACQ_CONDOTTA_EDGES, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_GRAFO_FOGNATURA_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('FGN_CONDOTTA_NODES');
BEGIN
    select count(0) into v_count from dbiait_analysis.FGN_CONDOTTA_NODES;
    perform test_assertTrue('count TAB FGN_CONDOTTA_NODES, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    v_expected := dbiait_analysis._test_expected_count('FGN_CONDOTTA_EDGES');
    select count(0) into v_count from dbiait_analysis.FGN_CONDOTTA_EDGES;
    perform test_assertTrue('count TAB FGN_CONDOTTA_EDGES, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_STATS_CLORATORE_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('STATS_CLORATORE');
BEGIN
    select count(0) into v_count from dbiait_analysis.STATS_CLORATORE WHERE counter>0;
    perform test_assertTrue('count TAB STATS_CLORATORE, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SCHEMA_ACQ_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SCHEMA_ACQ');
BEGIN
    select count(0) into v_count from dbiait_analysis.SCHEMA_ACQ;
    perform test_assertTrue('count TAB SCHEMA_ACQ, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UBIC_ALLACCIO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UBIC_ALLACCIO');
BEGIN
    select count(0) into v_count from dbiait_analysis.UBIC_ALLACCIO;
    perform test_assertTrue('count TAB UBIC_ALLACCIO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UBIC_F_ALLACCIO_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UBIC_F_ALLACCIO');
BEGIN
    select count(0) into v_count from dbiait_analysis.UBIC_F_ALLACCIO;
    perform test_assertTrue('count TAB UBIC_F_ALLACCIO, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UBIC_CONTATORI_CASS_CONT_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UBIC_CONTATORI_CASS_CONT');
BEGIN
    select count(0) into v_count from dbiait_analysis.UBIC_CONTATORI_CASS_CONT;
    perform test_assertTrue('count TAB UBIC_CONTATORI_CASS_CONT, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UBIC_CONTATORI_FGN_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UBIC_CONTATORI_FGN');
BEGIN
    select count(0) into v_count from dbiait_analysis.UBIC_CONTATORI_FGN;
    perform test_assertTrue('count TAB UBIC_CONTATORI_FGN, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZE_DISTRIBUZIONI_ADDUTTRICI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZE_DISTRIBUZIONI_ADDUTTRICI');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZE_DISTRIBUZIONI_ADDUTTRICI;
    perform test_assertTrue('count TAB UTENZE_DISTRIBUZIONI_ADDUTTRICI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_UTENZE_FOGNATURE_COLLETTORI_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('UTENZE_FOGNATURE_COLLETTORI');
BEGIN
    select count(0) into v_count from dbiait_analysis.UTENZE_FOGNATURE_COLLETTORI;
    perform test_assertTrue('count TAB UTENZE_FOGNATURE_COLLETTORI, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SUPPORT_CODICE_CAPT_ACCORP_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SUPPORT_CODICE_CAPT_ACCORP');
BEGIN
    select count(0) into v_count from dbiait_analysis.SUPPORT_CODICE_CAPT_ACCORP;
    perform test_assertTrue('count TAB SUPPORT_CODICE_CAPT_ACCORP, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_count_SUPPORT_POZZI_INPOTAB_tab() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=dbiait_analysis._test_expected_count('SUPPORT_POZZI_INPOTAB');
BEGIN
    select count(0) into v_count from dbiait_analysis.SUPPORT_POZZI_INPOTAB;
    perform test_assertTrue('count TAB SUPPORT_POZZI_INPOTAB, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_duplicated_SUPPORT_POZZI_INPOTAB_tab() returns void as $$
DECLARE
  v_count BIGINT:=0;
BEGIN
    select max(cnt) INTO v_count from (
        select ids_codice, count(0) cnt
        from dbiait_analysis.SUPPORT_POZZI_INPOTAB group by ids_codice
    ) t;
    perform test_assertTrue('check for duplicated items in TAB SUPPORT_POZZI_INPOTAB', v_count = 1 );

END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenza_padre_defalco_non_a_sistema() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1733;
BEGIN
    select utenze_distribuzioni_adduttrici.nr_allacci
    INTO v_count
    from dbiait_analysis.acq_rete_distrib acq_rete_distrib
        left join dbiait_analysis.acq_auth_rete_dist acq_auth_rete_dist on acq_rete_distrib.idgis = acq_auth_rete_dist.id_rete_distrib
        left join dbiait_analysis.acq_lunghezza_rete acq_lunghezza_rete on acq_lunghezza_rete.idgis = acq_rete_distrib.idgis
        left join dbiait_analysis.acq_vol_utenze acq_vol_utenze on acq_vol_utenze.ids_codice_orig_acq = acq_rete_distrib.codice_ato
        left join dbiait_analysis.utenze_distribuzioni_adduttrici utenze_distribuzioni_adduttrici on utenze_distribuzioni_adduttrici.id_rete = acq_rete_distrib.idgis
        left join dbiait_analysis.stats_cloratore stats_cloratore on acq_rete_distrib.idgis = stats_cloratore.id_rete
        left join dbiait_analysis.schema_acq schema_acq on acq_rete_distrib.idgis = schema_acq.idgis
    where acq_rete_distrib.d_gestore = 'PUBLIACQUA'
        and acq_rete_distrib.d_ambito in ('AT3', null)
        and acq_rete_distrib.d_stato not in ('IPR', 'IAC')
        and acq_rete_distrib.codice_ato ='DI01149';
    perform test_assertTrue('count FOR DEFALCO PADRE NON A SISTEMA, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_nr_allacci_distribuzioni_codice_ato_DI01075() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=18502;
BEGIN
    select utenze_distribuzioni_adduttrici.nr_allacci
    INTO v_count
    from dbiait_analysis.acq_rete_distrib acq_rete_distrib
        left join dbiait_analysis.acq_auth_rete_dist acq_auth_rete_dist on acq_rete_distrib.idgis = acq_auth_rete_dist.id_rete_distrib
        left join dbiait_analysis.acq_lunghezza_rete acq_lunghezza_rete on acq_lunghezza_rete.idgis = acq_rete_distrib.idgis
        left join dbiait_analysis.acq_vol_utenze acq_vol_utenze on acq_vol_utenze.ids_codice_orig_acq = acq_rete_distrib.codice_ato
        left join dbiait_analysis.utenze_distribuzioni_adduttrici utenze_distribuzioni_adduttrici on utenze_distribuzioni_adduttrici.id_rete = acq_rete_distrib.idgis
        left join dbiait_analysis.stats_cloratore stats_cloratore on acq_rete_distrib.idgis = stats_cloratore.id_rete
        left join dbiait_analysis.schema_acq schema_acq on acq_rete_distrib.idgis = schema_acq.idgis
    where acq_rete_distrib.d_gestore = 'PUBLIACQUA'
        and acq_rete_distrib.d_ambito in ('AT3', null)
        and acq_rete_distrib.d_stato not in ('IPR', 'IAC')
        and acq_rete_distrib.codice_ato ='DI01075';
    perform test_assertTrue('count FOR distribuzioni id DI01075, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_nr_allacci_distribuzioni_codice_ato_DI01079() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=148;
BEGIN
    select utenze_distribuzioni_adduttrici.nr_allacci
	INTO v_count
    from dbiait_analysis.acq_rete_distrib acq_rete_distrib
        left join dbiait_analysis.acq_auth_rete_dist acq_auth_rete_dist on acq_rete_distrib.idgis = acq_auth_rete_dist.id_rete_distrib
        left join dbiait_analysis.acq_lunghezza_rete acq_lunghezza_rete on acq_lunghezza_rete.idgis = acq_rete_distrib.idgis
        left join dbiait_analysis.acq_vol_utenze acq_vol_utenze on acq_vol_utenze.ids_codice_orig_acq = acq_rete_distrib.codice_ato
        left join dbiait_analysis.utenze_distribuzioni_adduttrici utenze_distribuzioni_adduttrici on utenze_distribuzioni_adduttrici.id_rete = acq_rete_distrib.idgis
        left join dbiait_analysis.stats_cloratore stats_cloratore on acq_rete_distrib.idgis = stats_cloratore.id_rete
        left join dbiait_analysis.schema_acq schema_acq on acq_rete_distrib.idgis = schema_acq.idgis
    where acq_rete_distrib.d_gestore = 'PUBLIACQUA'
        and acq_rete_distrib.d_ambito in ('AT3', null)
        and acq_rete_distrib.d_stato not in ('IPR', 'IAC')
        and acq_rete_distrib.codice_ato ='DI01079';
    perform test_assertTrue('count FOR distribuzioni id DI01079, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_nr_utenze_misuratore_PAACON00000000752869() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=14;
BEGIN
    select utenze_mis INTO v_count from dbiait_analysis.acq_shape where ids_codi_1 = 'PAACON00000000752869';
    perform test_assertTrue('count FOR acq_shape.utenze_mis id PAACON00000000752869, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_ubic_cont_fuori_rete_e_idrete_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1;
BEGIN
    select count(0)
    INTO v_count
    from dbiait_analysis.log_standalone
    where description='Contatore servito da Fognatura non allacciato e fuori rete di raccolta'
    and id = 'PAAUCO00000002102420';
    perform test_assertTrue('count LOG standalone id PAAUCO00000002102420 (Contatore servito da Fognatura non allacciato e fuori rete di raccolta), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_logstda_ubic_cont_fuori_rete_e_idrete_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1745;
BEGIN

    select count(0) INTO v_count
    from dbiait_analysis.log_standalone
    where description='Contatore servito da Fognatura non allacciato e fuori rete di raccolta';

    perform test_assertTrue('count total LOG standalone (Contatore servito da Fognatura non allacciato e fuori rete di raccolta), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_logstda_ubic_cont_non_allacciato() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=7365;
BEGIN

    select count(0) INTO v_count
    from dbiait_analysis.log_standalone
    where description='Contatore servito da Fognatura non allacciato';
    perform test_assertTrue('count total LOG standalone (Contatore servito da Fognatura non allacciato), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_logstda_ubic_cont_non_allacciato_minus_fuori_rete_e_idrete_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=5620;
BEGIN
    select count(0) INTO v_count
    from dbiait_analysis.log_standalone
    where description='Contatore servito da Fognatura non allacciato'
    and id not in (
        select id from dbiait_analysis.log_standalone
        where description='Contatore servito da Fognatura non allacciato e fuori rete di raccolta'
    );
    perform test_assertTrue('count total LOG standalone (Contatore servito da Fognatura non allacciato), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(utenze_mis) INTO v_count
    from dbiait_analysis.acq_shape
    where utenze_mis IS NULL;
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000905600() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=15;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000905600';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000905600), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000755672() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=8;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000755672';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000755672), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000769625() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=110;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000769625';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000769625), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000758673() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=105;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000758673';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000758673), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000806238() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=31;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000806238';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000806238), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000753473() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=6;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000753473';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000753473), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
---------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000905676() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=1;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000905676';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000905676), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000798961() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=8;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000798961';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000798961), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000856224() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=2;18100
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000856224';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000856224), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000818246() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=6;
BEGIN
    select utenze_mis INTO v_count
    from dbiait_analysis.acq_shape
    where ids_codi_1 = 'PAACON00000000818246';
    perform test_assertTrue('ACQ_SHAPE:UTENZE_MIS (PAACON00000000818246), expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_loc_48021() returns void as $$
DECLARE
  v_count       BIGINT:=0;
  v_expected    BIGINT:=13767;
BEGIN
    SELECT sum(popres) INTO v_count FROM dbiait_analysis.pop_res_loc where pro_com='48021';
    perform test_assertTrue('POP_RES_LOC (48021):, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );

    SELECT count(0) INTO v_count FROM dbiait_analysis.pop_res_loc where pro_com='48021' and popres=0;
    perform test_assertTrue('Ci sono ' || v_count || ' localita senza popolazione!', v_count = 0);

END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_support_codice_capt_accorp_PAACAP00000000012960() returns void as $$
DECLARE
  v_count          BIGINT:=0;
  v_expected       BIGINT:=1;
  v_denom          VARCHAR;
BEGIN
    select count(0) INTO v_count from dbiait_analysis.support_codice_capt_accorp where idgis='PAACAP00000000012960' ;
    perform test_assertTrue('populate_codice_capt_accorp (PAACAP00000000012960):, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    select count(0) INTO v_count
    from dbiait_analysis.support_codice_capt_accorp
    where idgis='PAACAP00000000012960' ;
    perform test_assertTrue(
        'populate_codice_capt_accorp (PAACAP00000000012960):, expected ' || v_expected || ' but found ' || v_count,
        v_count = v_expected
    );

    select COALESCE(denom, '?') INTO v_denom from dbiait_analysis.support_codice_capt_accorp where idgis='PAACAP00000000012960' ;
    perform test_assertTrue('populate_codice_capt_accorp (PAACAP00000000012960):, expected <NULL> but found ' || v_denom, v_denom = '?' );
    select COALESCE(denom, '?') INTO v_denom
    from dbiait_analysis.support_codice_capt_accorp
    where idgis='PAACAP00000000012960';
    perform test_assertTrue(
        'populate_codice_capt_accorp (PAACAP00000000012960):, expected <NULL> but found ' || v_denom,
        v_denom = '?'
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_support_codice_capt_accorp_PAACAP00000000011433() returns void as $$
DECLARE
  v_count          BIGINT:=0;
  v_expected       BIGINT:=1;
  v_denom          VARCHAR;
BEGIN
    select count(0) INTO v_count from dbiait_analysis.support_codice_capt_accorp where idgis='PAACAP00000000011433' ;
    perform test_assertTrue('populate_codice_capt_accorp (PAACAP00000000011433):, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
    select count(0) INTO v_count
    from dbiait_analysis.support_codice_capt_accorp
    where idgis='PAACAP00000000011433' ;
    perform test_assertTrue(
        'populate_codice_capt_accorp (PAACAP00000000011433):, expected ' || v_expected || ' but found ' || v_count,
        v_count = v_expected
    );

    select COALESCE(denom, '?') INTO v_denom from dbiait_analysis.support_codice_capt_accorp where idgis='PAACAP00000000011433' ;
    perform test_assertTrue('populate_codice_capt_accorp (PAACAP00000000011433):, expected <NULL> but found ' || v_denom, v_denom = '?' );
    select COALESCE(denom, '?') INTO v_denom
    from dbiait_analysis.support_codice_capt_accorp
    where idgis='PAACAP00000000011433';
    perform test_assertTrue(
        'populate_codice_capt_accorp (PAACAP00000000011433):, expected <NULL> but found ' || v_denom,
        v_denom = '?'
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_ADDUT() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check total number of records in the support table
    select count(0) INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete like 'PAAADD%' AND counter > 0;
    perform test_assertTrue('populate_STATS_CLORATORE (ADDUTTRICI): total expected 30 but found ' || v_count, v_count = 30 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_ADDUT_AD00985() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check number of cloratore for AD00985
    select counter INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete = 'PAAADD00000000005181';
    perform test_assertTrue('populate_STATS_CLORATORE_DISTR (AD00985): expected 2 but found ' || v_count, v_count = 2 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_ADDUT_AD00986() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check number of cloratore for AD00986
    select counter INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete = 'PAAADD00000000005188';
    perform test_assertTrue('populate_STATS_CLORATORE_DISTR (AD00986): expected 1 but found ' || v_count, v_count = 1 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_ADDUT_AD00229() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check number of cloratore for AD00229
    select counter INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete = 'PAAADD00000000005184';
    perform test_assertTrue('populate_STATS_CLORATORE_DISTR (AD00229): expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
-- -- Il test e' stato commentato in quanto sul database non risulta presente il record.
-- -- select codice_ato
-- -- from dbiait_analysis.acq_adduttrice
-- -- where codice_ato = 'AD00914'
--CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_ADDUT_AD00914() returns void as $$
--DECLARE
--    v_count BIGINT:=0;
--BEGIN
--    -- Check number of cloratore for AD00914
--    select counter INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete = '??????????????????';
--    perform test_assertTrue('populate_STATS_CLORATORE_DISTR (AD00914): expected 3 but found ' || v_count, v_count = 3 );
--END;
--$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_DISTR() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check total number of records in the support table
    select count(0) INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete LIKE 'PAARDI%' and counter > 0;
    select count(0) INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete LIKE 'DI%';
    perform test_assertTrue('populate_STATS_CLORATORE_DISTR: total expected 6 but found ' || v_count, v_count = 6 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_STATS_CLORATORE_DISTR_DI00914() returns void as $$
DECLARE
    v_count BIGINT:=0;
BEGIN
    -- Check number of cloratore for DI00914
    select counter INTO v_count from dbiait_analysis.STATS_CLORATORE WHERE id_rete = 'PAARDI00000000001409';
    perform test_assertTrue('populate_STATS_CLORATORE_DISTR (DI00914): expected 3 but found ' || v_count, v_count = 3 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_UBIC_F_ALLACCIO_PAAUCO00000002029321() returns void as $$
DECLARE
    v_idrete VARCHAR2(32);
    v_expected VARCHAR2(32) := 'PAFRRC00000000001403';
BEGIN
    select fgn_idrete into v_idrete from dbiait_analysis.UBIC_F_ALLACCIO
    where id_ubic_contatore = 'PAAUCO00000002029321';
    perform test_assertTrue(
        'test_case_UBIC_F_ALLACCIO_PAAUCO00000002029321: expected ' || v_expected || ' but found ' || v_idrete,
        v_idrete = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_UBIC_F_ALLACCIO_PAAUCO00000002029178() returns void as $$
DECLARE
    v_idrete VARCHAR2(32);
    v_expected VARCHAR2(32) := 'PAFRRC00000000001404';
BEGIN
    select fgn_idrete into v_idrete from dbiait_analysis.UBIC_F_ALLACCIO
    where id_ubic_contatore = 'PAAUCO00000002029178';
    perform test_assertTrue(
        'test_case_UBIC_F_ALLACCIO_PAAUCO00000002029178: expected ' || v_expected || ' but found ' || v_idrete,
        v_idrete = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_UBIC_F_ALLACCIO_PAAUCO00000002027995() returns void as $$
DECLARE
    v_idrete VARCHAR2(32);
    v_expected VARCHAR2(32) := 'PAFRRC00000000001264';
BEGIN
    select fgn_idrete into v_idrete from dbiait_analysis.UBIC_F_ALLACCIO
    where id_ubic_contatore = 'PAAUCO00000002027995';
    perform test_assertTrue(
        'test_case_UBIC_F_ALLACCIO_PAAUCO00000002027995: expected ' || v_expected || ' but found ' || v_idrete,
        v_idrete = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_PAACON00000000755126() returns void as $$
DECLARE
    v_count INTEGER;
    v_expected INTEGER := 36;
BEGIN
    select utenze_mis INTO v_count from dbiait_analysis.acq_shape where ids_codi_1 = 'PAACON00000000755126';
    perform test_assertTrue(
        'test_case_acq_shape_utenze_mis_PAACON00000000755126: expected ' || v_expected || ' but found ' || v_count,
        v_count = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_utenze_mis_NULL() returns void as $$
DECLARE
    v_count INTEGER;
BEGIN
    select count(0) INTO v_count from dbiait_analysis.acq_shape where UTENZE_MIS is null;
    perform test_assertTrue(
        'test_case_acq_shape_utenze_mis_NULL: expected 0 but found ' || v_count,
        v_count = 0
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_allacci_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.acq_shape
    where allacci IS NULL;
    perform test_assertTrue('ACQ_SHAPE:ALLACCI NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_shape_lunghezza_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.acq_shape
    where lunghezza_ IS NULL;
    perform test_assertTrue('ACQ_SHAPE:LUNGHEZZA_ NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_fgn_shape_allacci_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.fgn_shape
    where allacci IS NULL;
    perform test_assertTrue('FGN_SHAPE: ALLACCI NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_fgn_shape_lunghezza_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.fgn_shape
    where lunghezza_ IS NULL;
    perform test_assertTrue('FGN_SHAPE: LUNGHEZZA_ NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_adduttrici_nr_utenze_dirette() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=1485;
BEGIN

    select sum(nr_utenze_dirette) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAAAD%'; --Adduttrici
    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAAAD): nr_utenze_dirette, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_adduttrici_nr_utenze_indir_indirette() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=2021;
BEGIN

    select sum(nr_utenze_indir_indirette) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAAAD%'; --Adduttrici

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAAAD): nr_utenze_indir_indirette, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_adduttrici_nr_utenze_indir_domestici() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=1776;
BEGIN

    select sum(nr_utenze_indir_domestici) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAAAD%'; --Adduttrici

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAAAD): nr_utenze_indir_domestici, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_adduttrici_nr_utenze_indir_residente() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=1573;
BEGIN

    select sum(nr_utenze_indir_residente) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAAAD%'; --Adduttrici

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAAAD): nr_utenze_indir_residente, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_distribuzioni_nr_utenze_dirette() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=394034;
BEGIN

    select sum(nr_utenze_dirette) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAARD%'; --Rete Distribuzione

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAARD): nr_utenze_dirette, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_distribuzioni_nr_utenze_indir_indirette() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=682766;
BEGIN

    select sum(nr_utenze_indir_indirette) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAARD%'; --Rete Distribuzione

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAARD): nr_utenze_indir_indirette, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_distribuzioni_nr_utenze_indir_domestici() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=621868;
BEGIN

    select sum(nr_utenze_indir_domestici) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAARD%'; --Rete Distribuzione

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAARD): nr_utenze_indir_domestici, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_utenze_distribuzioni_nr_utenze_indir_residente() returns void as $$
DECLARE
  v_count BIGINT:=0;
  v_expected BIGINT:=509484;
BEGIN

    select sum(nr_utenze_indir_residente) INTO v_count
    from dbiait_analysis.utenze_distribuzioni_adduttrici
    where id_rete like 'PAARD%'; --Rete Distribuzione

    perform test_assertTrue('utenze_distribuzioni_adduttrici (PAARD): nr_utenze_indir_residente, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_fgn_shape_allacci_in_not_null() returns void as $$
DECLARE
  v_count       BIGINT:=0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.fgn_shape
    where allacci_in IS NULL;
    perform test_assertTrue('FGN_SHAPE: ALLACCI_IN NOT NULL, expected 0 but found ' || v_count, v_count = 0 );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_acq_cond_altro_allacci_NULL() returns void as $$
DECLARE
    v_count INTEGER;
    v_expected INTEGER := 0;
BEGIN
    select count(0) INTO v_count
    from dbiait_analysis.ACQ_COND_ALTRO
    where nr_allacci_sim is null
       or lu_allacci_sim is null
       or nr_allacci_ril is null
       or lu_allacci_ril is null;
    perform test_assertTrue(
        'test_case_acq_cond_altro_allacci_NULL: expected ' || v_expected || ' but found ' || v_count,
        v_count = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_fgn_cond_altro_allacci_NULL() returns void as $$
DECLARE
    v_count INTEGER;
    v_expected INTEGER := 0;
BEGIN
    select COUNT(0) INTO v_count
    from dbiait_analysis.fgn_cond_altro
    where lu_allacci_c is null
        or lu_allacci_c_ril is null
        or lu_allacci_i is null
        or lu_allacci_i_ril is null
        or nr_allacci_c is null
        or nr_allacci_c_ril is null
        or nr_allacci_i is null
        or nr_allacci_i_ril is null;
    perform test_assertTrue(
        'test_case_fgn_cond_altro_allacci_NULL: expected ' || v_expected || ' but found ' || v_count,
        v_count = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_schema_acq_AD01093() returns void as $$
DECLARE
    v_code VARCHAR(128);
    v_denom VARCHAR(255);
    v_exp_code VARCHAR(128) := 'DI01165;DI01166';
    v_exp_denom VARCHAR(255) := 'CASOLE;LE MASSE';
BEGIN
    SELECT codice_schema_acq, denominazione_schema_acq
    INTO v_code, v_denom
    FROM dbiait_analysis.schema_acq
    WHERE idgis IN (
        select idgis
        from dbiait_analysis.acq_adduttrice
        where codice_ato = 'AD01093'
    );
    perform test_assertTrue(
        'test_case_schema_acq_AD01093: expected (code) ' || v_exp_code || ' but found ' || v_code,
        v_code = v_exp_code
    );
    perform test_assertTrue(
        'test_case_schema_acq_AD01093: expected (denom) ' || v_exp_denom || ' but found ' || v_denom,
        v_denom = v_exp_denom
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_depuratori_ab_tr_vol_civ_DE00089() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 280295;
BEGIN
    select vol_civ::INTEGER INTO v_value
    from dbiait_analysis.abitanti_trattati where idgis in (
       select idgis from dbiait_analysis.fgn_trattamento where codice_ato = 'DE00089'
    );
    perform test_assertTrue(
        'test_case_depuratori_ab_tr_vol_civ_DE00089: expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
--------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_depuratori_ab_tr_vol_ind_DE00089() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 1841;
BEGIN
    select vol_ind::INTEGER INTO v_value
    from dbiait_analysis.abitanti_trattati where idgis in (
       select idgis from dbiait_analysis.fgn_trattamento where codice_ato = 'DE00089'
    );
    perform test_assertTrue(
        'test_case_depuratori_ab_tr_vol_ind_DE00089: expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_depurato_incoll_DE00078() returns void as $$
DECLARE
    v_value VARCHAR(255);
    v_expected VARCHAR(255) := 'CL00143;CL00144;CL00154';
BEGIN
    SELECT string_agg(ids_codice_collettore, ';')
    INTO v_value
    FROM(
        select ids_codice, ids_codice_collettore
        from dbiait_analysis.DEPURATO_INCOLL
        where ids_codice = 'DE00078'
        order by ids_codice_collettore
    ) t
    group by t.ids_codice;
    perform test_assertTrue(
        'test_case_depurato_incoll_DE00078: expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_pop_res() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 1896;
BEGIN
    select pop_res INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (pop_res): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_anno_rif() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 2019;
BEGIN
    select anno_rif INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (anno_rif): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_data_rif() returns void as $$
DECLARE
    v_value VARCHAR(10);
    v_expected VARCHAR(10) := '2019_08_31';
BEGIN
    select TO_CHAR(data_rif,'YYYY_MM_DD') INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (data_rif): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_pop_ser_dep() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 19;
BEGIN
    select pop_ser_dep INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (pop_ser_dep): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_pop_ser_fgn() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 1042;
BEGIN
    select pop_ser_fgn INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (pop_ser_fgn): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_pop_ser_acq() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 1526;
BEGIN
    select pop_ser_acq INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (pop_ser_acq): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_perc_acq() returns void as $$
DECLARE
    v_value NUMERIC;
    v_expected NUMERIC := 80.46;
BEGIN
    select perc_acq INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (perc_acq): expected ' || v_expected || ' but found ' || v_value,
        v_value BETWEEN v_expected - 0.01 AND v_expected + 0.01
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_perc_fgn() returns void as $$
DECLARE
    v_value NUMERIC;
    v_expected NUMERIC := 54.96;
BEGIN
    select perc_fgn INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (perc_fgn): expected ' || v_expected || ' but found ' || v_value,
        v_value BETWEEN v_expected - 0.01 AND v_expected + 0.01
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_perc_dep() returns void as $$
DECLARE
    v_value NUMERIC;
    v_expected NUMERIC := 0.98;
BEGIN
    select perc_dep INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (perc_dep): expected ' || v_expected || ' but found ' || v_value,
        v_value BETWEEN v_expected - 0.01 AND v_expected + 0.01
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_ut_abit_tot() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 1090;
BEGIN
    select ut_abit_tot INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (ut_abit_tot): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_ut_abit_fgn() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 750;
BEGIN
    select ut_abit_fgn INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (ut_abit_fgn): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_pop_res_comune_LONDA_ut_abit_dep() returns void as $$
DECLARE
    v_value INTEGER;
    v_expected INTEGER := 19;
BEGIN
    select ut_abit_dep INTO v_value
    from POP_RES_COMUNE where pro_com = '48025';
    perform test_assertTrue(
        'test_case_pop_res_comune_LONDA (ut_abit_dep): expected ' || v_expected || ' but found ' || v_value,
        v_value = v_expected
    );
END;
$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit;
-----------------------------------------------------------------------------------------