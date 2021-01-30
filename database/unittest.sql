---------------------------------------------------------------------------------------------
-- It is necessary to have a dedicated database for stub (dbiait_stub)
-- In this database:
-- CREATE SCHEMA pgunit;
-- CREATE EXTENSION DBLINK SCHEMA pgunit;
-- run PGUnit.sql (from https://github.com/adrianandrei-ca/pgunit)
---------------------------------------------------------------------------------------------

-- ------------------------------------------------------------------------------------------
-- RUN ALL TESTS:
-- SELECT * FROM pgunit.test_run_all();
--
-- RUN A SPECIFIC SUITE
-- SELECT * FROM test_run_suite('sqlexport');
-- ==========================================================================================
-- TEST POPULATE STATS CLORATORE
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_stats_cloratore() returns void as $$
DECLARE
  new_id varchar;
  new_count bigint;
begin
    -- run the new version of the procedure
	perform dbiait_analysis.populate_stats_cloratore();
    --- check if the count of the selected id_rete is still the same
    SELECT id_rete,counter INTO new_id, new_count FROM dbiait_analysis.stats_cloratore WHERE id_rete='PAARDI00000000001319';
    perform test_assertTrue(new_id, 5 = new_count );
    -- check if the total rows are the same
    SELECT count(*) INTO new_count FROM DBIAIT_ANALYSIS.stats_cloratore;
    perform test_assertTrue('numero totale di righe è cambiato', 43 = new_count );
END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;

-- ------------------------------------------------------------------------------------------
-- TEST POPULATE LUNG RETE FGN
-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_lung_rete_fgn() returns void as $$
DECLARE
  dummy_int bigint;
  dummy_string varchar;
  dummy_decimal decimal;
  dummy_decimal2 decimal;
  dummy_decimal_expected decimal;
  dummy_decimal_actual decimal;
begin
    -- run the new version of the procedure
	PERFORM dbiait_analysis.populate_lung_rete_fgn();

	-- ASSERTION TESTS START FROM HERE

    --- given a selected idgis, the value should be the expected
    SELECT lung_rete_mista, lung_rete_nera into dummy_decimal, dummy_decimal2 FROM dbiait_analysis.fgn_lunghezza_rete flr WHERE idgis ='PAFRRC00000000001908';
	PERFORM test_assertTrue('Check rete mista sia 15', dummy_decimal = 15 );
    PERFORM test_assertTrue('Check rete nera sia 20', dummy_decimal2 = 20 );

    --- given a selected idgis, the value should be the expected
    SELECT lung_rete_mista, lung_rete_nera into dummy_decimal, dummy_decimal2 FROM dbiait_analysis.fgn_lunghezza_rete flr WHERE idgis ='PAFRRC00000000001273';
	PERFORM test_assertTrue('Check rete mista sia 0', dummy_decimal = 0 );
    PERFORM test_assertTrue('Check rete nera sia 10', dummy_decimal2 = 10 );

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
CREATE OR REPLACE function dbiait_analysis.test_case_populate_fgn_shape() returns void as $$
DECLARE
  dummy_int bigint;
  dummy_string varchar;
  dummy_decimal decimal;
begin
    -- run the new version of the procedure
	PERFORM dbiait_analysis.populate_fgn_shape();

	-- ASSERTION TESTS START FROM HERE

    --- given a selected ids_codi_1, the value of sezione should be null
    SELECT sezione into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000420230';
    PERFORM test_assertNull('Check la sezione ALTRO è NULL', dummy_string );

   --- given a selected ids_codi_1, the value of sezione should be circolare
    SELECT sezione into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000368712';
    PERFORM test_assertTrue('Check la sezione CIRCOLARE esista', dummy_string = 'CIRCOLARE' );

   --- given a selected ids_codi_1, the value of comune_nom should be the pro_com and not the codice_istat
    SELECT id_comune_ into dummy_int FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000375524';
    PERFORM test_assertTrue('Check comune sia pro_com e non id_istat', dummy_int = 48017 );

   --- given a selected ids_codi_1, the value of lunghz_1 should be rounded to the 6° decimal
    SELECT lunghez_1 into dummy_decimal FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000420850';
    PERFORM test_assertTrue('Check lunghez_ rounded al 6 decimale', dummy_decimal = 0.052013 );

   --- given a selected ids_codi_1, the value of lunghz_1 should be rounded to the 6° decimal
    SELECT id_refluo_ into dummy_int FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000371107';
    PERFORM test_assertTrue('Check id_refluo_ is no longer 0', dummy_int = 1 );

   --- given a selected ids_codi_1, the value of copertura should be null in case of ASFALTO SIMILI
    SELECT copertura into dummy_string FROM dbiait_analysis.fgn_shape WHERE ids_codi_1 ='PAFCON00000000400248';
    PERFORM test_assertNull('Check la sezione ASFALTO SIMILI è NULL', dummy_string );

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
CREATE OR REPLACE function dbiait_analysis.test_case_populate_schema_acq() returns void as $$
DECLARE
  cod_schema varchar;
  denom_schema varchar;
  error varchar;

begin
    -- run the new version of the procedure
	perform dbiait_analysis.populate_schema_acq();

--- check if the output of the selected idgis is the expected
    SELECT codice_schema_acq,denominazione_schema_acq INTO cod_schema, denom_schema FROM dbiait_analysis.schema_acq sa WHERE idgis='PAARDI00000000001299';
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected 1 ma trovata ' || cod_schema , '1' = cod_schema );
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected foo_denominazione ma trovata ' || denom_schema , 'foo_denominazione' = denom_schema );

    --- check if the output of the selected idgis is the expected
    SELECT codice_schema_acq,denominazione_schema_acq INTO cod_schema, denom_schema FROM dbiait_analysis.schema_acq sa WHERE idgis='PAARDI00000000001426';
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected 2;3 ma trovata ' || cod_schema , '2;3' = cod_schema );
    perform test_assertTrue('Schema Acquedottistico denominazione schema non valida expected bar_denominazione;foobar_denominazione ma trovata ' || denom_schema , 'bar_denominazione;foobar_denominazione' = denom_schema );

END;
$$  LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = public,pgunit;

-- ------------------------------------------------------------------------------------------
CREATE OR REPLACE function dbiait_analysis.test_case_populate_ubic_allaccio() returns void as $$
DECLARE
  sn_alla varchar;
  id_rete varchar;
  new_count bigint;
begin
    -- run the new version of the procedure
	perform dbiait_analysis.populate_ubic_allaccio();
    --- check if the count of the selected id_rete is still the same
    SELECT acq_sn_alla,acq_idrete into sn_alla,id_rete FROM dbiait_analysis.ubic_allaccio ua WHERE id_ubic_contatore ='PAAUCO00000001907206';
    perform test_assertTrue('ID_rete wrong, expected PAARDI00000000001511 but found ' || id_rete, 'PAARDI00000000001511' = id_rete );
    perform test_assertTrue('sn_alla wrong, expected SI but found ' || sn_alla, 'SI' = sn_alla );
    --- check if the count of the selected id_rete is still the same
    SELECT acq_sn_alla,acq_idrete into sn_alla,id_rete FROM dbiait_analysis.ubic_allaccio ua WHERE id_ubic_contatore ='PAAUCO00000002073907';
    perform test_assertTrue('ID_rete wrong, expected PAARDI00000000001511 but found <' || id_rete || '>', 'PAARDI00000000001511' = id_rete );
    perform test_assertTrue('sn_alla wrong, expected SI but found ' || sn_alla, 'NO' = sn_alla );
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
  v_x_exp DOUBLE PRECISION:=1705469.68;
  v_y_exp DOUBLE PRECISION:=4830687.91;
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
CREATE OR REPLACE function dbiait_analysis._test_expected_count(v_table IN VARCHAR) returns INTEGER as $$
DECLARE
    v_count INTEGER;
BEGIN
    v_count := ('{
        "POP_RES_LOC":          			1072,
        "DISTRIB_LOC_SERV":     			1430,
        "POP_RES_COMUNE":       			54,
        "DISTRIB_COM_SERV":     			451,
        "UTENZA_SERVIZIO":      			401650,
        "UTENZA_SERVIZIO_LOC":  			425970,
        "UTENZA_SERVIZIO_ACQ":  			425916,
        "UTENZA_SERVIZIO_FGN":  			380435,
        "UTENZA_SERVIZIO_BAC":  			281618,
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
        "ACQ_ALLACCIO": 					221859,
        "ACQ_LUNGHEZZA_ALLACCI": 			971,
        "SUPPORT_ACQ_ALLACCI": 				231228,
        "FGN_ALLACCIO": 					195189,
        "FGN_LUNGHEZZA_ALLACCI": 			1092,
        "FGN_LUNGHEZZA_ALLACCI_id_rete":	0,
        "SUPPORT_FGN_ALLACCI": 				195858,
        "ACQ_SHAPE": 						119262,
        "ACQ_SHAPE_A": 						10443,
        "ACQ_SHAPE_D": 						108819,
        "ACQ_VOL_UTENZE": 					244,
        "FGN_SHAPE": 						69246,
        "FGN_SHAPE_F": 						63233,
        "FGN_SHAPE_C": 						6013,
        "FGN_VOL_UTENZE": 					935,
        "STATS_POMPE": 						1413,
        "POZZI_POMPE": 						0,
        "POTAB_POMPE": 						0,
        "POMPAGGI_POMPE": 					0,
        "SOLLEV_POMPE": 					0,
        "DEPURATO_POMPE": 					0,
        "ADDUT_COM_SERV":		 			0,
        "COLLET_COM_SERV": 					0,
        "FIUMI_INRETI": 					0,
        "LAGHI_INRETI": 					0,
        "POZZI_INRETI": 					0,
        "SORGENTI_INRETI": 					0,
        "POTAB_INRETI": 					0,
        "ADDUT_INRETI": 					0,
        "ACCUMULI_INRETI": 					0,
        "ACCUMULI_INADD": 					0,
        "DEPURATO_INCOLL": 					0,
        "SCARICATO_INFOG": 					0,
        "ACQ_CONDOTTA_NODES": 				0,
        "ACQ_CONDOTTA_EDGES": 				0,
        "FGN_CONDOTTA_NODES": 				0,
        "FGN_CONDOTTA_EDGES": 				0,
        "STATS_CLORATORE": 					0,
        "SCHEMA_ACQ": 						0,
        "UBIC_ALLACCIO": 					0,
        "UBIC_CONTATORI_CASS_CONT": 		0,
        "UTENZE_DISTRIBUZIONI_ADDUTTRICI": 	0,
        "UBIC_CONTATORI_FGN": 				0,
        "UBIC_F_ALLACCIO": 					0,
        "UTENZE_FOGNATURE_COLLETTORI": 		0,
        "SUPPORT_CODICE_CAPT_ACCORP": 		0
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

