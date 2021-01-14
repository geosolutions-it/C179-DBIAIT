CREATE OR REPLACE function dbiait_analysis.test_populate_stats_cloratore() returns void as $$
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
$$ LANGUAGE plpgsql;

CREATE OR REPLACE function dbiait_analysis.test_populate_lung_rete_fgn() returns void as $$
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
$$ LANGUAGE plpgsql;

CREATE OR REPLACE function dbiait_analysis.test_populate_fgn_shape() returns void as $$
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
$$ LANGUAGE plpgsql;

select dbiait_analysis.test_populate_lung_rete_fgn();
select dbiait_analysis.test_populate_stats_cloratore();
select dbiait_analysis.test_populate_fgn_shape();
