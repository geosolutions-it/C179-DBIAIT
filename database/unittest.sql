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

select dbiait_analysis.test_populate_fgn_shape();
