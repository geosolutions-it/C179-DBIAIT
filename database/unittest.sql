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

END;
$$ LANGUAGE plpgsql;

select dbiait_analysis.test_populate_lung_rete_fgn();
select dbiait_analysis.test_populate_stats_cloratore();
