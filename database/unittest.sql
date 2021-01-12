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
