CREATE OR REPLACE function pgunit.test_populate_stats_cloratore() returns void as $$
DECLARE
  stab_id varchar;
  stab_count bigint;
  new_id varchar;
  new_count bigint;
begin
    -- drop old test table and recreate it
	DROP TABLE IF EXISTS pgunit.STATS_CLORATORE;
	CREATE table IF NOT EXISTS pgunit.STATS_CLORATORE(
		id_rete		VARCHAR(32),
		counter	bigint
	);
    -- copy old data to the new table in pgunit schema
    INSERT INTO pgunit.stats_cloratore(id_rete, counter)
	select id_rete,counter from DBIAIT_ANALYSIS.stats_cloratore;
    -- run the new version of the procedure
	perform dbiait_analysis.populate_stats_cloratore();
    --- check if the count of the selected id_rete is still the same
    SELECT id_rete,counter INTO new_id, new_count FROM DBIAIT_ANALYSIS.stats_cloratore WHERE id_rete='PAARDI00000000001319';
    SELECT id_rete,counter INTO stab_id, stab_count FROM pgunit.stats_cloratore WHERE id_rete='PAARDI00000000001319';
    perform test_assertTrue(stab_id, stab_count = new_count );
    -- check if the total rows are the same
    SELECT count(*) INTO new_count FROM DBIAIT_ANALYSIS.stats_cloratore;
    SELECT count(*) INTO stab_count FROM pgunit.stats_cloratore;
    perform test_assertTrue('numero totale di righe è cambiato', stab_count = new_count );
END;
$$ LANGUAGE plpgsql;

select pgunit.test_populate_stats_cloratore();