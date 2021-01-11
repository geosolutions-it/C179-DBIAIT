### Database
This folder contains initial SQL script to install database schemas.

### Run procedure with unittest
In order to have more control on the changes, is usefull to use the unittest developed with [pgUnit](https://github.com/adrianandrei-ca/pgunit).
Follow the instruction [here](https://github.com/adrianandrei-ca/pgunit) for the local installation

In order to install the tests, execute the file `unittest.sql`

How it works:
--
Here is an example of a `unittest` for the procedure `populate_stats_cloratore()`
The unittest is diveded in 3 parts:

- Creation of a function for the procedure that we are writing
```sql
CREATE OR REPLACE function pgunit.test_populate_stats_cloratore() returns void as $$
DECLARE
  stab_id varchar;
  stab_count bigint;
  new_id varchar;
  new_count bigint;
begin
```
- Create the table under the schema `pgunit`

```sql
DROP TABLE IF EXISTS pgunit.STATS_CLORATORE;
CREATE table IF NOT EXISTS pgunit.STATS_CLORATORE(
    id_rete		VARCHAR(32),
    counter	bigint
);
```
- dump of the old data to a temporary table
```
INSERT INTO pgunit.stats_cloratore(id_rete, counter)
select id_rete,counter from DBIAIT_ANALYSIS.stats_cloratore;
```
- execution of the new procedure
```
perform dbiait_analysis.populate_stats_cloratore();
```
- assertions 
```
SELECT id_rete,counter INTO new_id, new_count FROM DBIAIT_ANALYCrSIS.stats_cloratore WHERE id_rete='PAARDI00000000001319';
SELECT id_rete,counter INTO stab_id, stab_count FROM pgunit.stats_cloratore WHERE id_rete='PAARDI00000000001319';
perform test_assertTrue(stab_id, stab_count = new_count );
-- check if the total rows are the same
SELECT count(*) INTO new_count FROM DBIAIT_ANALYSIS.stats_cloratore;
SELECT count(*) INTO stab_count FROM pgunit.stats_cloratore;
perform test_assertTrue('numero totale di righe è cambiato', stab_count = new_count );
```
- run the test:
```
select pgunit.test_populate_stats_cloratore();
```
Here is a complete example:

```
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
    SELECT id_rete,counter INTO new_id, new_count FROM DBIAIT_ANALYCrSIS.stats_cloratore WHERE id_rete='PAARDI00000000001319';
    SELECT id_rete,counter INTO stab_id, stab_count FROM pgunit.stats_cloratore WHERE id_rete='PAARDI00000000001319';
    perform test_assertTrue(stab_id, stab_count = new_count );
    -- check if the total rows are the same
    SELECT count(*) INTO new_count FROM DBIAIT_ANALYSIS.stats_cloratore;
    SELECT count(*) INTO stab_count FROM pgunit.stats_cloratore;
    perform test_assertTrue('numero totale di righe è cambiato', stab_count = new_count );
END;
$$ LANGUAGE plpgsql;

select pgunit.test_populate_stats_cloratore();
```