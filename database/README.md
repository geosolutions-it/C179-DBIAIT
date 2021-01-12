### Database
This folder contains initial SQL script to install database schemas.

### Run procedure with unittest
In order to have more control on the changes, is usefull to use the unittest developed with [pgUnit](https://github.com/adrianandrei-ca/pgunit).
Follow the instruction [here](https://github.com/adrianandrei-ca/pgunit) for the local installation

In order to install the tests, execute as script the file `unittest.sql`

Before start
--

Some preliminary actions are needed in order to run the unittests.
1) create a copy of the database, with a name like `unittest_pa`
2) run the `install.sql` and `functions.sql`  
3) Import the dump for the stabbed data
4) run the `unittest.sql`


How it works:
--
Here is an example of a `unittest` for the procedure `populate_stats_cloratore()`
The unittest is divided in 3 parts:

- Creation of a function for the procedure that we are writing
```sql
CREATE OR REPLACE function dbiait_analysis.test_populate_stats_cloratore() returns void as $$
DECLARE
  new_id varchar;
  new_count bigint;
begin
```
- Execution of the procedure
```
perform dbiait_analysis.populate_stats_cloratore();
```
- Definition of the assertions 
```
-- run the new version of the procedure
perform dbiait_analysis.populate_stats_cloratore();
--- check if the count of the selected id_rete is still the same
SELECT id_rete,counter INTO new_id, new_count FROM dbiait_analysis.stats_cloratore WHERE id_rete='PAARDI00000000001319';
perform test_assertTrue(new_id, 5 = new_count );
-- check if the total rows are the same
SELECT count(*) INTO new_count FROM DBIAIT_ANALYSIS.stats_cloratore;
perform test_assertTrue('numero totale di righe è cambiato', 43 = new_count );
```
- run the test:
```
select dbiait_analysis.test_populate_stats_cloratore();
```
Here is a complete example:

```
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

select dbiait_analysis.test_populate_stats_cloratore();
```
