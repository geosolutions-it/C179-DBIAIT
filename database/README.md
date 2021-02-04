### Database
This folder contains initial SQL script to install database schemas.

Unittest for procedure
--

Some preliminary actions are needed in order to run the unittests.
1) create a copy of the database, with a name like `dbiait_stub`
2) run the `install.sql` and `functions.sql`  
3) Import the dump for the stabbed data
4) Create a specific schema for the pgunit framework
> CREATE SCHEMA pgunit;
5) Create the `dblink` extension
> CREATE EXTENSION DBLINK SCHEMA pgunit;
6) Give to the test user (eg dbiait_stub) the following permissions:
> GRANT EXECUTE ON FUNCTION dblink_connect_u(text) TO dbiait_stub;
> GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO dbiait_stub;
7) install [pgUnit](https://github.com/adrianandrei-ca/pgunit).
8) Import the `unittest.sql`
9) To test all procedures execute the following SQL statement:
> SELECT * FROM pgunit.test_run_all();
10) To run a specific suite (eg sqlexport => all procedures starting with name test_case_sqlexport*)
> SELECT * FROM test_run_suite('sqlexport');

---------------------------------------------------------------------------------------------

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
```sql
perform dbiait_analysis.populate_stats_cloratore();
```
- Definition of the assertions 
```sql
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
```sql
select dbiait_analysis.run_unittests();
```
Here is a complete example:

```sql
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

select dbiait_analysis.run_unittests();
```

Add a new unittest:
--
In order to add a new unit test, is required to:
- Create the procedure of the test and paste it at the end of the file (just before the `dbiait_analysis.run_unittests`)
- Add at the beginning of the file in the procedure named `dbiait_analysis.run_unittests()` a new row the the `perform`
 need to run the procedure
 - run all the unittests with `select dbiait_analysis.run_unittests();`
 