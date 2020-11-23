# Django custom commands:

Here is a simple readme for run the django commands available for configure the local environment.
<br><br>

Import csv into postgres.
---
#### Step 1
Be confident that you already run the `database/install.sql`
#### Step 2
Download the CSVs (it's a `.rar` archive) from the GoogleDrive folder and extract the file into a folder
#### Step 3
Copy the absolute path of the folder
#### Step 4
Open the file `app/scheduler/management/commands/import_csv_in_postgres.py` with a text editor (ex: Pycharm or SublimeText3)
#### Step 5
Set the postgres connection, by changing them with your local ones ex:
```python
t_host = "localhost"  # either "localhost", a domain name, or an IP address.
t_port = "5432"  # default postgres port
t_dbname = "postgres"  # database name
t_user = "postgres"  # database access
t_pw = "password"  # database password
```
#### Step 6
Run the import processing with this command (note: you must be in the django environment)
```python 
command: 
python manange.py import_csv_in_postgres "ABSOLUTE\Path\Of\The\Folder\Of\The\CSVs"

Example:

python manange.py import_csv_in_postgres "C:\Users\user\Documents\Csv"
```
The output of from the console will be something like this:
```
C:\OSGeo4W64\apps\Python37\python.exe C:/Users/user/Documents/Projects/C179-DBIAIT/manage.py import_csv_in_postgres C:\Users\user\Documents\Csv
Import dbiait_analysis.accumuli_inadd DONE
Import dbiait_analysis.accumuli_inreti DONE
Import dbiait_analysis.addut_com_serv DONE
Import dbiait_analysis.addut_inreti DONE
Import dbiait_analysis.a_fgn_rel_prod_imm DONE
Import dbiait_analysis.a_rel_prod_cont DONE
Import dbiait_analysis.collet_com_serv DONE
Import dbiait_analysis.depurato_incoll DONE
Import dbiait_analysis.fgn_rel_prod_imm DONE
Import dbiait_analysis.fiumi_inpotab DONE
Import dbiait_analysis.fiumi_inreti DONE
Import dbiait_analysis.laghi_inpotab DONE
Import dbiait_analysis.laghi_inreti DONE
Import dbiait_analysis.pompaggi_inpotab DONE
Import dbiait_analysis.pompaggi_inserba DONE
Import dbiait_analysis.pop_res_comune DONE
Import dbiait_analysis.potab_incaptaz DONE
Import dbiait_analysis.potab_inreti DONE
Import dbiait_analysis.pozzi_inpotab DONE
Import dbiait_analysis.pozzi_inreti DONE
Import dbiait_analysis.rel_prod_cont DONE
Import dbiait_analysis.scaricato_infog DONE
Import dbiait_analysis.sorgenti_inpotab DONE
Import dbiait_analysis.sorgenti_inreti DONE
Import dbiait_analysis.utenza_sap DONE
Import completed, please check the console for errors

```