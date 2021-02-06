import os
import pathlib
from app.scheduler.tasks.export_definitions.config_scraper import XlsExportConfig


def get_plpgsql_test_lines(sheet, sources):
    sheet = sheet.upper()
    query = " UNION ALL ".join(sources)
    procs = []
    procs.append("CREATE OR REPLACE function dbiait_analysis.test_case_count_{0}_xls() returns void as $$ ".format(sheet))
    procs.append("DECLARE ")
    procs.append("   v_count BIGINT:=0; ")
    procs.append("   v_expected BIGINT:=dbiait_analysis._test_expected_xls_count('XLS_{0}'); ".format(sheet))
    procs.append("BEGIN ")
    procs.append("   SET search_path = public,dbiait_analysis; ")
    procs.append("   SELECT count(0) INTO v_count FROM ( ")
    procs.append("   " + query)
    procs.append("   ) t; ")
    procs.append("   SET search_path = public,pgunit; ")
    procs.append("   PERFORM test_assertTrue('count XLS {0}, expected ' || v_expected || ' but found ' || v_count, v_count = v_expected); ")
    procs.append("END; ")
    procs.append("$$  LANGUAGE plpgsql SECURITY DEFINER SET search_path = public,pgunit; ")
    return procs

root_folder = pathlib.Path(os.path.dirname(__file__)).parents[0].parents[0]
file_path = os.path.join(root_folder, "database", "unittest_xls.py")
all_sql_query = XlsExportConfig().config
with open(file_path.replace("\\", "/"), 'w') as file:
    for q in all_sql_query:
        lines = get_plpgsql_test_lines(q["sheet"], q["sql_sources"])
        file.write("-"*80 + "\n")
        for line in lines:
            file.write(line + "\n")
    file.write("-" * 80 + "\n")