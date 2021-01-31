import getopt
import sys
import psycopg2
import os
_HOST = "127.0.0.1"
_PORT = "5432"
_DATABASE = "dbiait_stub"
_USER = "DBIAIT_STUB"
_SCHEMA_ANL = "DBIAIT_ANALYSIS"
_SCHEMA_UT = "PGUNIT"
_CONNECTION = None


def getConnection(password):
    global _CONNECTION
    if _CONNECTION is None:
        _CONNECTION = psycopg2.connect(user=_USER,
                                       password=password,
                                       host=_HOST,
                                       port=_PORT,
                                       database=_DATABASE)
    return _CONNECTION


def clean_stda_tables(password):
    connection = getConnection(password)
    sql = "SELECT {0}.reset_proc_stda_tables()".format(_SCHEMA_ANL)
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()


def execute_stda_procs(reset, password):
    p_reset = "TRUE" if reset else "FALSE"
    message = "Executing STDA procedures"
    if p_reset:
        message += " WITH "
    else:
        message += " WITHOUT "
    message += " reset of tables..."
    print(message)
    connection = getConnection(password)
    sql = "SELECT {0}.run_all_procs({1})".format(_SCHEMA_ANL, p_reset)
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()


def run_tests(password, folder, file):
    print("Executing All Unit Tests...")
    connection = getConnection(password)
    cursor = connection.cursor()
    if os.name == "nt":
        conn_str = "hostaddr={0} port={1} dbname={2} user={3} password={4}".format(
            _HOST, _PORT, _DATABASE, _USER, password
        )
        sql = "SELECT set_config('{0}.dblink_conn_extra', '{1}', false);".format(_SCHEMA_UT, conn_str)
        cursor.execute(sql)
        result = cursor.fetchone()
    sql = "SELECT status, status || ' ' || rowid || ' ' || message tap_line "
    sql += "FROM("
    sql += "SELECT"
    sql += "   case when successful then 'ok' else 'not ok' end status, "
    sql += "   ROW_NUMBER () OVER (ORDER BY test_name) rowid, "
    sql += "   test_name || ': ' || error_message || ' (' || duration || ')' message "
    sql += "   FROM {0}.test_run_all() ".format(_SCHEMA_UT)
    sql += "   ORDER BY test_name"
    sql += ") t;"
    cursor.execute(sql)
    rows = cursor.fetchall()

    tap_file = os.path.join(folder, file)
    with open(tap_file, 'w') as f:
        for row in rows:
            tap_line = row[1]
            if row[0] != "ok":
                s_idx = tap_line.find("DO $body$")
                e_idx = tap_line.find("end; $body$") + 11
                tap_line = row[1][0:s_idx]
                tap_line += row[1][e_idx:]
            tap_line = tap_line.replace("Error on executing:", "").replace("  ", "")
            f.write(tap_line + "\n")
        f.write("1..{0}\n".format(len(rows)))


def main(argv):
    global _USER
    jenkins_workspace = ""
    output_file = ""
    db_password = ""
    run_procs = True
    try:
        opts, args = getopt.getopt(argv, "hj:o:u:p:r:", ["workspace=", "output=", "user=", "password=", "run="])
    except getopt.GetoptError:
        print('test.py -j <jenkins_workspace> -o <output_file> -u <db_user> -p <db_password> -r <run_stda_procs>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -j <jenkins_workspace> -o <output_file> -u <db_user> -p <db_password> -r <run_stda_procs>')
            sys.exit()
        elif opt in ("-j", "--workspace"):
            jenkins_workspace = arg
        elif opt in ("-o", "--output"):
            output_file = arg
        elif opt in ("-u", "--user"):
            _USER = arg
        elif opt in ("-p", "--password"):
            db_password = arg
        elif opt in ("-r", "--run"):
            if arg.lower() != "true" or arg.lower() != "t" or arg.lower() != "y" or arg.lower() != "yes":
                run_procs = False
    if run_procs:
        execute_stda_procs(True, db_password)
    run_tests(db_password, jenkins_workspace, output_file)


if __name__ == "__main__":
    main(sys.argv[1:])
