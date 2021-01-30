import getopt
import sys
import psycopg2
import os


def run_tests(database, user, password, folder, file):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host="127.0.0.1",
                                  port="5432",
                                  database=database)
    cursor = connection.cursor()
    if os.name == "nt":
        conn_str = "hostaddr=127.0.0.1 port=5432 dbname={0} user={1} password={2}".format(database, user, password)
        sql = "SELECT set_config('pgunit.dblink_conn_extra', '{0}', false);".format(conn_str)
        cursor.execute(sql)
        result = cursor.fetchone()
    sql = "SELECT status, status || ' ' || rowid || ' ' || message tap_line "
    sql += "FROM("
    sql += "SELECT"
    sql += "   case when successful then 'ok' else 'not ok' end status, "
    sql += "   ROW_NUMBER () OVER (ORDER BY test_name) rowid, "
    sql += "   test_name || ': ' || error_message || ' (' || duration || ')' message "
    sql += "   FROM pgunit.test_run_all() "
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
    jenkins_workspace = ""
    output_file = ""
    db_user = "dbiait_stub"
    db_password = ""
    database = "dbiait_stub"
    try:
        opts, args = getopt.getopt(argv, "hj:o:u:p:", ["workspace=", "output=", "user=", "password="])
    except getopt.GetoptError:
        print('test.py -j <jenkins_workspace> -o <output_file> -u <db_user> -p <db_password>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -j <jenkins_workspace> -o <output_file> -u <db_user> -p <db_password>')
            sys.exit()
        elif opt in ("-j", "--workspace"):
            jenkins_workspace = arg
        elif opt in ("-o", "--output"):
            output_file = arg
        elif opt in ("-u", "--user"):
            db_user = arg
        elif opt in ("-p", "--password"):
            db_password = arg
    run_tests(database, db_user, db_password, jenkins_workspace, output_file)


if __name__ == "__main__":
    main(sys.argv[1:])
