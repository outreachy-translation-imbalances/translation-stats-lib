import csv
import pymysql


def make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.
    
    `replica_type` can be either "analytics" (default), or "web"."""
    assert replica_type == "web" or replica_type == "analytics"
    return pymysql.connect(
        host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
        read_default_file="~/.my.cnf",
        database=f"{wiki}_p",
        charset='utf8'
    )


def query(conn, query):
    """Execute a SQL query against the connection, and return **all** the results."""
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        return data


def fetch_usernames(dbnames):
    for database_name in dbnames:
        try:
            conn = make_connection(database_name)
            results = query(
                conn,
                "SELECT DISTINCT r.rev_actor AS userid, a.actor_name AS username FROM revision r JOIN change_tag ct ON r.rev_id = ct.ct_rev_id JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id AND ctd.ctd_name = 'contenttranslation' LEFT JOIN actor a ON r.rev_actor = a.actor_id"
            )

            print(f"Number of rows in results for {database_name}: {len(results)}")

            user_ids = []
            usernames = []

            for result in results:
                user_ids.append(result[0])
                usernames.append(result[1].decode("utf-8"))

            with open(f'/home/paws/translation-stats-data/translator_usernames/{database_name}_usernames.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['User ID', 'Username'])

                if len(user_ids) == 0:
                    print("No results found.")
                else:
                    for i in range(len(user_ids)):
                        writer.writerow([user_ids[i], usernames[i]])

            print(f"CSV file for {database_name} generated successfully.")

            conn.close()
        except pymysql.Error as e:
            print(f"Error occurred while processing {database_name}: {e}")
            
def generate_csv_files():
    dbnames = []
    
    language_data = "/home/paws/translation-stats-data/language_data.csv"
    
    with open(language_data, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dbnames.append(row['DB Name'])
            
    fetch_usernames(dbnames)
