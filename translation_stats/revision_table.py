import pymysql
import csv


def make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.
    
    `replica_type` can be either "analytics" (default) or "web"."""
    assert replica_type == "web" or replica_type == "analytics"
    return pymysql.connect(
        host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
        read_default_file="~/.my.cnf",
        database=f"{wiki}_p",
        charset='utf8'
    )


def query(conn, query):
    """Execute a SQL query against the connection and return **all** the results."""
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        return data


def extract_db_names(file_path):
    """Extracts database names from the 'DB Names' column of the specified CSV file."""
    db_names = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            db_names.append(row['DB Name'])
    return db_names


def process_database(wiki):
    try:
        conn = make_connection(wiki)
        results = query(
            conn,
            """
            SELECT
                r.rev_id as revision_id,
                a.actor_name AS username,
                r.rev_timestamp as revision_timestamp,
                c.comment_text as comment
            FROM revision r
            JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
            JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id
                AND ctd.ctd_name = 'contenttranslation'
            LEFT JOIN actor a ON r.rev_actor = a.actor_id
            LEFT JOIN comment c ON r.rev_comment_id = c.comment_id
            """
        )
        conn.close()

        revision_ids = []
        usernames = []
        revision_timestamps = []
        source_wikis = []
        target_wikis = []
        comments = []

        for result in results:
            revision_ids.append(result[0])
            usernames.append(result[1].decode("utf-8"))
            revision_timestamps.append(result[2].decode("utf-8"))
            comment = result[3]
            comment = comment.decode("utf-8") if comment is not None else ''
            comment_bytes = comment.encode('utf-8')

            source_wiki = ''
            if b'[[:' in comment_bytes:
                start_index = comment_bytes.find(b'[[:') + 3
                end_index = comment_bytes.find(b':', start_index)
                if end_index != -1:
                    source_wiki = comment_bytes[start_index:end_index].decode('utf-8') + "wiki"

            target_wiki = wiki

            source_wikis.append(source_wiki)
            target_wikis.append(target_wiki)
            comments.append(comment)

        return revision_ids, usernames, revision_timestamps, source_wikis, target_wikis, comments

    except pymysql.OperationalError as e:
        print(f"Error occurred while connecting to the database: {e}")
        return [], [], [], [], [], []


def save_to_csv(file_path, revision_ids, usernames, revision_timestamps, source_wikis, target_wikis, comments):
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Revision ID', 'Username', 'Revision Timestamp', 'Source wiki', 'Target wiki', 'Comment'])

        if len(revision_ids) == 0:
            print("No results found.")
        else:
            for i in range(len(revision_ids)):
                writer.writerow([revision_ids[i], usernames[i], revision_timestamps[i], source_wikis[i], target_wikis[i], comments[i]])

    print(f"CSV file generated successfully: {file_path}")


def generate_csv_files():
    
    # Provide the path to the language_data.csv file
    csv_file_path = '/home/paws/translation-stats-data/language_data.csv'

    # Provide the output directory path where CSV files will be saved
    output_dir = '/home/paws/translation-stats-data/revision_table'
    db_names = extract_db_names(csv_file_path)

    for db_name in db_names:
        revision_ids, usernames, revision_timestamps, source_wikis, target_wikis, comments = process_database(db_name)

        output_filename = f"{output_dir}/{db_name}_revision_table.csv"
        save_to_csv(output_filename, revision_ids, usernames, revision_timestamps, source_wikis, target_wikis, comments)


