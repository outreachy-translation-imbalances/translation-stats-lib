import csv
from .wiki_replica import query


def fetch_usernames(dbnames):
    for database_name in dbnames:
        results = query(
            database_name,
            """
            SELECT
                DISTINCT r.rev_actor AS userid,
                a.actor_name AS username
            FROM revision r
            JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
            JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id
                AND ctd.ctd_name = 'contenttranslation'
            LEFT JOIN actor a ON r.rev_actor = a.actor_id
            """
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

            
def generate_csv_files():
    dbnames = []
    
    language_data = "/home/paws/translation-stats-data/language_data.csv"
    
    with open(language_data, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dbnames.append(row['DB Name'])
            
    fetch_usernames(dbnames)
