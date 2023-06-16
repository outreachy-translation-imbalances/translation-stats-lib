import csv
from functools import wraps


# TODO: document parameter types


def cached(path):
    def decorated(func):
        @wraps(func)
        def calculate():
            try:
                with open(path) as f:
                    reader = csv.DictReader(f)
                    return [row for row in reader]

            except FileNotFoundError:
                data = func()

                with open(path, "w") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)

                return data

        return calculate

    return decorated
