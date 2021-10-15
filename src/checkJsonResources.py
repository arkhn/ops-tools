import click
import sys
import requests
import json


@click.command()
@click.option("--db_url", required=True, envvar="TARGET_DB_URL", help="url of the target db")
@click.option("--token", required=False, envvar="TOKEN", help="Authorization token")
@click.option(
    "--requestfile",
    required=False,
    envvar="REQUESTFILE",
    help="Name of the file containing the requests",
    default="requests.json",
)
def main(db_url, token, requestfile):
    """Check if the db contains resources for all json requests indicated"""

    headers = {"Authorization": token}

    with open(requestfile) as json_file:
        json_object = json.load(json_file)
        for req in json_object["requests"]:
            r = requests.get(f"{db_url}/{req}&_summary=count", headers=headers)
            bundle = r.json()
            if bundle["total"] == 0:
                print(f"no resource found for request {req}")
            else:
                print(f"{bundle['total']} resource(s) found for request {req}")


if __name__ == "__main__":
    sys.exit(main.main())
