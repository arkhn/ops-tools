#!/usr/bin/python
# Simple python script to delete list of resources from the API from an url

import requests
import sys
import getopt


def usage():
    print("deleteJsonResources.py -u <url> -r <request> [-t <token>]")
    print(
        "example: python deleteJsonResources.py -u http://hapi.fhir.org/baseR4 -r Patient? -t aaa"
    )
    sys.exit()


def deleteEntries(entries, url, headers):
    if len(entries) > 0:
        for entry in entries:
            requests.delete(
                f"{url}{entry['resource']['resourceType']}/{entry['resource']['id']}",
                headers=headers,
            )


def getNextLink(r):
    for i in r.json()["link"]:
        if i["relation"] == "next":
            return i["url"]


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hu:r:t:", ["url=", "request=", "token="])
    except getopt.GetoptError:
        usage()

    headers = None
    url = None
    req = None

    for opt, arg in opts:
        if opt == "-h":
            usage()
        elif opt in ("-u", "--url"):
            url = arg
            if not url.endswith("/"):
                url += "/"
        elif opt in ("-r", "--request"):
            req = arg + "&_elements=_id"
        elif opt in ("-t", "--token"):
            token = arg
            if not token.startswith("Bearer "):
                token = "Bearer " + token
            headers = {"Authorization": token}

    if url is None or req is None:
        usage()

    nextBundleUrl = f"{url}{req}"
    while nextBundleUrl:
        if headers is None:
            r = requests.get(nextBundleUrl)
        else:
            r = requests.get(nextBundleUrl, headers=headers)
        bundle = r.json()
        if bundle["resourceType"] == "OperationOutcome":
            raise SyntaxError("OperationOutcome")

        entries = bundle["entry"]
        deleteEntries(entries, url, headers)

        nextBundleUrl = getNextLink(r)


if __name__ == "__main__":
    main(sys.argv[1:])
