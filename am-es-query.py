import datetime
import sys
import json
import os.path
import sqlite3
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, NotFoundError


# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])


def allaips():
    """
    Output all AIP metadata as a dictionary that can be dumped as a JSON file
    or used input for the AIP files database
    """

    start_page = 1
    # TODO: use https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan
    # to return all docs in ES index
    items_per_page = 10000

    wildcard_query = {"query": {"query_string": {"query": "*",},}}

    try:
        results = es.search(
            body=wildcard_query,
            index="aips",
            from_=start_page - 1,
            size=items_per_page,
        )
    except RequestError:
        print("Query error")
        sys.exit()
    except NotFoundError:
        print("No results found")
        sys.exit()

    aips = []
    print("%d AIP hits" % results["hits"]["total"])
    print("%d ms search time" % results["took"])

    for result in results["hits"]["hits"]:
        # date = str(datetime.datetime.fromtimestamp(result["_source"]["created"]))
        esdoc = {
            "ES doc ID": result["_id"],
            "AM pipeline": result["_source"]["origin"],
            "AIP name": result["_source"]["name"],
            "AIP UUID": result["_source"]["uuid"],
            "AIC ID": result["_source"]["AICID"],
            "Date created": result["_source"]["created"],
            "Size": result["_source"]["size"],
            "Filepath": result["_source"]["filePath"],
        }
        aips.append(esdoc)

    return aips


def allaipfiles():
    """
    Output all AIP file metadata as a dictionary that can be dumped as a JSON file
    """

    start_page = 1
    # TODO: use https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan
    # to return all docs in ES index
    items_per_page = 10000

    wildcard_query = {"query": {"query_string": {"query": "*",},}}

    try:
        results = es.search(
            body=wildcard_query,
            index="aipfiles",
            from_=start_page - 1,
            size=items_per_page,
        )
    except RequestError:
        print("Query error")
        sys.exit()
    except NotFoundError:
        print("No results found")
        sys.exit()

    aipfiles = []
    print("%d hits" % results["hits"]["total"])
    print("%d ms search time" % results["took"])
    for result in results["hits"]["hits"]:
        filename = os.path.basename(result["_source"]["filePath"])
        filepath = os.path.dirname(result["_source"]["filePath"])
        formatName = result["_source"]["METS"]["amdSec"]["mets:amdSec_dict_list"][0][
            "mets:techMD_dict_list"
        ][0]["mets:mdWrap_dict_list"][0]["mets:xmlData_dict_list"][0][
            "premis:object_dict_list"
        ][
            0
        ][
            "premis:objectCharacteristics_dict_list"
        ][
            0
        ][
            "premis:format_dict_list"
        ][
            0
        ][
            "premis:formatDesignation_dict_list"
        ][
            0
        ][
            "premis:formatName"
        ]
        try:
            formatVersion = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatDesignation_dict_list"
            ][
                0
            ][
                "premis:formatVersion"
            ]
        except:
            formatVersion = None
        try:
            formatRegistryName = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatRegistry_dict_list"
            ][
                0
            ][
                "premis:formatRegistryName"
            ]
        except:
            formatRegistryName = None
        try:
            formatRegistryKey = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatRegistry_dict_list"
            ][
                0
            ][
                "premis:formatRegistryKey"
            ]
        except:
            formatRegistryKey = None

        esdoc = {
            "ES doc ID": result["_id"],
            "AIP UUID": result["_source"]["AIPUUID"],
            "Object UUID": result["_source"]["FILEUUID"],
            "Filepath": filepath,
            "Filename": filename,
            "Format name": formatName,
            "Format version": formatVersion,
            "Format registry key": formatRegistryKey,
            "Format registry name": formatRegistryName,
        }
        aipfiles.append(esdoc)

    return aipfiles


def allaipfilesdb():
    """
    Insert all AIPfiles and linked AIP data from the ElasticSearch index into
    a Sqlite database that used the Archivematica pipeline UUID as its name.
    """

    start_page = 1
    # TODO: use https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan
    # to return all docs in ES index
    items_per_page = 10000

    wildcard_query = {"query": {"query_string": {"query": "*",},}}

    try:
        results = es.search(
            body=wildcard_query,
            index="aipfiles",
            from_=start_page - 1,
            size=items_per_page,
        )
    except RequestError:
        print("Query error")
        sys.exit()
    except NotFoundError:
        print("No results found")
        sys.exit()

    print("%d AIP file hits" % results["hits"]["total"])
    print("%d ms search time" % results["took"])

    es_pipeline = results["hits"]["hits"][0]["_source"]["origin"]
    # only creates a new db if this one doesn't already exit
    db = sqlite3.connect("am_es_" + es_pipeline + ".db")
    cursor = db.cursor()

    # create an aips table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS aips(uuid TEXT PRIMARY KEY, pipeline TEXT, name TEST, size TEXT, dateCreated TEXT, aicId TEXT, esReadDate TEXT)"""
    )
    db.commit()

    aips = allaips()
    for aip in aips:
        # only insert aip records that don't already exist
        cursor.executemany(
            "INSERT OR IGNORE INTO aips VALUES (?,?,?,?,?,?,?)",
            [
                (
                    aip["AIP UUID"],
                    aip["AM pipeline"],
                    aip["AIP name"],
                    aip["Size"],
                    aip["Date created"],
                    aip["AIC ID"],
                    str(datetime.datetime.now()),
                )
            ],
        )
        db.commit()

    # create an aipfiles table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS aipfiles(uuid TEXT PRIMARY KEY, aipUUID TEXT, filepath TEXT, filename TEXT, formatName TEXT, formatVersion TEXT, formatRegistryKey TEXT, formatRegistryName TEXT, esReadDate, atomURL TEXT, atomSlug TEXT, atomLinkStatus TEXT, atomLinkDate TEXT, FOREIGN KEY(aipUUID) REFERENCES aip(uuid))"""
    )
    db.commit()

    for result in results["hits"]["hits"]:
        filename = os.path.basename(result["_source"]["filePath"])
        filepath = os.path.dirname(result["_source"]["filePath"])
        formatName = result["_source"]["METS"]["amdSec"]["mets:amdSec_dict_list"][0][
            "mets:techMD_dict_list"
        ][0]["mets:mdWrap_dict_list"][0]["mets:xmlData_dict_list"][0][
            "premis:object_dict_list"
        ][
            0
        ][
            "premis:objectCharacteristics_dict_list"
        ][
            0
        ][
            "premis:format_dict_list"
        ][
            0
        ][
            "premis:formatDesignation_dict_list"
        ][
            0
        ][
            "premis:formatName"
        ]
        try:
            formatVersion = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatDesignation_dict_list"
            ][
                0
            ][
                "premis:formatVersion"
            ]
        except:
            formatVersion = None
        try:
            formatRegistryName = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatRegistry_dict_list"
            ][
                0
            ][
                "premis:formatRegistryName"
            ]
        except:
            formatRegistryName = None
        try:
            formatRegistryKey = result["_source"]["METS"]["amdSec"][
                "mets:amdSec_dict_list"
            ][0]["mets:techMD_dict_list"][0]["mets:mdWrap_dict_list"][0][
                "mets:xmlData_dict_list"
            ][
                0
            ][
                "premis:object_dict_list"
            ][
                0
            ][
                "premis:objectCharacteristics_dict_list"
            ][
                0
            ][
                "premis:format_dict_list"
            ][
                0
            ][
                "premis:formatRegistry_dict_list"
            ][
                0
            ][
                "premis:formatRegistryKey"
            ]
        except:
            formatRegistryKey = None

        cursor = db.cursor()

        # only insert aipfile records that don't already exist
        cursor.executemany(
            "INSERT OR IGNORE INTO aipfiles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    result["_source"]["FILEUUID"],
                    result["_source"]["AIPUUID"],
                    filepath,
                    filename,
                    formatName,
                    formatVersion,
                    formatRegistryKey,
                    formatRegistryName,
                    str(datetime.datetime.now()),
                    None,
                    None,
                    None,
                    None,
                )
            ],
        )

        db.commit()

    db.close()
    return ()


def alltransfers():
    """
    Output all transfer metadata as a dictionary that can be dumped as a JSON file
    """
    start_page = 1
    items_per_page = 20

    wildcard_query = {"query": {"query_string": {"query": "*",},}}

    try:
        results = es.search(
            body=wildcard_query,
            index="transfers",
            from_=start_page - 1,
            size=items_per_page,
        )
    except RequestError:
        print("Query error")
        sys.exit()
    except NotFoundError:
        print("No results found")
        sys.exit()

    return results


def alltransferfiles():
    """
    Output all transfer file metadata as a dictionary that can be dumped
    as a JSON file.
    """

    start_page = 1
    items_per_page = 20

    wildcard_query = {"query": {"query_string": {"query": "*",},}}

    try:
        results = es.search(
            body=wildcard_query,
            index="transferfiles",
            from_=start_page - 1,
            size=items_per_page,
        )
    except RequestError:
        print("Query error")
        sys.exit()
    except NotFoundError:
        print("No results found")
        sys.exit()

    return results


def main():

    try:
        if sys.argv[1] == "--json":
            results = allaipfiles()
            with open("allaipfiles.json", "w") as json_file:
                json.dump(results, json_file, indent=4)

    except:
        allaipfilesdb()


if __name__ == "__main__":
    main()
