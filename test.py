import yaml
import random


def findInListOfDict(list, key, value):
    for i, dic in enumerate(list):
        if dic[key] == value:
            return i
    return -1


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def writeFile(filename: str, fileData: str):
    with open(filename, "w") as f:
        f.write(fileData)


filePath = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations/models/raw/_sources/_ingest_stage_sources.yml"
name = ("ingest_stage_hana_s4_ppf",)
ref = "knvp"

with open(filePath) as file:
    sourcesList = yaml.load(file, Loader=yaml.FullLoader)


sourceIndex = findInListOfDict(
    sourcesList["sources"], "name", "ingest_stage_hana_s4_ppf"
)

tableIndex = findInListOfDict(
    sourcesList["sources"][sourceIndex]["tables"], "name", ref
)

if tableIndex == -1:
    newTable = {"name": ref}

    sourcesList["sources"][sourceIndex]["tables"].insert(
        random.randrange(0, len(sourcesList["sources"][sourceIndex]["tables"]) - 1),
        newTable.copy(),
    )
    with open(filePath, "w") as file:
        sourcesListUpdated = yaml.dump(sourcesList, file, sort_keys=False, indent=4)
    fileData = readFile(filePath)
    fileData = (
        fileData.replace("version: 2", "version: 2\n")
        .replace("-  ", "  -")
        .replace(
            """freshness:
        warn_after:
            count: 30
            period: minute""",
            """freshness: # default freshness
      warn_after: { count: 30, period: minute }
      #error_after: {count: 8, period: hour}""",
        )
    )
    writeFile(filePath, fileData)
else:
    print("table exists")
