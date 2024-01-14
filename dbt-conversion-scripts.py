"""
Frequently copied lines

,         operation_flag
    ,         recordstamp

,         cdc_operation_type
    ,         cdc_timestamp

,         operation_flag    as cdc_operation_type
    ,         recordstamp       as cdc_timestamp

,	    current_timestamp() as metadata_inserted_timestamp
    ,	    current_timestamp() as metadata_updated_timestamp

- name: cdc_operation_type
        data_type: string
      - name: cdc_timestamp
        data_type: timestamp

where recordstamp > '{{ min_max_cdc_timestamp.min }}' and recordstamp <= '{{min_max_cdc_timestamp.max}}'

"""
# Imports
import pandas as pd
import subprocess
import re
import yaml
import random

# Define Values
table_name = "TXN_SALES_ORDER_OPEN_ORDER_PERFORMANCE"
datT = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations"
datTr = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations-ref"
commandPath = "/temp/command.sh"
userDomainName = "gm"
emojis = "🤺"


# Function definitions
def writeToFile(filename: str, content: str):
    f = open(filename, "w", encoding="utf-8")
    if filename.__contains__(".sh"):
        f.write("#!/bin/bash\n")
    f.write(content)
    f.close()


def runScript():
    subprocess.run([commandPath], shell=True)


def getTableType(table_name: str) -> str:
    table_types_path = "table_types.xlsx"
    table_types = pd.read_excel(table_types_path, sheet_name="Sheet1")
    return table_types[table_types["table"] == table_name.upper()].table_type.item()


def isHub(table_name: str) -> bool:
    return (
        table_name.startswith("md_")
        or table_name.startswith("txn_")
        or table_name.__contains__("_md_")
        or table_name.__contains__("_txn_")
    )


def isAnalytics(table_name: str) -> bool:
    return (
        table_name.startswith("dim_")
        or table_name.startswith("fact_")
        or table_name.__contains__("_dim_")
        or table_name.__contains__("_fact_")
    )


def isStage(table_name: str) -> bool:
    return table_name.startswith("stg_")


def getLayer(table_name: str) -> str:
    if isHub(table_name):
        return "hub"
    elif isAnalytics(table_name):
        return "analytics"
    return getLayerFromUser(table_name)


def getLayerFromUser(table_name: str) -> str:
    choice = input(
        f"""Enter Layer for {table_name}:
h - hub (default value)
a - analytics
"""
    )
    match choice:
        case "h":
            return "hub"
        case "a":
            return "analytics"
        case _:
            return "hub"


def findInListOfDict(list, key: str, value: str) -> int:
    for i, dic in enumerate(list):
        if dic[key] == value:
            return i
    return -1


def readYMLFile(filename: str):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def writeFile(filename: str, fileData: str):
    with open(filename, "w") as f:
        f.write(fileData)


def readBetweenTheLine(fileContent: str, start: str, end: str) -> list[str]:
    startRe = str(re.escape(start))
    endRe = str(re.escape(end))
    lines = re.findall(startRe + "(.*)" + endRe, fileContent)
    return lines


def removeDiceTableSuffix(filename: str) -> str:
    return filename.removesuffix("_current").removesuffix("_change_hist")


def isIngestStageSource(ref: str) -> bool:
    table_keys_path = "table_keys.xlsx"
    table_keys = pd.read_excel(table_keys_path, sheet_name="Sheet2")
    tableCount = table_keys["table"].str.contains(ref).sum()
    return tableCount > 0


def isTableInYML(filename: str, name: str, ref: str) -> bool:
    sourceFile = readYMLFile(filename)
    sourceIndex = findInListOfDict(sourceFile["sources"], "name", name)
    tableIndex = findInListOfDict(
        sourceFile["sources"][sourceIndex]["tables"], "name", ref
    )
    return tableIndex != -1


def addNewTableToSource(ref: str, filename: str, name: str):
    newTable = {"name": ref}
    sourceFile = readYMLFile(filename)
    sourceIndex = findInListOfDict(sourceFile["sources"], "name", name)
    sourceFile["sources"][sourceIndex]["tables"].insert(
        random.randrange(0, len(sourceFile["sources"][sourceIndex]["tables"]) - 1),
        newTable.copy(),
    )

    with open(filename, "w") as file:
        yaml.dump(sourceFile, file, sort_keys=False, indent=4)

    ymlData = readFile(filename)
    ymlData = (
        ymlData.replace("version: 2", "version: 2\n")
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
    writeFile(filename, ymlData)


def createRefFiles(refs: list[str]):
    setOfRefs = set(refs)
    for ref in setOfRefs:
        ref = ref.replace("'", "")

        if isStage(ref) or isHub(ref) or isAnalytics(ref):
            layer = getLayer(ref)
            filePath = f"{datT}/models/raw/_sources/_{layer}_sources.yml"
            # TODO: Write to respective layer yml file
            if isStage(ref):
                name = f"{layer}_stage"
                # writeToSourceYML(filePath,name,database,schema,freshness,loaded_at_field,tableToAppend)
            else:
                name = layer
                # writeToSourceYML(filePath,name,database,schema,freshness,loaded_at_field,tableToAppend)
        else:
            ref = removeDiceTableSuffix(ref)
            if isIngestStageSource(ref):
                filename = f"{datT}/models/raw/_sources/_ingest_stage_sources.yml"
                folderPath = f"{datT}/models/raw/hana_s4_ppf/"
                name = "ingest_stage_hana_s4_ppf"

                if isTableInYML(filename, name, ref):
                    print(f"{ref} already exists")
                else:
                    addNewTableToSource(ref, filename, name)

            else:
                filePath = f"{datT}/models/raw/_sources/_ingest_stage_sources.yml"
                folderPath = f"{datT}/models/raw/dice_sources/"


def do_type0(table_name: str, layer: str):
    command = f"""cd {datT}

git checkout main
git pull
git checkout -b {userDomainName}-{table_name}

"""
    if isStage(table_name):
        command = (
            command
            + f"""cd {datT}/models/staging/{layer}/ppf/
mkdir {table_name}
cp {datTr}/models/staging/{layer}/ppf/{table_name}/* ./{table_name}

"""
        )
    else:
        command = (
            command
            + f"""cd {datT}/models/marts/{layer}/ppf/
mkdir {table_name}
cp {datTr}/models/marts/{layer}/ppf/{table_name}/* ./{table_name}

"""
        )
    command = (
        command
        + f"""cd {datT}

git add .
git commit -m "Adding in files for {table_name} {emojis}"

"""
    )
    print("Running Type 0 steps")
    writeToFile(commandPath, command)
    runScript()
    print("Type 0 script complete")


def do_type1(table_name: str, layer: str):
    command = f"""cd {datT}

git checkout main
git pull
git checkout -b {userDomainName}-{table_name}

cd {datT}/models/marts/{layer}/ppf/

mkdir {table_name}
cp {datTr}/models/marts/{layer}/ppf/{table_name}/* ./{table_name}

mkdir legacy_{table_name}
cp {datTr}/models/marts/{layer}/ppf/legacy_{table_name}/* ./legacy_{table_name}

cd {datT}

git add .
git commit -m "Adding in files for {table_name} {emojis}"

"""
    print("Running Type 1 steps")
    writeToFile(commandPath, command)
    runScript()
    print("Type 1 script complete")


def do_type2(table_name: str, layer: str):
    command = f"""cd {datT}

git checkout main
git pull
git checkout -b {userDomainName}-{table_name}

cd {datT}/models/staging/{layer}/ppf/
mkdir stg_{table_name}
cp {datTr}/models/staging/{layer}/ppf/stg_{table_name}/* ./stg_{table_name}

cd {datT}/snapshots/marts/{layer}/ppf/
mkdir {table_name}
cp {datTr}/snapshots/marts/{layer}/ppf/{table_name}/* ./{table_name}

cd {datT}/models/marts/{layer}/ppf/
mkdir legacy_{table_name}
cp {datTr}/models/marts/{layer}/ppf/legacy_{table_name}/* ./legacy_{table_name}

cd {datT}

git add .
git commit -m "Adding in files for {table_name} {emojis}"

"""
    # TODO: remove comments after test
    # print("Running Type 2 steps")
    # writeToFile(commandPath, command)
    # runScript()
    print("Type 2 script complete")

    print("Reading Type 2 stg file")
    stgPath = (
        f"{datT}/models/staging/{layer}/ppf/stg_{table_name}/stg_{table_name}_v1.sql"
    )
    stgFile = readFile(stgPath)
    refs = readBetweenTheLine(stgFile, "{{ ref(", ") }}")
    print(refs)
    createRefFiles(refs)
    sources = readBetweenTheLine(stgFile, "{{ source(", ") }}")
    print(sources)


# Processing start
table_name = table_name.lower()
print("Getting Type")
type = getTableType(table_name)
print("Getting Layer")
layer = getLayer(table_name)

match type:
    case "type0":
        print("Doing Type 0")
        do_type0(table_name, layer)
    case "type1":
        print("Doing Type 1")
        do_type1(table_name, layer)
    case "type2":
        print("Doing Type 2")
        do_type2(table_name, layer)
    case _:
        print(type)
        print("i don't know what to do")
