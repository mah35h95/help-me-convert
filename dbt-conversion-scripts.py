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


def getLayer(table_name: str) -> str:
    if (
        table_name.startswith("md_")
        or table_name.startswith("txn_")
        or table_name.__contains__("_md_")
        or table_name.__contains__("_txn_")
    ):
        return "hub"
    elif (
        table_name.startswith("dim_")
        or table_name.startswith("fact_")
        or table_name.__contains__("_dim_")
        or table_name.__contains__("_fact_")
    ):
        return "analytics"
    return ""


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


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def do_type0(table_name: str, layer: str):
    if layer == "":
        layer = getLayerFromUser(table_name)

    command = f"""cd {datT}

git checkout main
git pull
git checkout -b {userDomainName}-{table_name}

"""
    if table_name.startswith("stg_"):
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
    print(stgFile)


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
