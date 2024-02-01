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
table_name = ""
userDomainName = "Your-Domain-Name"
emojis = "🌈📁"
fromBranch = "utf_baseline_fix"
datT = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations"
datTr = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations-ref"
commandPath = "C:/Users/GM/Documents/local_dev/help-me-convert/temp/convert.sh"


# Function definitions
def getCheckoutStuff(table_name: str) -> str:
    checkout_stuff = f"""cd {datTr}
git checkout main
git pull
git checkout {fromBranch}

cd {datT}
git checkout main
git pull
git checkout {userDomainName}-{table_name} || git checkout -b {userDomainName}-{table_name}
"""
    return checkout_stuff


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
Layer: """
    )
    match choice:
        case "h":
            return "hub"
        case "a":
            return "analytics"
        case _:
            return "hub"


def getIngestLayerFromUser(table_name: str) -> str:
    choice = input(
        f"""Enter Ingest Layer for {table_name}:
i - ingest stage (default value)
d - datalake
Ingest Layer: """
    )
    match choice:
        case "i":
            return "i"
        case "d":
            return "d"
        case _:
            return "i"


def getTableTypeFromUser(table_name: str, layer: str):
    choice = input(
        f"""Enter Type for {table_name}:
0 - type0
1 - type1
2 - type2
Type: """
    )
    type = "nan"
    match choice:
        case "0":
            type = "type0"
        case "1":
            type = "type1"
        case "2":
            type = "type2"
        case _:
            print("what input is this???")

    doType(table_name, type, layer)


def collectRefs(ref: str):
    dependantRefs = readFile("dependantRefs.txt")
    dependantRefs = f"{dependantRefs}{ref}\n"
    writeToFile("dependantRefs.txt", dependantRefs)


def clearRefs():
    writeToFile("dependantRefs.txt", "")


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


def readBetweenTheLine(fileContent: str, start: str, end: str) -> list[str]:
    startRe = str(re.escape(start))
    endRe = str(re.escape(end))
    lines = re.findall(startRe + "(.*)" + endRe, fileContent)
    return lines


def removeDiceTableSuffix(name: str) -> str:
    return name.removesuffix("_current").removesuffix("_change_hist")


def isIngestStageSource(ref: str) -> bool:
    table_keys_path = "table_keys.xlsx"
    table_keys = pd.read_excel(table_keys_path, sheet_name="Sheet2")
    tableCount = table_keys["table"].str.contains(ref).sum()
    return tableCount > 0


def getTableKeys(table_name: str) -> str:
    table_keys_path = "table_keys.xlsx"
    table_keys = pd.read_excel(table_keys_path, sheet_name="Sheet2")
    return table_keys[table_keys["table"] == table_name.lower()].table_keys.item()


def isTableInYML(filename: str, name: str, ref: str) -> bool:
    sourceFile = readYMLFile(filename)
    sourceIndex = findInListOfDict(sourceFile["sources"], "name", name)
    if sourceIndex == -1:
        return False
    tableIndex = findInListOfDict(
        sourceFile["sources"][sourceIndex]["tables"], "name", ref
    )
    return tableIndex != -1


def getRandomPosition(listLength: int) -> int:
    randIndex = 0
    if listLength - 1 > 0:
        randIndex = random.randrange(0, listLength - 1)
    elif listLength == 1:
        randIndex = 1
    return randIndex


def addNewTableToSourceYML(
    tableToAppend: str,
    filename: str,
    name: str,
    database: str,
    schema: str,
    warnCount: int,
    warnPeriod: str,
    errCount: int,
    errPeriod: str,
    loaded_at_field: str,
):
    newTable = {"name": tableToAppend}
    sourceFile = readYMLFile(filename)
    sourceIndex = findInListOfDict(sourceFile["sources"], "name", name)
    if sourceIndex == -1:
        sourceIndex = getRandomPosition(len(sourceFile["sources"]))
        newSource = {
            "name": name,
            "database": database,
            "schema": schema,
            "freshness": {"warn_after": {"count": warnCount, "period": warnPeriod}},
            "loaded_at_field": loaded_at_field,
            "tables": [],
        }
        sourceFile["sources"].insert(sourceIndex, newSource.copy())

    sourceFile["sources"][sourceIndex]["tables"].insert(
        getRandomPosition(len(sourceFile["sources"][sourceIndex]["tables"])),
        newTable.copy(),
    )

    with open(filename, "w") as f:
        yaml.dump(sourceFile, f, sort_keys=False, indent=4)

    ymlData = readFile(filename)
    ymlData = (
        ymlData.replace("version: 2", "version: 2\n")
        .replace("-  ", "  -")
        .replace("'''", "'")
        .replace(
            f"""freshness:
        warn_after:
            count: {warnCount}
            period: {warnPeriod}""",
            f"""freshness: # default freshness
      warn_after: {{ count: {warnCount}, period: {warnPeriod} }}
      #error_after: {{count: {errCount}, period: {errPeriod}}}""",
        )
    )
    writeToFile(filename, ymlData)


def insertStringAtIndex(data: str, stringToInsert: str, index: int) -> str:
    return data[:index] + stringToInsert + data[index:]


def addNewTableToModelYML(folderPath: str, tableName: str, columnName: str):
    filename = f"{folderPath}/_models.yml"

    unique_column_name = ""
    if columnName == "":
        unique_column_name = "<TODO: ADD Primary Keys>"
    else:
        unique_column_name = columnName.replace(", ", " || '-' || ")

    newModel = f"""- name: {tableName}
    latest_version: 1
    versions:
      - v: 1
    tests:
      - unique:
          column_name: "{unique_column_name}"
  """

    modelFile = readYMLFile(filename)
    randPos = getRandomPosition(len(modelFile["models"]))
    nameRandPos = modelFile["models"][randPos]["name"]

    searchName = f"- name: {nameRandPos}"
    ymlData = readFile(filename)
    searchMatches = re.finditer(searchName, ymlData)

    for searchMatch in searchMatches:
        ymlData = insertStringAtIndex(ymlData, newModel, searchMatch.start())

    writeToFile(filename, ymlData)


def createIngestSQLFiles(folderPath: str, ref: str, tableKeys: str):
    partitionPKs = ""
    unique_key: list[str] = []

    if tableKeys == "":
        partitionPKs = "<TODO: ADD Primary Keys>"
        unique_key = ["<TODO: ADD Primary Keys>"]
    else:
        partitionPKs = tableKeys
        unique_key = tableKeys.split(", ")

    sqlData = f"""{{{{
    config(
        materialized            =   'incremental',
        incremental_strategy    =   'merge',
        incremental_predicates  =   ["dynamic_timestamp_range", "recordstamp"],
        unique_key              =   [{"'" + "', '".join(unique_key) + "'"}],
        cluster_by              =   ['recordstamp'],  
        partition_by            =   {{
                                    "field": "recordstamp",
                                    "data_type": "timestamp",
                                    "granularity": "day"
                                }}
    )
}}}}

select  
    *,
    current_timestamp() as metadata_inserted_timestamp,
    current_timestamp() as metadata_updated_timestamp,  
    case
        when operation_flag = 'D' then 1
        when operation_flag = 'U' then 2
        when operation_flag = 'I' then 3
    else 4
    end as operation_rank
from {{{{ source('ingest_stage_hana_s4_ppf', '{ref}') }}}}

{{% if is_incremental() %}}
    {{% set min_max_timestamp_query_result = get_incremental_timestamp('recordstamp', '120') %}} 
    {{% if min_max_timestamp_query_result.min != None %}}
    for SYSTEM_TIME as of TIMESTAMP_MILLIS(UNIX_MILLIS(TIMESTAMP '{{{{min_max_timestamp_query_result.max}}}}'))
    -- this filter will only be applied on an incremental run
    where recordstamp >= '{{{{ min_max_timestamp_query_result.min }}}}' and recordstamp < '{{{{min_max_timestamp_query_result.max}}}}'
    {{% endif %}}
{{% endif %}}

QUALIFY ROW_NUMBER() OVER (PARTITION BY {partitionPKs} ORDER BY recordstamp DESC, operation_rank ASC) = 1 
"""
    filename = f"{folderPath}/{ref}_current_v1.sql"
    writeToFile(filename, sqlData)


def createDatalakeSQLFiles(folderPath: str, ref: str, schemaName: str):
    sqlData = f"""{{{{
    config(
        materialized            =   'incremental',
        incremental_strategy    =   'merge',
        incremental_predicates  =   ["dynamic_timestamp_range", "DICE_CHANGE_SOURCE_WATERMARK"],
        unique_key              =   ['<TODO: ADD Primary Keys>'],
        cluster_by              =   ['DICE_CHANGE_SOURCE_WATERMARK'],  
        partition_by            =   {{
                                    "field": "DICE_CHANGE_SOURCE_WATERMARK",
                                    "data_type": "timestamp",
                                    "granularity": "day"
                                }}
    )
}}}}

select  
    *,
    current_timestamp() as metadata_inserted_timestamp,
    current_timestamp() as metadata_updated_timestamp,  
    case
        when DICE_CHANGE_INDICATOR = 'D' then 1
        when DICE_CHANGE_INDICATOR = 'U' then 2
        when DICE_CHANGE_INDICATOR = 'I' then 3
    else 4
    end as operation_rank
from {{{{ source('{schemaName}', '{ref}_change_hist') }}}}

{{% if is_incremental() %}}
    {{% set min_max_timestamp_query_result = get_incremental_timestamp('DICE_CHANGE_SOURCE_WATERMARK', '120') %}} 
    {{% if min_max_timestamp_query_result.min != None %}}
    for SYSTEM_TIME as of TIMESTAMP_MILLIS(UNIX_MILLIS(TIMESTAMP '{{{{min_max_timestamp_query_result.max}}}}'))
    -- this filter will only be applied on an incremental run
    where DICE_CHANGE_SOURCE_WATERMARK >= '{{{{ min_max_timestamp_query_result.min }}}}' and DICE_CHANGE_SOURCE_WATERMARK < '{{{{min_max_timestamp_query_result.max}}}}'
    {{% endif %}}
{{% endif %}}

QUALIFY ROW_NUMBER() OVER (PARTITION BY <TODO: ADD Primary Keys> ORDER BY DICE_CHANGE_SOURCE_WATERMARK DESC, operation_rank ASC) = 1 
"""
    filename = f"{folderPath}/{ref}_current_v1.sql"
    writeToFile(filename, sqlData)


def addNewLineToIngestSQLFile(ref: str):
    try:
        folderPath = f"{datT}/models/raw/hana_s4_ppf"
        filename = f"{folderPath}/{ref}_current_v1.sql"

        sqlData = readFile(filename)
        sqlData = f"{sqlData}\n\n"

        writeToFile(filename, sqlData)
    except:
        print(f"{ref} could not be found")


def createIngestRefFiles(ref: str, filename: str, name: str, tableKeys: str):
    folderPath = f"{datT}/models/raw/hana_s4_ppf"
    database = """'{{env_var("DBT_SOURCE_INGEST_STAGE_GCP_PROJECT")}}'"""
    schema = "S4HANA"
    warnCount = 30
    warnPeriod = "minute"
    errCount = 8
    errPeriod = "hour"
    loaded_at_field = "recordstamp"
    addNewTableToSourceYML(
        ref,
        filename,
        name,
        database,
        schema,
        warnCount,
        warnPeriod,
        errCount,
        errPeriod,
        loaded_at_field,
    )
    tableName = f"{ref}_current"
    addNewTableToModelYML(folderPath, tableName, tableKeys)
    createIngestSQLFiles(folderPath, ref, tableKeys)


def addNewLineToDatalakeSQLFile(ref: str):
    folderPath = f"{datT}/models/raw/dice_sources"
    filename = f"{folderPath}/{ref}_current_v1.sql"

    sqlData = readFile(filename)
    sqlData = f"{sqlData}\n\n"

    writeToFile(filename, sqlData)


def createDatalakeRefFiles(
    ref: str, schemaName: str, filename: str, name: str, tableKeys: str
):
    folderPath = f"{datT}/models/raw/dice_sources"
    database = """'{{env_var("DBT_SOURCE_LAKE_GCP_PROJECT")}}'"""
    schema = schemaName
    warnCount = 24
    warnPeriod = "hour"
    errCount = 48
    errPeriod = "hour"
    loaded_at_field = "dice_change_source_watermark"
    currentName = f"{ref}_current"
    addNewTableToSourceYML(
        currentName,
        filename,
        name,
        database,
        schema,
        warnCount,
        warnPeriod,
        errCount,
        errPeriod,
        loaded_at_field,
    )
    # tableName = f"{ref}_current"
    # addNewTableToModelYML(folderPath, tableName, tableKeys)
    # createDatalakeSQLFiles(folderPath, ref, schemaName)


def createRefFiles(refs: list[str]):
    setOfRefs = set(refs)
    for ref in setOfRefs:
        ref = ref.replace("'", "")

        if isStage(ref) or isHub(ref) or isAnalytics(ref):
            collectRefs(ref)
        else:
            ref = removeDiceTableSuffix(ref)
            filename = f"{datT}/models/raw/_sources/_ingest_stage_sources.yml"
            name = "ingest_stage_hana_s4_ppf"
            if isIngestStageSource(ref):
                if isTableInYML(filename, name, ref):
                    addNewLineToIngestSQLFile(ref)
                else:
                    tableKeys = getTableKeys(ref)
                    createIngestRefFiles(ref, filename, name, tableKeys)

            else:
                #! No Keys
                choice = getIngestLayerFromUser(ref)
                if choice == "i":
                    if isTableInYML(filename, name, ref):
                        addNewLineToIngestSQLFile(ref)
                    else:
                        createIngestRefFiles(ref, filename, name, "")
                else:
                    currentName = f"{ref}_current"
                    schemaName = input(f"Enter the schema name for {currentName}: ")
                    filename = f"{datT}/models/raw/_sources/_lake_sources.yml"
                    name = schemaName
                    if isTableInYML(filename, name, currentName):
                        # addNewLineToDatalakeSQLFile(ref)
                        pass
                    else:
                        createDatalakeRefFiles(ref, schemaName, filename, name, "")


def createSourceFiles(sources: list[str]):
    setOfSources = set(sources)
    for source in setOfSources:
        source = source.replace("'", "")
        sourceSplit = source.split(", ")
        schemaName = sourceSplit[0]
        ref = removeDiceTableSuffix(sourceSplit[1])
        filename = f"{datT}/models/raw/_sources/_lake_sources.yml"
        name = schemaName
        currentName = f"{ref}_current"
        if isTableInYML(filename, name, currentName):
            # addNewLineToDatalakeSQLFile(ref)
            pass
        else:
            createDatalakeRefFiles(ref, schemaName, filename, name, "")


def convertSourcesToRefs(filename: str, sources: list[str]):
    setOfSources = set(sources)
    for source in setOfSources:
        fromStr = f"{{{{ source({source}) }}}}"
        sourceSplit = source.split(", ")
        ref = sourceSplit[1]
        toStr = f"{{{{ ref({ref}) }}}}"

        sqlData = readFile(filename)
        sqlData = sqlData.replace(fromStr, toStr)
        writeToFile(filename, sqlData)


def addIncrementalLine(filename: str):
    fromStr = """-- this filter will only be applied on an incremental run
"""
    toStr = """{% if min_max_cdc_timestamp.min != None %}
        -- this filter will only be applied on an incremental run
        where -- and
        recordstamp > '{{ min_max_cdc_timestamp.min }}' and recordstamp <= '{{min_max_cdc_timestamp.max}}'
        -- DICE_CHANGE_SOURCE_WATERMARK > '{{ min_max_cdc_timestamp.min }}' and DICE_CHANGE_SOURCE_WATERMARK <= '{{min_max_cdc_timestamp.max}}'
        {% endif %}"""
    sqlData = readFile(filename)
    sqlData = sqlData.replace(fromStr, toStr)
    writeToFile(filename, sqlData)


def removeIncrementalLine(filename: str):
    fromStr = """{% if is_incremental() %}
        {% set min_max_cdc_timestamp = get_incremental_timestamp('cdc_timestamp') %} 
        -- this filter will only be applied on an incremental run

    {% else %}
        
    {% endif %}"""
    toStr = ""
    sqlData = readFile(filename)
    sqlData = sqlData.replace(fromStr, toStr)
    writeToFile(filename, sqlData)


def removeIncrementalPredicate(filename: str):
    fromStr = """incremental_predicates  =   ["dynamic_timestamp_range", "cdc_timestamp"],
            """
    toStr = ""
    sqlData = readFile(filename)
    sqlData = sqlData.replace(fromStr, toStr)
    writeToFile(filename, sqlData)


def addCdcColumns(filename: str):
    fromStr = "current_timestamp() as metadata_inserted_timestamp"
    toStr = """cdc_operation_type -- operation_flag/DICE_CHANGE_INDICATOR       as cdc_operation_type
    ,     cdc_timestamp      -- recordstamp/DICE_CHANGE_SOURCE_WATERMARK   as cdc_timestamp
    ,     current_timestamp() as metadata_inserted_timestamp"""
    sqlData = readFile(filename)
    sqlData = sqlData.replace(fromStr, toStr)
    writeToFile(filename, sqlData)


def addCdcOperationType(filename: str):
    fromStr = """- name: metadata_inserted_timestamp
        data_type: timestamp"""
    toStr = """- name: cdc_operation_type
        data_type: string
      - name: metadata_inserted_timestamp
        data_type: timestamp"""
    ymlData = readFile(filename)
    ymlData = ymlData.replace(fromStr, toStr)
    writeToFile(filename, ymlData)


def removeCdcTimestamp(filename: str):
    fromStr = """- name: cdc_timestamp
        data_type: timestamp
      """
    toStr = ""
    ymlData = readFile(filename)
    ymlData = ymlData.replace(fromStr, toStr)
    writeToFile(filename, ymlData)


def addMetaTimestamp(filename: str):
    modelData = readYMLFile(filename)
    columnName = modelData["models"][0]["tests"][0]["unique"]["column_name"]
    fromStr = f"""tests:
      - unique:
          column_name: "{columnName}"
    """
    toStr = f"""tests:
      - unique:
          column_name: "{columnName} || '-' || metadata_hub_begin_timestamp || '-' || metadata_hub_end_timestamp"
      - not_null:
          column_name: "{columnName} || '-' || metadata_hub_begin_timestamp"
    """
    ymlData = readFile(filename)
    ymlData = ymlData.replace(fromStr, toStr)
    writeToFile(filename, ymlData)


def createLatestVersion(table_name: str, basePath: str):
    latestVersionSQL = (
        """{{
        config(
            labels  =  {'model' : '"""
        + table_name
        + """_t1'},
            alias   = '"""
        + table_name
        + """_t1',
            materialization = 'view'
        )
    }}
    
    select * from {{ ref('"""
        + table_name
        + """') }}
"""
    )
    filename = f"{basePath}/{table_name}_latest_version.sql"
    writeToFile(filename, latestVersionSQL)


def do_type0(table_name: str, layer: str):
    modelLayer = ""
    if isStage(table_name):
        modelLayer = "staging"
    else:
        modelLayer = "marts"

    command = f"""{getCheckoutStuff(table_name)}

cd {datT}/models/{modelLayer}/{layer}/ppf/
mkdir {table_name}
cp {datTr}/models/{modelLayer}/{layer}/ppf/{table_name}/* ./{table_name}

cd {datT}

git add .
git commit -m "Adding in files for {table_name} {emojis}"

"""
    print("Running type0 copy steps")
    writeToFile(commandPath, command)
    runScript()

    print("Doing type0 alteration steps")
    fileZeroPath = (
        f"{datT}/models/{modelLayer}/{layer}/ppf/{table_name}/{table_name}_v1.sql"
    )
    fileZero = readFile(fileZeroPath)

    refs = readBetweenTheLine(fileZero, "{{ ref(", ") }}")
    createRefFiles(refs)

    sources = readBetweenTheLine(fileZero, "{{ source(", ") }}")
    createSourceFiles(sources)
    # convertSourcesToRefs(fileZeroPath, sources)

    # addIncrementalLine(fileZeroPath)
    removeIncrementalLine(fileZeroPath)

    # removeIncrementalPredicate(fileZeroPath)
    # addCdcColumns(fileZeroPath)

    fileZeroBasePath = f"{datT}/models/{modelLayer}/{layer}/ppf/{table_name}"
    createLatestVersion(table_name, fileZeroBasePath)

    fileZeroModelPath = (
        f"{datT}/models/{modelLayer}/{layer}/ppf/{table_name}/_models.yml"
    )
    if table_name.startswith("md_"):
        removeCdcTimestamp(fileZeroModelPath)
    if table_name.startswith("dim_"):
        removeCdcTimestamp(fileZeroModelPath)
    else:
        addCdcOperationType(fileZeroModelPath)


def do_type1(table_name: str, layer: str):
    command = f"""{getCheckoutStuff(table_name)}

cd {datT}/models/marts/{layer}/ppf/

mkdir {table_name}
cp {datTr}/models/marts/{layer}/ppf/{table_name}/* ./{table_name}

mkdir legacy_{table_name}
cp {datTr}/models/marts/{layer}/ppf/legacy_{table_name}/* ./legacy_{table_name}

cd {datT}

git add .
git commit -m "Adding in files for {table_name} {emojis}"

"""
    print("Running type1 copy steps")
    writeToFile(commandPath, command)
    runScript()

    print("Doing type1 alteration steps")
    martPath = f"{datT}/models/marts/{layer}/ppf/{table_name}/{table_name}_v1.sql"
    martFile = readFile(martPath)

    refs = readBetweenTheLine(martFile, "{{ ref(", ") }}")
    createRefFiles(refs)

    sources = readBetweenTheLine(martFile, "{{ source(", ") }}")
    createSourceFiles(sources)
    # convertSourcesToRefs(martPath, sources)

    # addIncrementalLine(martPath)
    removeIncrementalLine(martPath)
    removeIncrementalPredicate(martPath)
    addCdcColumns(martPath)

    martModelPath = f"{datT}/models/marts/{layer}/ppf/{table_name}/_models.yml"
    addCdcOperationType(martModelPath)

    legacyModelPath = f"{datT}/models/marts/{layer}/ppf/legacy_{table_name}/_models.yml"
    removeCdcTimestamp(legacyModelPath)


def do_type2(table_name: str, layer: str):
    command = f"""{getCheckoutStuff(table_name)}

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
    print("Running type2 copy steps")
    writeToFile(commandPath, command)
    runScript()

    print("Doing type2 alteration steps")
    stgPath = (
        f"{datT}/models/staging/{layer}/ppf/stg_{table_name}/stg_{table_name}_v1.sql"
    )
    stgFile = readFile(stgPath)

    refs = readBetweenTheLine(stgFile, "{{ ref(", ") }}")
    createRefFiles(refs)

    sources = readBetweenTheLine(stgFile, "{{ source(", ") }}")
    createSourceFiles(sources)
    # convertSourcesToRefs(stgPath, sources)

    # addIncrementalLine(stgPath)
    removeIncrementalLine(stgPath)
    removeIncrementalPredicate(stgPath)
    addCdcColumns(stgPath)

    stgModelPath = f"{datT}/models/staging/{layer}/ppf/stg_{table_name}/_models.yml"
    addCdcOperationType(stgModelPath)

    legacyModelPath = f"{datT}/models/marts/{layer}/ppf/legacy_{table_name}/_models.yml"
    removeCdcTimestamp(legacyModelPath)
    addMetaTimestamp(legacyModelPath)


def doType(table_name: str, type: str, layer: str):
    match type:
        case "type0":
            print("Doing Type0")
            do_type0(table_name, layer)
        case "type1":
            print("Doing Type1")
            do_type1(table_name, layer)
        case "type2":
            print("Doing Type2")
            do_type2(table_name, layer)
        case _:
            print(f"i don't know what to do...\nType: {type}")
            getTableTypeFromUser(table_name, layer)


# Processing start
while True:
    table_name = input("Enter Table Name: ")
    clearRefs()
    table_name = table_name.lower()
    print("Getting Type")
    type = getTableType(table_name)
    print("Getting Layer")
    layer = getLayer(table_name)
    doType(table_name, type, layer)
    print(f"Initial migration steps for {table_name} done")
