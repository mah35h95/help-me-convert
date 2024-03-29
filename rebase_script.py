# Imports
import subprocess
import re
import yaml
import random

# Common file paths
ingestSourcesPath = "models/raw/_sources/_ingest_stage_sources.yml"
lakeSourcesPath = "models/raw/_sources/_lake_sources.yml"
diceModelsPath = "models/raw/dice_sources/_models.yml"
hanaModelsPath = "models/raw/hana_s4_ppf/_models.yml"

# Define Values
baseCommitHash = ""
fromBranch = ""
toBranchSuffix = ""
userDomainName = "Your-Domain-Name"
datT = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations"
datTr = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations-ref"
commandPath = "C:/Users/GM/Documents/local_dev/help-me-convert/temp/rebase.sh"
fileListPath = "C:/Users/GM/Documents/local_dev/help-me-convert/temp/fileList.txt"


# Function definitions
def runDIffFileScript():
    command = f"""cd {datT}
git checkout main
git pull
git checkout {userDomainName}-{toBranchSuffix} || git checkout -b {userDomainName}-{toBranchSuffix}

cd {datTr}
git checkout main
git pull

git branch | grep -v "main" | grep -v "utf_baseline_fix" | xargs git branch -D

git checkout {fromBranch}

git diff {baseCommitHash} {fromBranch} --name-only > {fileListPath}
"""
    writeToFile(commandPath, command)
    runScript()


def writeToFile(filename: str, content: str):
    f = open(filename, "w", encoding="utf-8")
    if filename.__contains__(".sh"):
        f.write("#!/bin/bash\nset +o history #set -o history\n")
    f.write(content)
    if filename.__contains__(".sh"):
        f.write("\nset -o history\n")
    f.close()


def runScript():
    subprocess.run([commandPath], shell=True)


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def readYMLFile(filename: str):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def getDiffFilesList() -> list[str]:
    fileListData = readFile(fileListPath)
    return fileListData.split("\n")


def copyFilesOver(filesToCopyList: list[str]):
    command = ""
    for file in filesToCopyList:
        command = (
            command
            + f"""install -Dv {datTr}/{file} {datT}/{file}
"""
        )
    writeToFile(commandPath, command)
    runScript()


def isIngestPathIncluded(pathsList: list[str]) -> bool:
    for path in pathsList:
        if path == ingestSourcesPath:
            return True
    return False


def isLakePathIncluded(pathsList: list[str]) -> bool:
    for path in pathsList:
        if path == lakeSourcesPath:
            return True
    return False


def getFileToCopy(pathsList: list[str]) -> list[str]:
    copyList = []
    for path in pathsList:
        if (
            path != ""
            and path != "models/raw/hana_s4_ppf/_models.yml"
            and not path.__contains__("models/raw/dice_sources/")
            and not path.__contains__("models/raw/_sources/")
        ):
            copyList.append(path)
    return copyList


def getSelectedFileToCopy(pathsList: list[str], fileToCopy: str) -> list[str]:
    copyList = []
    for path in pathsList:
        if path == fileToCopy:
            copyList.append(path)
    return copyList


def findInListOfDict(list, key: str, value: str) -> int:
    for i, dic in enumerate(list):
        if dic[key] == value:
            return i
    return -1


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


def isTableInYML(filename: str, name: str, ref: str) -> bool:
    sourceFile = readYMLFile(filename)
    sourceIndex = findInListOfDict(sourceFile["sources"], "name", name)
    if sourceIndex == -1:
        return False
    tableIndex = findInListOfDict(
        sourceFile["sources"][sourceIndex]["tables"], "name", ref
    )
    return tableIndex != -1


def insertStringAtIndex(data: str, stringToInsert: str, index: int) -> str:
    return data[:index] + stringToInsert + data[index:]


def addNewTableToModelYML(folderPath: str, tableName: str, fromModelPath: str):
    filename = f"{folderPath}/_models.yml"

    fromModelYml = readYMLFile(f"{datTr}/{fromModelPath}")
    tableIndex = findInListOfDict(fromModelYml["models"], "name", tableName)

    column_name = ""
    if tableIndex == -1:
        column_name = "<TODO: ADD Primary Keys>"
    else:
        column_name = fromModelYml["models"][tableIndex]["tests"][0]["unique"][
            "column_name"
        ]

    newModel = f"""- name: {tableName}
    latest_version: 1
    versions:
      - v: 1
    tests:
      - unique:
          column_name: "{column_name}"
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


def createIngestRefFiles(ref: str, filename: str, name: str):
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
    addNewTableToModelYML(folderPath, tableName, hanaModelsPath)


def copyIngestChanges():
    toSourcePath = f"{datT}/{ingestSourcesPath}"
    fromSourcePath = f"{datTr}/{ingestSourcesPath}"
    fromSourceYml = readYMLFile(fromSourcePath)

    name = "ingest_stage_hana_s4_ppf"
    for table in fromSourceYml["sources"][0]["tables"]:
        ref = table["name"]
        if not isTableInYML(toSourcePath, name, ref):
            createIngestRefFiles(ref, toSourcePath, name)


def removeDiceTableSuffix(name: str) -> str:
    return name.removesuffix("_current").removesuffix("_change_hist")


def createDatalakeRefFiles(ref: str, schemaName: str, filename: str, name: str):
    folderPath = f"{datT}/models/raw/dice_sources"
    database = """'{{env_var("DBT_SOURCE_LAKE_GCP_PROJECT")}}'"""
    schema = schemaName
    warnCount = 24
    warnPeriod = "hour"
    errCount = 48
    errPeriod = "hour"
    loaded_at_field = "dice_change_source_watermark"
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

    # if ref.__contains__("_change_hist"):
    #     refClean = removeDiceTableSuffix(ref)
    #     tableName = f"{refClean}_current"
    #     addNewTableToModelYML(folderPath, tableName, diceModelsPath)


def copyLakeChanges():
    toSourcePath = f"{datT}/{lakeSourcesPath}"
    fromSourcePath = f"{datTr}/{lakeSourcesPath}"
    fromSourceYml = readYMLFile(fromSourcePath)

    for schema in fromSourceYml["sources"]:
        schemaName = schema["name"]
        name = schemaName
        for table in schema["tables"]:
            ref = table["name"]
            if not isTableInYML(toSourcePath, name, ref):
                createDatalakeRefFiles(ref, schemaName, toSourcePath, name)


# Processing start
while True:
    baseCommitHash = input("Enter Base Commit Hash: ")
    fromBranch = input("Enter from Branch: ")
    toBranchSuffix = input("Enter to Branch Suffix: ")
    print("Getting file diffs")
    runDIffFileScript()
    go = input(
        """Can i go ahead?
y - Yes (default)
n - No
So what say: """
    )
    if go == "n":
        print("Okay Existing")
    else:
        pathsList = getDiffFilesList()
        ingestExist = isIngestPathIncluded(pathsList)
        lakeExist = isLakePathIncluded(pathsList)
        print("Copying files over")
        filesToCopy = getFileToCopy(pathsList)
        copyFilesOver(filesToCopy)
        if ingestExist:
            print("Copying Ingest Changes over")
            copyIngestChanges()
        if lakeExist:
            print("Copying Datalake Changes over")
            copyLakeChanges()
        print(f"Rebasing {fromBranch} changes done")
