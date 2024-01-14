import yaml
import random
import re


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def writeToFile(filename: str, fileData: str):
    with open(filename, "w") as f:
        f.write(fileData)


def readYMLFile(filename: str):
    with open(filename) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


def getRandomPosition(listLength: int) -> int:
    randIndex = 0
    if listLength - 1 > 0:
        randIndex = random.randrange(0, listLength - 1)
    elif listLength == 1:
        randIndex = 1
    return randIndex


def insertStr(data: str, strToInsert: str, index: int) -> str:
    return data[:index] + strToInsert + data[index:]


filename = "C:/Users/GM/Documents/GitHub/2763-entdatawh/data-at-tyson-transformations/models/raw/dice_sources/_models.yml"
ref = "customer_account_group_record_type_xref"

columnName = "some, here, yes"
unique_column_name = columnName.replace(", ", " || '-' || ")
tableName = f"{ref}_current"
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
    ymlData = insertStr(ymlData, newModel, searchMatch.start())

writeToFile(filename, ymlData)
