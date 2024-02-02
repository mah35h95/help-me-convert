import re


def writeToFile(filename: str, content: str):
    f = open(filename, "w", encoding="utf-8")
    if filename.__contains__(".sh"):
        f.write("#!/bin/bash\n")
    f.write(content)
    f.close()


def readFile(filename: str) -> str:
    with open(filename) as f:
        data = f.read()
    return data


def removeMultiLineTxt(filename: str, start: str, end: str):
    fileData = readFile(filename)
    startRe = str(re.escape(start))
    endRe = str(re.escape(end))
    fileData = re.sub(
        f"{startRe}(\n.*?)*?\n{endRe}\n", "", fileData, flags=re.MULTILINE
    )
    writeToFile(filename, fileData)


def removeTxt(filename: str, fromStr: str):
    toStr = ""
    data = readFile(filename)
    data = data.replace(fromStr, toStr)
    writeToFile(filename, data)


files = []

for file in files:
    fileData = readFile(file)
    removeMultiLineTxt(
        file, "=======", ">>>>>>> 3a1cc2bdfac64472f4a2a92125129f463794f674"
    )
    removeTxt(
        file,
        """<<<<<<< HEAD
""",
    )
