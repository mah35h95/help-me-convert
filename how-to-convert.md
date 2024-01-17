# help me convert

A Python Script to parse and update dbt script

## how to convert

### steps in git

- [git](https://www.git-scm.com/downloads)
- [github desktop](https://desktop.github.com/) - very useful for quick navigation
- password set in [gitlab.com](https://gitlab.com/-/user_settings/password/edit) in [user-settings/password](https://gitlab.com/-/user_settings/password/edit)

### git config

To allow git to be able to use long paths run the following command in powershell in administrator mode

```sh
git config --system core.longpaths true
```

### file structure

```
dbt
│
└───data-at-tyson-transformations
│   └───branch - main/your-branch
│
└───data-at-tyson-transformations-ref
│   └───branch - utf_baseline_fix
|
└───help-me-convert
    │   dbt-conversion-script.py
    │   dependantRefs.txt
    |   how-to-convert.md
    |   table_keys.xlsx
    |   table_types.xlsx
```

### script global variables

```python
# Define Values
datT = "<full-path-to>/data-at-tyson-transformations"
datTr = "<full-path-to>/data-at-tyson-transformations-ref"
userDomainName = "your-domain-name"

```

### install python

#### python download

You can install python via [python.org](https://www.python.org/downloads/)

#### or

#### scoop package manager

install via [scoop](https://scoop.sh/)

to install scoop in windows run the following command in powershell

```ps1
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression
```

then install python scoop and follow the instructions it says of running the `install-pep-514.reg` you can run it by double clicking

```sh
scoop install python
```

### python dependencies installation

```sh
pip install pandas pyyaml openpyxl
```

### run the script

```sh
cd <full-path-to>/help-me-convert

python dbt-conversion-script.py
```

and then provide the table name as input so the script when it ask `Enter Table Name: ` in the terminal so it can create the branch and make the changes as required for the branch

### after the script runs

- verify the `*_v1.sql` files correctly place cdc_timestamp and cdc_operation_type
- Keep an eye for `<TODO: ADD Primary Keys>` where you might need to add the primary keys
- commit once confirmed if everything is working and then publish the branch to remote
- you can build the table in `dbt-dev`
- or create a new MR to trigger a pipeline to build in `dbt-qa`
- the script will wait until you provide it the next table name, i recommend first commit any changes before providing another table name

### flow of work with script

1. run script
2. enter the table name
3. verify the changes made in `data-at-tyson-transformations`
4. commit your changes
5. publish the branch
6. build the branch/changes in dbt development
7. create a new MR for the branch
8. make sure the pipeline passes
9. then sent the MR for approval
10. then repeat from step 2
