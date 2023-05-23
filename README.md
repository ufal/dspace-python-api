[![Test dspace on dev-5](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml/badge.svg)](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml)

# Dspace-python-api
used for blackbox testing, data-ingestion procedures

# How to migrate CLARIN-DSpace5.* to CLARIN-DSpace7.*
### Prerequisites:
- Installed CLARIN-DSpace7.*. with running database, solr, tomcat

### Steps:
1. Clone python-api: https://github.com/dataquest-dev/dspace-python-api (branch `main`) and dpace://https://github.com/dataquest-dev/DSpace (branch `dtq-dev`)

***
2. Get database dump (old CLARIN-DSpace) and unzip it into the `<PSQL_PATH>/bin` (or wherever you want)

***
3. Create CLARIN-DSpace5.* databases (dspace, utilities) from dump.
> // clarin-dspace database
> - `createdb --username=postgres --owner=dspace --encoding=UNICODE clarin-dspace` // create a clarin database with owner

> // It run on second try:
> - `psql -U postgres clarin-dspace < <CLARIN_DUMP_FILE_PATH>`

> // clarin-utilities database
> - `createdb --username=postgres --owner=dspace --encoding=UNICODE clarin-utilities` // create a utilities database with owner

> // It run on second try:
> - `psql -U postgres clarin-utilities < <UTILITIES_DUMP_FILE_PATH>`

***
4. Recreate your local CLARIN-DSpace7.* database **NOTE: all data will be deleted**
- Install again the database following the official tutorial steps: https://wiki.lyrasis.org/display/DSDOC7x/Installing+DSpace#InstallingDSpace-PostgreSQL11.x,12.xor13.x(withpgcryptoinstalled)
- Or try to run these commands in the <PSQL_PATH>/bin:
> - `createdb --username=postgres --owner=dspace --encoding=UNICODE dspace` // create database
> - `psql --username=postgres dspace -c "CREATE EXTENSION pgcrypto;"` // Add pgcrypto extension
> > If it throws warning that `-c` parameter was ignored, just write a `CREATE EXTENSION pgcrypto;` command in the database cmd.
> > CREATE EXTENSION pgcrypto;
![image](https://user-images.githubusercontent.com/90026355/228528044-f6ad178c-f525-4b15-b6cc-03d8d94c8ccc.png)
 

> // Now the clarin database for DSpace7 should be created
> - Run the database by the command: `pg_ctl start -D "<PSQL_PATH>\data\"`

***
5. (Your DSpace project must be installed) Go to the `dspace/bin` and run the command `dspace database migrate force` // force because of local types
**NOTE:** `dspace database migrate force` creates default database data that may be not in database dump, so after migration, some tables may have more data than the database dump. Data from database dump that already exists in database is not migrated.

***
6. Create an admin by running the command `dspace create-administrator` in the `dspace/bin`

***
7. Prepare `dspace-python-api` project for migration
**IMPORTANT:** If `data` folder doesn't exist in the project, create it

Update `const.py`
- `user = "<ADMIN_NAME>"`
- `password = "<ADMIN_PASSWORD>"`

- `# http or https`
- `use_ssl = False`
- `host = "<YOUR_SERVER>" e.g., localhost`
- `# host = "dev-5.pc"`
- `fe_port = "<YOUR_FE_PORT>"`
- `# fe_port = ":4000"`
- `be_port = "<YOUR_BE_PORT>"`
- `# be_port = ":8080"`
- `be_location = "/server/"`

Update `migration_const.py`
- `REPOSITORY_PATH = "<PROJECT_PATH>"`
- `DATA_PATH = REPOSITORY_PATH + "data/"`

***
8. Create JSON files from the database tables. **NOTE: You must do it for both databases `clarin-dspace` and `clarin-utilities`** (JSON files are stored in the `data` folder)
- Go to `dspace-python-api` in the cmd
- Run `pip install -r requirements.txt`
- Run `python data_migration.py <DATABSE NAME> <HOST> postgres <PASSWORD FOR POSTGRES>` e.g., `python data_migration.py clarin-dspace localhost postgres pass` (arguments for database connection - database, host, user, password) for the BOTH databases // NOTE there must exist data folder in the project structure

***
9. Copy `assetstore` from dspace5 to dspace7 (for bitstream import). `assetstore` is in the folder where you have installed DSpace `dspace/assetstore`.

***
10. Import data from the json files (python-api/data/*) into dspace database (CLARIN-DSpace7.*)
- **NOTE:** database must be up to date (`dspace database migrate force` must be called in the `dspace/bin`)
- **NOTE:** dspace server must be running
- From the `dspace-python-api` run command `python dspace_import.py`

***
## !!!Migration notes:!!!
- The values of table attributes that describe the last modification time of dspace object (for example attribute `last_modified` in table `Item`) have a value that represents the time when that object was migrated and not the value from migrated database dump.



## How to write new tests
Check test.example package. Everything necessary should be there.

Test data are in `test/data` folder.
If your test data contains special characters like čřšáý and so on, it is recommended
to make `.stripped` variation of the file. 
E.g. `my_format.json` and `my_format.stripped.json` for loading data
and `my_format.test.xml` and `my_format.test.stripped.xml` for testing.

If not on dev-5 (e.g. when run on localhost), `.stripped` version of files will be loaded.
The reason for this is, that when dspace runs on windows, it has trouble with special characters.


## Settings
See const.py for constants used at testing.

To set up logs, navigate to support.logs.py and modify method set_up_logging.

## Run

In order to run tests, use command
`python -m unittest`

Recommended variation is
`python -m unittest -v 2> output.txt`
which leaves result in output.txt

Before running for the first time, requirements must be installed with following command
`pip install -r requirements.txt`

It is possible to run in Pycharm with configuration like so:

![image](https://user-images.githubusercontent.com/88670521/186934112-d0f828fd-a809-4ed8-bbfd-4457b734d8fd.png)
