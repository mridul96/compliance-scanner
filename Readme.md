# Compliance Scanner
The scanner will take Email ID, First Name and Last Name as input using User
Interface and pull PI data from database and storage.

The output (i.e. analysis report) of the tool will be a text file (in JSON
format). The file consists of UUID as Key (unique for each entry) whereas
value contains bucket name and file name which contains the personal information
of the requested user.

File also contains the UUID and column name of personal information of the user
(i.e. the person for whom the search is done) in the file mapped as value for
the corresponding file name.

All the scanner report and personal information list of user will be stored
in a cloud. Files mentioned below will be available in a folder named as
the date on which S3-Scanner is executed, in the format: `YYYYMMDD`.

## TODO
- Mention appropriate value in `CREDENTIAL.json`.
- Update the query in `worker.py` as per database.
- Install the appropriate `python` packages and modules.

## Python Scripts
### `master.py - worker.py`
Script `worker.py` designed to auto scale the load over the multiple
cores of the existing system and perform the following task.

    1. Connect with cloud to fetch metadata.
    2. Connect with database for user PI data.
    3. Connect with cloud and search PI data in s3 buckets.

Note: Script auto clean any file downloaded in memory or on disk.
