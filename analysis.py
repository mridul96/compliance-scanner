# import libraries
import json
import csv
import os
import time
import logging
import sys


def analysisS3Scanner(File=None,Find=None,Output=None):
    """
    Function iterate over all the files in the list and combine the
    result in `S3Scanner_Analysis.csv` based on `FIND` value.
    """
    try:
        STATUS = False # return variable
        FILE   = File
        FIND   = Find
        WRITER = Output
        SESSION = FILE.split(SEP)[-1].split("_")[0]
        # read the content of file
        with open(FILE, "r") as INFILE:
            DATA = json.loads(INFILE.read())
            for UUID,LIST in DATA.items():
                BUCKET = LIST[0]
                S3FILE = LIST[1]
                REPORT = LIST[2]
                # check if find contains any value
                if FIND:
                    for KEY,VALUE in REPORT.items():
                        if isinstance(VALUE, list):
                            if set(VALUE).intersection(set(FIND)):
                                SUBFILE = " "
                                USER    = KEY
                                FOUND   = VALUE
                                WRITER.writerow([SESSION,BUCKET,
                                            S3FILE,SUBFILE,USER,FOUND])
                            else:
                                None
                        elif isinstance(VALUE, dict):
                            for USER,FOUND in VALUE.items():
                                if set(FOUND).intersection(set(FIND)):
                                    SUBFILE = KEY
                                    WRITER.writerow([SESSION,BUCKET,
                                            S3FILE,SUBFILE,USER,FOUND])
                                else:
                                    None
                        else:
                            None
                # incase no value recived iterate over all values
                else:
                    for KEY,VALUE in REPORT.items():
                        if isinstance(VALUE, list):
                            SUBFILE = " "
                            USER    = KEY
                            FOUND   = VALUE
                            WRITER.writerow([SESSION,BUCKET,S3FILE,
                                                SUBFILE,USER,FOUND])
                        elif isinstance(VALUE, dict):
                            for USER,FOUND in VALUE.items():
                                SUBFILE = KEY
                                WRITER.writerow([SESSION,BUCKET,
                                            S3FILE,SUBFILE,USER,FOUND])
                        else:
                            None

        STATUS = True
    # in case of exception
    except Exception as e:
        logging.error("`analysisS3Scanner` failed to execute.\n{}".format(e))
    # return status
    return(STATUS)


if __name__ == "__main__":
    """
    Script `analysis.py` will find the files `S3Scanner.json` in
    the current working directory and merge the result into single
    `S3Scanner_Analysis.csv` file based on `FIND` value.
    """
    # mark script start time
    START = time.time()

    try:
        CWD, SEP = os.getcwd(), os.sep
        PATH = CWD + SEP
        FIND = sys.argv[1].split(",")
    # in case of exception
    except Exception as e:
        FIND = []
        logging.critical("`analysis.py`: `PATH`,`FIND` not passed.")
    finally:
        # setup logging level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)
        # log the value
        logging.info("Path : `{}`.".format(PATH))
        logging.info("Find : `{}`.".format(FIND))

    # open `S3Scanner_Analysis.csv` as output file
    with open("S3Scanner_Analysis.csv", "w", newline="") as CSVFILE:
        # create file handler
        WRITER = csv.writer(CSVFILE)
        # list all the file in the present directory
        for SUBDIR, DIR, FILES in os.walk(PATH):
            # for each file in list of files
            for FILE in FILES:
                # if file ends with `S3Scanner.json`
                if FILE.endswith("S3Scanner.json"):
                    FILEPATH = os.path.join(SUBDIR,FILE)
                    # execute the analysis based
                    STATUS = analysisS3Scanner(File=FILEPATH,
                                                    Find=FIND,
                                                        Output=WRITER)
                    logging.info("`{}`: analysis status `{}`." \
                                 .format(FILE,STATUS))

    # mark script end time
    STOP = time.time()

    # log the general criticalrmation
    logging.info("`analysis.py` execution completed in {0:.2f} min." \
                 .format((STOP-START)/60))

    sys.exit("`analysis.py` execution completed.")
