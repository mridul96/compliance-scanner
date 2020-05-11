# import module
import multiprocessing
import warnings
import zipfile
import tarfile
import logging
import pyspark  #pip install pyspark==2.4.4
import boto3    #pip install boto3==1.11.0
import pandas   #pip install pandas==1.0.0
import xlrd     #pip install xlrd==1.2.0
import io
import os
import sys
import ast
import docx     #!pip install python-docx==0.8.10
import uuid
import json
import time
import gzip
import tables   #!pip install tables==3.6.1
import pyxlsb   #!pip install pyxlsb==1.0.6
import shutil
import pickle
import pyarrow  #!pip install pyarrow==0.13.0
import functools

# suppress warning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Extension readable text
_TXT = (".txt",".json",".log",".tsv",".csv",".csv000",".yaml",".yml",".html")
_STR = (".ipynb",".py",".sql",".sh",".bash",".js",".temp",".mf",".css") # text
_GZP = (".gz",".csv.gz",".json.gz",".txt.gz",".log.gz",".tar.gz") # gnu-zip #TODO
_ZIP = (".zip",) # compressed-zip #TODO
_TAR = (".tar",) # compressed-tar #TODO
_ORC = (".ORC",".orc") # compressed-orc
_PKT = (".parquet",) # hadoop-file
_HFI = (".h5",".hdf5") # HDF-file #TODO
_EXL = (".xlsx",".xls",".xlsb") # MS-Excel #TODO
_DOC = (".docx",) # document #TODO
_PKL = (".pkl",) # python-pickle
_EXT = _TXT+_STR+_GZP+_ZIP+_TAR+_ORC+_PKT+_HFI+_EXL+_DOC+_PKL # total extension

def maskingHFI(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Hierarchical Data Format (HDF)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingHFI`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingORC(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Highly Compressed Apache (ORC)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        SAVE    = FILE+".temp"
        KEY     = FILE.split(SEP)[-1]
        # create spark session
        SPARK = pyspark.sql.SparkSession.builder \
                .master("local") \
                .appName("Scanner") \
                .getOrCreate()
        # convert ORC to pandas
        ORCDF = SPARK.read.orc(FILE).toPandas()
        # convert value python built-in-type
        try:
            FIND = int(FIND)
        except Exception as e:
            try:
                FIND = float(FIND)
            except Exception as e:
                try:
                    FIND = str(FIND)
                except Exception as e:
                    None
        # replace value
        ORCDF_NEW = ORCDF.replace(to_replace=FIND,value=REPLACE)
        # replace file on disk
        SPARK.createDataFrame(ORCDF_NEW).write.format("orc").save(SAVE)
        # remove old file from disk
        os.remove(FILE) if os.path.exists(FILE) else None
        # rename temp file
        os.rename(SAVE, FILE)
        # change status to true
        STATUS = not (ORCDF == ORCDF_NEW).all()[0]
    # in case of exception
    except Exception as e:
        logging.error("`maskingORC`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingPKT(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Apache Parquet (.parquet)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        SAVE    = FILE+".temp"
        KEY     = FILE.split(SEP)[-1]
        # convert parquet to pandas
        PARQUET = pandas.read_parquet(FILE)
        # convert value python built-in-type
        try:
            FIND = int(FIND)
        except Exception as e:
            try:
                FIND = float(FIND)
            except Exception as e:
                try:
                    FIND = str(FIND)
                except Exception as e:
                    None
        # replace value
        PARQUET_NEW = PARQUET.replace(to_replace=FIND,value=REPLACE)
        # replace file on disk
        PARQUET_NEW.to_parquet(FILE+".temp")
        # remove old file from disk
        os.remove(FILE) if os.path.exists(FILE) else None
        # rename temp file
        os.rename(SAVE, FILE)
        # change status to true
        STATUS = not (PARQUET == PARQUET_NEW).all()[0]
    # in case of exception
    except Exception as e:
        logging.error("`maskingPKT`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingEXL(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from MS Excel (xls, xlsb,xlsx)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingEXL`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingPKL(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Python pickel (pkl)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        SAVE    = FILE+".temp"
        KEY     = FILE.split(SEP)[-1]
        # convert pickel to string
        with open(FILE, "rb") as INFILE:
            DATA = pickle.load(INFILE)
        try:
            CONTENT = str(DATA.decode("utf-8"))
        except:
            CONTENT = str(DATA)
        finally:
            CONTENT_NEW = CONTENT.replace(FIND, REPLACE)
            with open(FILE,"wb") as OUTFILE:
                if isinstance(DATA, str):
                    pickle.dump(CONTENT_NEW, OUTFILE)
                else:
                    pickle.dump(ast.literal_eval(CONTENT_NEW), OUTFILE)
            STATUS = not (CONTENT == CONTENT_NEW)
    # in case of exception
    except Exception as e:
        logging.error("`maskingPKL`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingDOC(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Ms Word (doc,docx)
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingDOC`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingSTR(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from string readable formats and
    files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
        # try to read file as text
        try:
            with open(FILE,"r") as INFILE:
                CONTENT = INFILE.read()
        except Exception as e:
            with open(FILE,"rb") as INFILE:
                CONTENT = str(INFILE.read())
        finally:
            CONTENT_NEW = CONTENT.replace(FIND, REPLACE)
            with open(FILE,"w") as OUTFILE:
                OUTFILE.write(CONTENT_NEW)
            STATUS = not (CONTENT == CONTENT_NEW)
    # in case of exception
    except Exception as e:
        logging.error("`maskingSTR`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return status and result
    return(STATUS)

def maskingGZP(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from GNU Gzip files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingGZP`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingTAR(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Tar or Tarball files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingTAR`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)

def maskingZIP(File=None, Find=None, Replace=None):
    """
    Function to find and replace values from Zip files from disk.
    """
    try:
        STATUS  = None   # return variable
        FILE    = File
        FIND    = Find
        REPLACE = Replace
        KEY     = FILE.split(SEP)[-1]
    # in case of exception
    except Exception as e:
        logging.error("`maskingZIP`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return true if any one is true
    return(STATUS)


def checkExtension(File=None,Find=None,Replace=None):
    """
    Function to check extension of File and call the relevant
    content search function.
    """
    try:
        STATUS = None # defualt return value
        KEY    = File.split(SEP)[-1]
        # check the extension
        if KEY.lower().endswith(_TXT+_STR):
            STATUS = maskingSTR(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_GZP):
            STATUS = maskingGZP(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_ORC):
            STATUS = maskingORC(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_HFI):
            STATUS = maskingHFI(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_PKT):
            STATUS = maskingPKT(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_EXL):
            STATUS = maskingEXL(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_TAR):
            STATUS = maskingTAR(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_ZIP):
            STATUS = maskingZIP(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_DOC):
            STATUS = maskingDOC(File=File,Find=Find,Replace=Replace)
        elif KEY.lower().endswith(_PKL):
            STATUS = maskingPKL(File=File,Find=Find,Replace=Replace)
        else:
            STATUS = None
    # in case of exception
    except Exception as e:
        logging.error("`checkExtension`: Failed to assert file `{}`.\n{}" \
                      .format(KEY, e))
    # return the status and found
    return(STATUS)

if __name__ == '__main__':
    """
    Script `masking.py` designed to mask the `Find` value in the file
    with relavent `Replace` value. All the logging info will be shown
    on system console.
    """
    # mark script start time
    START = time.time()

    try:
        PATH    = sys.argv[1]
        FIND    = sys.argv[2]
        REPLACE = sys.argv[3]
    # in case of exception
    except Exception as e:
        # log the critical as arguments are not passed properly
        logging.critical("`masking.py`: `PATH`,`FIND`,`REPLACE` not passed.")
        sys.exit("`masking.py` execution stopped in between.")
    finally:
        # setup logging level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)
        # define sep and cwd
        CWD, SEP = os.getcwd(), os.sep
        # log the info
        logging.info("Path    : `{}`.".format(PATH))
        logging.info("Find    : `{}`.".format(FIND))
        logging.info("Replace : `{}`.".format(REPLACE))

    # check if path is file
    if os.path.isfile(PATH):
        FLAG = checkExtension(File=PATH,Find=FIND,Replace=REPLACE)
        logging.info("Masked status: `{}` - `{}`.".format(FLAG,PATH))
    # check if path is directory
    elif os.path.isdir(PATH):
        for SUBDIR, DIR, FILES in os.walk(PATH):
            for FILE in FILES:
                FILEPATH = os.path.join(SUBDIR,FILE)
                FLAG = checkExtension(File=FILEPATH,Find=FIND,Replace=REPLACE)
                logging.info("Masked status: `{}` - `{}`.".format(FLAG,FILE))
    # log as path is unknown
    else:
        logging.critical("`masking.py`: `PATH` is of unknown (not a file or dir).")
        sys.exit("`masking.py` execution stopped in between.")

    # mark script end time
    STOP = time.time()

    # log the general criticalrmation
    logging.critical("`masking.py` execution completed in {0:.2f} min." \
                 .format((STOP-START)/60))

    sys.exit("`masking.py` execution completed.")
