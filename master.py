# import libraries
import flask #!pip install flask==1.1.1
import subprocess #!pip install gunicorn==20.0.4
import uuid
import os
import sys
import time
import json
import signal
import logging
import itertools
import logging.handlers as handler


# create flask app
APP = flask.Flask(__name__)
# download filename
NAME= "InclusiveBucket.txt"
# process id
PID = None
# os seperator and cwd
CWD, SEP = os.getcwd(), os.sep
SYSTEM   = CWD+SEP+"system"+SEP
# create path if does not exist
if not os.path.exists(SYSTEM):
    os.makedirs(SYSTEM)
# define log file name
LOG = SYSTEM+"master.log"
# set rotating file handler
HANDLER = handler.RotatingFileHandler(LOG,maxBytes=1024*1024,backupCount=5)
# set logging level
HANDLER.setLevel(logging.INFO)
# define format
FORMAT  = "[%(asctime)s]-%(levelname)s-%(process)d-%(lineno)d---%(message)s"
# set format
HANDLER.setFormatter(logging.Formatter(FORMAT))
# add handler
APP.logger.addHandler(HANDLER)
# system exection path
EXECUTION = sys.executable
# logger exection path
APP.logger.critical("\nCurrent Execution Path: `{}`".format(EXECUTION))
# remove old master/worker file if-exists
CLEANA = SYSTEM+"UserRequest.json"
os.remove(CLEANA) if os.path.exists(CLEANA) else None
CLEANB = SYSTEM+"RedShiftPI.json"
os.remove(CLEANB) if os.path.exists(CLEANB) else None
CLEANC = SYSTEM+"BucketList.json"
os.remove(CLEANC) if os.path.exists(CLEANC) else None
CLEAND = SYSTEM+"BucketSkip.json"
os.remove(CLEAND) if os.path.exists(CLEAND) else None
CLEANE = SYSTEM+"S3Scanner.json"
os.remove(CLEANE) if os.path.exists(CLEANE) else None
CLEANF = SYSTEM+"worker.log"
os.remove(CLEANF) if os.path.exists(CLEANF) else None


def getFileDiskModeA(File=None):
    """
    Function to read file from passed location in append mode
    returns the file content.

        Arguments - File(or File path)

    Function will raise exception if path is not correct.
    """
    try:
        DATA = str() # return variable
        FILE = File
        # file created if not present
        with open(FILE, "a+") as INFILE:
            INFILE.seek(0)
            DATA = INFILE.read()
    # error as not a valid file
    except Exception as e:
        APP.logger.error("`getFileDisk`: Error 404 PATH `{}`.\n{}" \
                      .format(FILE, e))
    # return data
    return(DATA)

def getFileDiskModeR(File=None):
    """
    Function to read file from passed location in read mode
    returns the file content.

        Arguments - File(or File path)

    Function will raise exception if path is not correct.
    """
    try:
        DATA = str() # return variable
        FILE = File
        # file created if not present
        with open(FILE, "r") as INFILE:
            INFILE.seek(0)
            DATA = INFILE.read()
    # error as not a valid file
    except Exception as e:
        APP.logger.error("`getFileDisk`: Error 404 PATH `{}`.\n{}" \
                      .format(FILE, e))
    # return data
    return(DATA)

def putJSONDisk(File=None, Update=None):
    """
    Function to update JSON file passed as the argument, if the file is
    not a valid JSON it will flush the data and write the new content.

        Arguments - File(or File path), Update

    In case of file not found, it will create file in that location.
    Function will log exception if path is not correct.
    """
    try:
        FLAG = False # return variable
        FILE = File
        DATA = Update
        SIZE = len(DATA)
        JSON = dict()
        # check if DATA is valid dictionary
        if isinstance(DATA, dict) and SIZE:
            # file created if not present
            with open(FILE, "a+") as INFILE:
                try:
                    INFILE.seek(0) # reset cursor
                    JSON = json.load(INFILE)
                    JSON.update(DATA)
                    open(FILE, "w").close() # flush the file
                    json.dump(JSON, INFILE, indent=4) # write updated data
                # error as failed to load JSON
                except ValueError as e:
                    INFILE.truncate() # flush data in memory
                    open(FILE, "w").close()  # flush the file
                    json.dump(DATA, INFILE, indent=4) # write data
                # return true if succeed
                FLAG = True
        else:
            raise TypeError("`Dictionary` TypeError or Empty #{}.".format(SIZE))
    # in case of exception
    except Exception as e:
        APP.logger.error("`putJSONDisk`: Failed to save file `{}`.\n{}" \
                      .format(FILE, e))
    # return FLAG
    return(FLAG)

def putSubmitFile(File=None, Update=None):
    """
    Function to write data received from submit button which is passed
    as the argument, if it is a valid json it will flush the existing
    data and update the file and will write the new content in the file.

        Arguments - File(or File path), Update

    Function will raise exception if path is not correct. In case of
    file not found, it will create file in that location.
    """
    try:
        FLAG = False # return variable
        FILE = File
        DATA = Update
        JSON = dict()
        # file created if not present
        with open(FILE, "a+") as INFILE:
            JSON = json.load(DATA)
            # check if DATA is not empty
            if len(JSON):
                INFILE.seek(0) # reset cursor
                INFILE.truncate() # flush the old data
                open(FILE, "w").close() # flush the file
                json.dump(JSON, INFILE, indent=4) # write updated data
                FLAG = True # set status true if succeed
            else:
                raise TypeError("Empty file or data.")
    # in case of exception
    except Exception as e: # error as no file passed
        APP.logger.error("`putSubmitFile`: Not received a dictionary.\n{}" \
                      .format(e))
    # return status
    return(FLAG)

def putFileDisk(File=None, Update=None):
    """
    Function to update file passed as per the argument, if
    file is notvalid then data will be flushed and the new
    content is written.

        Arguments - File, Update

    In case of file not found, will create file in that location.
    Function will raise exception if path is not correct.
    """
    try:
        FLAG = False # return variable
        FILE = File
        DATA = Update
        TRASH = ["\n","\"","/","\'","[","]"," ","$","@","%","!",")",
                 "(","^","&","*","\\","\/","{","}","+","=",">","<",
                 "`","~","_","#","?","\r"]
        # convert to lower
        DATA = DATA.lower()
        # execute the converter
        for RE in TRASH:
            DATA=DATA.replace(RE,"")
        for RE in [";",":","|"]:
            DATA=DATA.replace(RE,",")
        # file created if not present
        with open(FILE, "w") as INFILE:
            INFILE.write(DATA)
        # set status true if succeed
        FLAG = True
    # error as no file passed
    except Exception as e:
        APP.logger.error("`putFileDisk`: Not able save JSON file.\n{}" \
                      .format(e))
    # return status
    return(FLAG)

@APP.route("/")
def index():
    """
    Default "/" when application is accessed from the web
    browser. Path for SYSTEM is set and Exclusive and
    inclusive bucket list are fetched from the file.
    """
    # fetch data from files
    ELIST = getFileDiskModeA(File=SYSTEM+"ExclusiveBucket.txt")
    ILIST = getFileDiskModeA(File=SYSTEM+"InclusiveBucket.txt")
    # return rendered template
    return(flask.render_template("input.html",TEXTI=ILIST,TEXTE=ELIST))

@APP.route("/submit",methods=["POST", "GET"])
def submitRequest():
    """
    Default "/submit" route which will render "input.html".
    "UserRequest.json" is updated via form or upload button
    when submit buttom is clicked. Exclusive and Inclusive
    bucket are update with each submit request.
    """
    # if request is POST
    if flask.request.method=="POST":
        # fetch input form data
        EMAIL = flask.request.form.get("EMAIL").strip(" ").lower()
        FNAME = flask.request.form.get("FNAME").strip(" ").lower()
        LNAME = flask.request.form.get("LNAME").strip(" ").lower()
        # update only when email is received
        if not len(EMAIL) == 0:
            UUID  = str(uuid.uuid4())
            VALUE = (EMAIL, FNAME, LNAME)
            putJSONDisk(File=SYSTEM+"UserRequest.json", Update={UUID:VALUE})
        # fetch submited file data
        elif not flask.request.files["Upload"].filename=="":
            # fetch file from user
            DATA = flask.request.files["Upload"]
            putSubmitFile(File=SYSTEM+"UserRequest.json",Update=DATA)
        # for any other request
        else:
            APP.logger.critical("Other request received apart from POST.")
        # fetch input text area data
        IBUCKET = flask.request.form.get("IBUCKET")
        EBUCKET = flask.request.form.get("EBUCKET")
        # update files
        putFileDisk(File=SYSTEM+"ExclusiveBucket.txt",Update=EBUCKET)
        putFileDisk(File=SYSTEM+"InclusiveBucket.txt",Update=IBUCKET)
    else:
        APP.logger.critical("Other request received apart from POST.")
        # fetch data from files
    EXCLUSIVE = getFileDiskModeA(File=SYSTEM+"ExclusiveBucket.txt")
    INCLUSIVE = getFileDiskModeA(File=SYSTEM+"InclusiveBucket.txt")
    # return rendered template
    return(flask.render_template("input.html",TEXTI=INCLUSIVE,TEXTE=EXCLUSIVE))

@APP.route("/dashboard",methods=["POST","GET"])
def viewRequest():
    """
    Default "/dashboard" route which will render the "output.html"
    with the arguments available. By default page will render with
    default control instruction about the functionality.
    """
    global NAME, PID # define global variable
    HEADER = "S3 Scanner" # page header
    CONTENT= """
    Click the button below:

    * `REQUEST` to view submitted user request for scanner.
    * `RESULT` to view scanner output.
    * `DOWNLOAD` to download file to local machine.
    * `CLEAN` to force clean the sytem.
    * `EXECUTE` to execute the scanner on submitted user request.
    """
    # if request is POST
    if flask.request.method=="POST":
        # `Input` button
        if flask.request.form["Request"]=="Request":
            HEADER = "Submitted Request"
            NAME   = "UserRequest.json"
            CONTENT= getFileDiskModeR(File=SYSTEM+NAME)[:10000]
        # `Result` button
        elif flask.request.form["Request"]=="Result":
            HEADER = "Scanner Result"
            NAME   = "S3Scanner.json"
            CONTENT= getFileDiskModeR(File=SYSTEM+NAME)[:10000]
        # `Clean` button
        elif flask.request.form["Request"]=="Clean":
            NAME   = "master.log"
            # master file
            CLEANA = SYSTEM+"UserRequest.json"
            os.remove(CLEANA) if os.path.exists(CLEANA) else None
            # worker file
            CLEANB = SYSTEM+"RedShiftPI.json"
            os.remove(CLEANB) if os.path.exists(CLEANB) else None
            CLEANC = SYSTEM+"BucketList.json"
            os.remove(CLEANC) if os.path.exists(CLEANC) else None
            CLEAND = SYSTEM+"BucketSkip.json"
            os.remove(CLEAND) if os.path.exists(CLEAND) else None
            CLEANE = SYSTEM+"S3Scanner.json"
            os.remove(CLEANE) if os.path.exists(CLEANE) else None
            CLEANF = SYSTEM+"worker.log"
            os.remove(CLEANF) if os.path.exists(CLEANF) else None
            # kill the process if exist
            try:
                os.kill(PID, signal.SIGSTOP)
                MSG = "\n`S3 Scanner` killed at PID {}.".format(PID)
                PID = None
                APP.logger.critical("`S3 Scanner` killed at PID {}."
                                    .format(PID))
            # exception if process id does not exist
            except Exception as e:
                MSG = "\n`S3 Scanner` does not exist at PID {}.".format(PID)
                APP.logger.critical("`S3 Scanner` does not exist at PID {}."
                                    .format(PID))
            # clean message
            CLEANMSG = """Following files removed from disk if exists:

            * `UserRequest.json`
            * `RedShiftPI.json`
            * `BucketList.json`
            * `BucketSkip.json`
            * `S3Scanner.json`
            * `worker.log`
            """
            # display the message
            CONTENT = CLEANMSG+MSG
        # `Execute` button
        elif flask.request.form["Request"]=="Execute":
            HEADER = "Execution"
            NAME   = "worker.log"
            # check if file exists
            if os.path.exists(SYSTEM+"S3Scanner.json") or \
                                    os.path.exists(SYSTEM+"worker.log"):
                MSG = "`S3 Scanner`running/completed at PID `{}`.\n".format(PID)
                CONTENT = MSG+getFileDiskModeR(File=SYSTEM+"worker.log")[-10000:]
                APP.logger.critical("`S3 Scanner`running/completed at PID `{}`."
                                    .format(PID))
            # try to execute if file does not exists
            else:
                # execute the script
                PID = subprocess.Popen([EXECUTION,"worker.py",SYSTEM]).pid
                MSG = "`S3 Scanner` started at PID `{}`.\n".format(PID)
                CONTENT = MSG
                APP.logger.critical("`S3 Scanner` started at PID `{}`."
                                    .format(PID))
        else:
            APP.logger.critical("Other request received from submit button.")
    else:
        APP.logger.critical("Other request received apart from POST.")
    # return rendered template
    return(flask.render_template("output.html",HEADER=HEADER,TEXT=CONTENT))

@APP.route("/getDownload")
def getDownload():
    """
    Default "/getDownload" route download the active file set
    from "/dashboard" route and if file not found in the location
    it will throw exception page.
    """
    # check if file exist
    LOAD = "master.log" if not os.path.exists(SYSTEM+NAME) else NAME
    # set path to file
    FILE = SYSTEM + LOAD
    # return file as attachment_filename
    return(flask.send_file(FILE,attachment_filename=LOAD,as_attachment=True))

@APP.errorhandler(Exception)
def internal_error(Exception):
    """
    Default route and page for all the exception that will occur
    in the flask application during runtime.
    """
    # log an error for flask
    APP.logger.error(Exception)
    # return rendered template
    return(flask.render_template("500.html"))

if __name__=="__main__":
    # run app on host 0.0.0.0:8000
    APP.run(host="0.0.0.0", port=8000)
