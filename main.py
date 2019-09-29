import logging
from flask import Flask, request
from append_data import run_append

app = Flask(__name__)

@app.route('/aarhus/append-data') #make up memorable URL-will be used in cron job syntax
def start_traffic_append_data(): #make up memorable function name for cron job
    is_cron = request.headers.get('X-Appengine-Cron', False)
    if not is_cron:
        return('Bad Request', 400)
    try:
        run_append() #the actual name of the script/function you want to run contained in the subfolder
        return("Pipeline started", 200)
    except Exception as e:
        logging.exception(e)
        return("Error: <pre>{}</pre>".format(e), 500)

@app.route('/aarhus/test') #make up memorable URL-will be used in cron job syntax
def start_test(): #make up memorable function name for cron job
    is_cron = request.headers.get('X-Appengine-Cron', False)
    return("Test", 200)
    

        
@app.errorhandler(500) #error handling script for troubleshooting
def server_error(e):
    logging.exception('An error occurred during a request.')
    return("""
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500)


if __name__ == '__main__': #hosting administration syntax
    app.run()