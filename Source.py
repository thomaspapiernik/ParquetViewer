from flask import Flask , render_template, request, send_from_directory, send_file
import pandas as pd
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))   # refers to application_top
csvfolderpath = os.path.join(APP_ROOT, 'OutputFolder')
@app.route('/files/')
def home():
    files = os.listdir(csvfolderpath)
    dirs = get_dirs(csvfolderpath, files)

    return render_template('index.html', current_root='', files=files, dirs=dirs, fileName='')

@app.route('/files/Login')
def Login():
    return render_template('login.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/files/download/<path:path>')
def download_file (path):
    try:
        filename = os.path.join(app.root_path, 'OutputFolder', path)
        if '.parquet' in filename:
            filename = to_csv(filename)
        return send_file(filename, as_attachment=True)
    except Exception as err:
        error='Error while opening file {}'.format(path)
        print(err)
        return render_template('index.html', error=error)


@app.route('/files/<path:path>')
def show(path):
    csvFile = os.path.join(csvfolderpath, path)
    current = Path(csvFile)
    data = None
    error = None
    root_dir = None
    files = dirs = None
    table = None
    try:
        if '.csv' in csvFile:
            table = pd.read_csv(csvFile, sep=';')
        if '.parquet' in csvFile:
            table = pd.read_parquet(csvFile,  engine='pyarrow')
    except Exception as err:
        print(err)
        error='Error while opening file {}'.format(path)

    root_dir = get_root(path)
    if current.is_dir():
        files = os.listdir(csvFile)
        dirs = get_dirs(csvFile, files)
        path = ''
    else:
        if table is not None and not table.empty:
            data = table.to_html()
        files = os.listdir(os.path.dirname(csvFile))
        dirs = get_dirs(os.path.dirname(csvFile), files)

    return render_template('index.html', current_root=root_dir, files=files, dirs=dirs, fileName=path, data=data, error=error)

def get_root(path):
    root = os.path.dirname(os.path.dirname(path))
    if root is '/' or root is '':
        return '/files/'
    return '/files' + os.path.sep + root + os.path.sep

def get_dirs(root, paths):
    result = []
    for path in paths:
        if Path(os.path.join(root, path)).is_dir():
            result.append(path)
    return result

def to_csv(filename):
    if 'parquet' in filename:
        table = pd.read_parquet(filename,  engine='pyarrow')
        filename = os.path.splitext(filename)[0]+'.csv'
        table.to_csv(filename)
    return filename

if __name__ == '__main__':
    app.run()
