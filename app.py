from flask import Flask, render_template, request, redirect, flash, send_from_directory
import os
import concurrent.futures
import pandas as pd
import json
import requests
from werkzeug.utils import secure_filename
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from tqdm import tqdm
import warnings

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with your actual secret key

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ['csv', 'xlsx', 'json']

def validate_emails(df):
    responsess = []
    mail_validation = {}
    ua = UserAgent()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i, row in df.iterrows():
            email = str(row.get('DirectEmail', ''))
            link = row.get('Source', '')
            if pd.isna(email) or pd.isna(link):
                mail_validation[i] = 0
                responsess.append('')
                continue
            headers = {'User-Agent': ua.random}
            futures.append(executor.submit(requests.get, link, headers=headers))
        for i, future in enumerate(tqdm(futures)):
            try:
                response = future.result()
                if isinstance(response, requests.Response):
                    if response.status_code == 404:
                        mail_validation[i] = 0
                    else:
                        with warnings.catch_warnings():
                            warnings.filterwarnings("ignore", category=Warning)
                            soup = BeautifulSoup(response.content, 'html5lib')
                        text = soup.get_text()
                        if '@' in text:
                            mail_validation[i] = 1
                        else:
                            mail_validation[i] = 0
                    responsess.append(response)
                elif isinstance(response, str):
                    mail_validation[i] = -1
                    responsess.append("Request Error")
                else:
                    mail_validation[i] = 0
                    responsess.append('')
            except requests.exceptions.RequestException:
                mail_validation[i] = -1
                responsess.append("Request Error")
    
    return pd.Series(mail_validation), pd.Series(responsess)

def process_file(filename):
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if filename.lower().endswith('.csv'):
        df = pd.read_csv(file_path, encoding='utf-8', decimal=',')
    elif filename.lower().endswith('.xlsx'):
        df = pd.read_excel(file_path)
    elif filename.lower().endswith('.json'):
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        df = pd.DataFrame(data)

    mail_validation, responsess = validate_emails(df)

    df["valid_email"] = mail_validation
    df["Response_Type"] = responsess

    # Create the 'processed' directory if it doesn't exist
    os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

    # Determine the processed file path
    processed_filename = filename.rsplit('.', 1)[0] + '_processed.' + filename.rsplit('.', 1)[1]
    processed_output_path = os.path.join(app.config['PROCESSED_FOLDER'], processed_filename)

    # Write the DataFrame to an Excel file with a new sheet for processed data
    with pd.ExcelWriter(processed_output_path, engine='openpyxl') as writer:
        if filename.lower().endswith('.csv') or filename.lower().endswith('.json'):
            df.to_excel(writer, sheet_name='ProcessedData', index=False)
        elif filename.lower().endswith('.xlsx'):
            df.to_excel(writer, sheet_name='ProcessedData', index=False)
            summary_df = pd.DataFrame({
                'Total data': [len(df)],
                'Total email validated': [len(df.loc[df['valid_email'] == 1])],
                'Total email could not validated': [len(df.loc[df['valid_email'] == 0])],
                'Total no of errors': [len(df.loc[df['valid_email'] == -1])],
            })
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

    summary = {
        "filename": processed_filename,
        "total": len(df),
        "valid_emails": len(df.loc[df['valid_email'] == 1]),
        "invalid_emails": len(df.loc[df['valid_email'] == 0]),
        "request_errors": len(df.loc[df['valid_email'] == -1]),
    }
    return summary



@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        file = request.files['file']
        option = request.form['option']

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            if option == 'store':
                flash('File stored successfully!')
            elif option == 'process':
                process_file(filename)
                flash('File processed and validated successfully!')

            return redirect('/view')

    return render_template('upload.html')

@app.route('/view', methods=['GET'])
def view():
    uploaded_files = os.listdir(app.config['UPLOAD_FOLDER'])
    processed_files = os.listdir(app.config['PROCESSED_FOLDER'])
    return render_template('view.html', uploaded_files=uploaded_files, processed_files=processed_files)

@app.route('/process/<filename>', methods=['GET'])
def process(filename):
    process_file(filename)
    flash('File processed and validated successfully!')
    return redirect('/view')

@app.route('/delete/<filename>', methods=['GET'])
def delete(filename):
    uploaded_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    processed_file_path = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    
    if os.path.exists(uploaded_file_path):
        os.remove(uploaded_file_path)
        flash('File deleted successfully!')

    if os.path.exists(processed_file_path):
        os.remove(processed_file_path)
        flash('Processed file deleted successfully!')

    return redirect('/view')


@app.route('/download/uploaded/<filename>', methods=['GET'])
def download_uploaded_file(filename):
    uploaded_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(uploaded_file_path):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)
    else:
        return "File not found", 404

@app.route('/download/processed/<filename>', methods=['GET'])
def download_processed_file(filename):
    processed_file_path = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    if os.path.exists(processed_file_path):
        return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)
    else:
        return "File not found", 404


if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)
    app.run(host='0.0.0.0', port=5000)
