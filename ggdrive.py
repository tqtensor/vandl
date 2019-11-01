import requests
import os
import os.path
import pickle
from collections import Counter

import pandas as pd
import psycopg2
from apiclient import errors
from apiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

import config


# If modifying these scopes, delete the file token.pickle
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive']


def upload_operator(patterns):

    def table_zip(pattern):
        conn = psycopg2.connect(config.conn_string)
        df = pd.read_sql_query(f'SELECT * FROM {pattern}', con=conn)
        df.to_csv(f'./data/cryptocean_{pattern}.csv.bz2',
                  compression='bz2', index=False)
        conn.close()

    def api_auth():
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('drive', 'v3', credentials=creds)
        return service

    def retrieve_all_files(service):
        page_token = None
        while True:
            try:
                param = {'q': "'root' in parents and trashed = false"}
                if page_token:
                    param['pageToken'] = page_token
                files = service.files().list(**param).execute()
                files_list = files.get('files', None)
                result = [[file['id'], file['name']] for file in files_list]

                page_token = files.get('nextPageToken')
                if not page_token:
                    break
            except errors.HttpError as e:
                print(getattr(e, "message", repr(e)))
                break
        return result

    def delete_duplicate(service, result, pattern):
        names = [file[1] for file in result if pattern in file[1]]
        file_ids = [file[0] for file in result if pattern in file[1]]

        try:
            if len(Counter(names).items()) > 1:
                # Delete duplicate files with matching pattern
                for file_id in file_ids:
                    service.files().delete(fileId=file_id).execute()
        except errors.HttpError as e:
            print(getattr(e, "message", repr(e)))

    def upload_file(service, file_path, pattern):
        service = api_auth()
        try:
            names = [file[1] for file in result if pattern in file[1]]
            file_ids = [file[0] for file in result if pattern in file[1]]

            try:
                if len(Counter(names).items()) == 1:
                    # Update existing file #

                    # First retrieve the file from the API
                    file_id = file_ids[0]
                    print(f'>>> Preparing media for file_id: {file_id}')
                    media = MediaFileUpload(file_path,
                                            mimetype='*/*',
                                            resumable=True)

                    # Send the request to the API
                    print(f'>>> Sending API request to update existing file')
                    service.files().update(
                        fileId=file_id,
                        media_body=media).execute()
                elif len(Counter(names).items()) == 0:
                    # Upload new file #
                    file_metadata = {
                        'name': file_path,
                        'mimeType': '*/*'
                    }
                    print(f'>>> Preparing media for file_id: {file_id}')
                    media = MediaFileUpload(file_path,
                                            mimetype='*/*',
                                            resumable=True)

                    # Send the request to the API
                    print(f'>>> Sending API request to upload new file')
                    service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id').execute()
            except errors.HttpError as e:
                print(getattr(e, "message", repr(e)))

        except errors.HttpError as e:
            print(getattr(e, "message", repr(e)))

    for pattern in patterns:
        # Compress data tables to zip files
        table_zip(pattern)

        pattern = f'cryptocean_{pattern}'
        file_path = f'./data/{pattern}.csv.bz2'

        # Upload zip files to Google Drive via API
        service = api_auth()
        result = retrieve_all_files(service)
        delete_duplicate(service, result, pattern)
        upload_file(service, file_path, pattern)


def download_operator(file_ids, patterns):

    def download_file_from_google_drive(id, file_path):
        URL = "https://docs.google.com/uc?export=download"

        session = requests.Session()

        response = session.get(URL, params={'id': id}, stream=True)
        token = get_confirm_token(response)

        if token:
            params = {'id': id, 'confirm': token}
            response = session.get(URL, params=params, stream=True)

        save_response_content(response, file_path)

    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def save_response_content(response, file_path):
        CHUNK_SIZE = 32768

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(CHUNK_SIZE):
                if chunk:  # Filter out keep-alive new chunks
                    f.write(chunk)

    # Get file_id from the shared link of the file
    for i in range(len(file_ids)):
        file_id = file_ids[i]
        pattern = f'cryptocean_{patterns[i]}'
        file_path = f'./data/{pattern}.csv.bz2'
        download_file_from_google_drive(file_id, file_path)


if __name__ == '__main__':
    upload_operator(['historical_price', 'ticker'])
