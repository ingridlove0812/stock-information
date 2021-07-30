# -*- coding: UTF-8 -*-
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import sys
# os.chdir('/home/g401657316/python/poetry/')

class up_to_drive():
    def __init__(self):
        pass

    def shareWithEveryone(folderId, service):
        payload = {"role": "reader","type": "anyone"}
        service.permissions().create(fileId=folderId, body=payload).execute()

    def letmewriter(folderId, service):
        payload = {"role": "writer","type": "user",'emailAddress': '401657316@gms.tku.edu.tw'}
        service.permissions().create(fileId=folderId, body=payload).execute()

    def run_or_not():
        sc = ['https://www.googleapis.com/auth/drive.metadata.readonly']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scopes=sc)
        serv = build('drive', 'v3', credentials=credentials)
        results = serv.files().list(q="mimeType='application/vnd.google-apps.folder'",pageSize=10, fields="nextPageToken, files(name)").execute()
        return str(datetime.date.today()) in [r['name'] for r in results.get('files')]

    def delete_folder():
        sc = ['https://www.googleapis.com/auth/drive.metadata.readonly']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scopes=sc)
        serv = build('drive', 'v3', credentials=credentials)
        results = serv.files().list(q="mimeType='application/vnd.google-apps.folder'", pageSize=10,
                                    fields="nextPageToken, files(id,name)").execute()
        file_id = [r['id'] for r in results.get('files', []) if str(datetime.date.today()) in r['name']]
        service.files().delete(fileId='1sk-djpnhP6WDHsK9ButRul7LMdc9sKXu').execute()

    def get_secrets():
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secrets.json', scopes=scope)
        service = build('drive', 'v3', credentials=credentials)
        return service

    def update_files(self,service):
        folder_metadata = {'name': str(datetime.date.today()), 'mimeType': 'application/vnd.google-apps.folder',
                           'parents': ['16_gTSfImTZcGprnQjoZLy6gDgKx7diZU']}
        folder = service.files().create(body=folder_metadata, fields='id,webViewLink').execute()
        self.letmewriter(folder.get('id'), service)
        self.shareWithEveryone(folder.get('id'), service)
        print('Folder ID: %s' % folder.get('webViewLink'))

        file_metadata = {'name': '成交額_日.xlsx', 'parents': [folder.get('id')],
                         'mimeType': 'application/vnd.google-apps.spreadsheet'}
        media = MediaFileUpload('成交額_日.xlsx', mimetype='text/xlsx', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink,parents').execute()
        self.letmewriter(file.get('id'), service)
        self.shareWithEveryone(file.get('id'), service)
        print('File URL: %s' % file.get('webViewLink'))

        file_metadata = {'name': '成交額_週.xlsx', 'parents': [folder.get('id')],
                         'mimeType': 'application/vnd.google-apps.spreadsheet'}
        media = MediaFileUpload('成交額_週.xlsx', mimetype='text/xlsx', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink,parents').execute()
        self.letmewriter(file.get('id'), service)
        self.shareWithEveryone(file.get('id'), service)
        print('File URL: %s' % file.get('webViewLink'))

        file_metadata = {'name': '累計上漲價格.xlsx', 'parents': [folder.get('id')],
                         'mimeType': 'application/vnd.google-apps.spreadsheet'}
        media = MediaFileUpload('累計上漲價格.xlsx', mimetype='text/xlsx', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink,parents').execute()
        self.letmewriter(file.get('id'), service)
        self.shareWithEveryone(file.get('id'), service)
        print('File URL: %s' % file.get('webViewLink'))

        file_metadata = {'name': '月營收.xlsx', 'parents': [folder.get('id')],
                         'mimeType': 'application/vnd.google-apps.spreadsheet'}
        media = MediaFileUpload('月營收.xlsx', mimetype='text/xlsx', resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink,parents').execute()
        self.letmewriter(file.get('id'), service)
        self.shareWithEveryone(file.get('id'), service)
        print('File URL: %s' % file.get('webViewLink'))

        return folder.get('webViewLink')
