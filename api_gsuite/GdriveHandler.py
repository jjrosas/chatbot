"""
Module to Handle the Google DRIVE API
    Requires as App authorization json
"""

import os
import pandas as pd
import shutil
from pydrive2.drive import GoogleDrive
from pydrive2.auth import GoogleAuth
from oauth2client.service_account import ServiceAccountCredentials
import csv

class GdriveHandler():
    """
    Class to handle the Google Drive API
    """

    def __init__(self,client_secret_path:str=None,service_account_path=None,service_account_dict=None) -> None:

        """
        Initialize the Google Drive Object
        """

        gauth = GoogleAuth()
        scope = scope = ["https://www.googleapis.com/auth/drive"]
        if service_account_path:
            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_path, scope)
        elif service_account_dict:
            gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_dict, scope)
        else:
            gauth.DEFAULT_SETTINGS['client_config_file']=client_secret_path
            gauth.LocalWebserverAuth() # Creates local webserver and auto handles

        self.gauth = gauth

        self.drive = GoogleDrive(gauth)

    def get_files_list(self,folder_id:str)->str:
        """
        List gfiles in Google Drive folder
        returns: json string
        """

        file_list = self.drive.ListFile({'q':
                    f"'{folder_id}' in parents and trashed=false"}).GetList()

        return file_list

    def walk_folder(self,folder_id:str):
        base_files = self.get_files_list(folder_id)
        ouput_files = []
        for file in base_files:
            if file["mimeType"]!='application/vnd.google-apps.folder':
                ouput_files.append(file)
            elif file["mimeType"]=='application/vnd.google-apps.folder':
                #print(file['id'],'\t',file['title'])
                ouput_files.extend(self.walk_folder(file["id"]))
        return ouput_files

    def check_file_in_folder(self,folder_id:str,file_name:str)->bool:
        """
        Checks whether a file exists in a specified folder or not
        returns True if exists
        """

        file_list = self.get_files_list(folder_id)

        for file_ in file_list:
            if file_name == file_['title']:
                return True

        return False

    def upload_file(self,folder_id:str,file_path:str,public:bool=False):
        """
        Upload a file to specified folder

        returns None
        """

        file_name = file_path.split('/')[-1]

        try:
            file_drive = self.drive.CreateFile({'parents': [
                                                            {'id': folder_id}]})
            file_drive.SetContentFile(file_path)
            file_drive['title'] = file_path.split('/')[-1]
            file_drive.Upload()

            if public:
                file_drive.InsertPermission({'type':  'anyone',
                                            'value': 'anyone',
                                            'role':  'reader'})
            return file_drive.attr['metadata']

        except Exception as e:
            print(file_name,e)

    def delete_file(self,file_id:str)->None:
        """
        Deletes an specified file_id

        Currently not working
        """

        file = self.drive.CreateFile({'id':file_id})
        file.Trash()

    def download_file(self,file_id:str,destination_folder:str=None)->None:
        """
        Downloads a file to local from Google drive
        """
        file = self.drive.CreateFile({'id':file_id})
        file_name = file['title']
        mimetypes = {
        # Drive Document files as PDF
        'application/vnd.google-apps.document': 'application/pdf',

        # Drive Sheets files as MS Excel files.
        'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

        # to do: add other types
        }

        if file['mimeType'] in mimetypes:
            download_mimetype = mimetypes[file['mimeType']]
            file.GetContentFile(file['title'], mimetype=download_mimetype)

        else:
            file.GetContentFile(filename=file_name)

        if destination_folder is not None:
            shutil.move(src = os.path.join(os.getcwd(),file_name),
                        dst = os.path.join(destination_folder,file_name)
                        )

    def move_or_rename_in_gdrive(self,file_id:str=None,destination_folder_id=None,new_name=None):

        """
        Currently not working so well
        """

        file = self.drive.CreateFile({'id': file_id})
        if destination_folder_id:
            file['parents'] = [{"kind": "drive#parentReference", "id": destination_folder_id}]
        if new_name:
            file['title'] = new_name
        file.Upload()

    def copy_file(self,file_id,destination_folder_id:str,new_name:str=None,delete_old:bool=False):
        """
        Copies a file id to an specified folder location
        """

        body = {}
        body['parents'] = [{"id": destination_folder_id}]
        if new_name is not None:
            body['title'] = new_name

        response = self.drive.auth.service.files().copy(fileId=file_id,
                                             body=body).execute()

        if delete_old:
            self.delete_file(file_id)

        return response

    def _get_metadata(self,file):
        email = file.get("lastModifyingUser").get("emailAddress")
        download_url = file.get("downloadUrl")
        file_created_at = pd.to_datetime(file.get("createdDate"))
        file_name = file.get("title")
        file_updated_at = pd.to_datetime(file.get("modifiedDate"))
        file_original_name = file.get("originalFilename")
        metadata_dict = {
            "file_uploaded_by":email,
            "file_download_url":download_url,
            "file_uploaded_at":file_created_at,
            "file_modified_at":file_updated_at,
            "file_name":file_name,
            "file_original_name":file_original_name
        }

        return metadata_dict

    def get_df_from_file(self,file_id:str,add_metadata=False,keep_file=False)->pd.DataFrame:
        file = self.drive.CreateFile({'id':file_id})
        file_name = file['title']
        self.download_file(file_id)

        metadata = {}
        if add_metadata:
            metadata = self._get_metadata(file)

        try:
            if file_name.endswith('xlsx'):
                return pd.read_excel(file_name).assign(**metadata)

            with open(file_name,'r',encoding="utf8") as f:
                first_line = f.readlines()[0]
                if first_line == []:
                    sep = ","
                else:
                    sep = csv.Sniffer().sniff(first_line).delimiter
                    sep = sep if not sep.isalpha() else ','

            return pd.read_csv(file_name,sep=sep).assign(**metadata)
        except:
            try:
                return pd.read_excel(file_name,dtype=str).assign(**metadata)
            except Exception as e:
                raise e
        finally:
            if not keep_file:
                os.remove(file_name)



    def rename_file(self,file_id:str,new_name:str)->None:
        """
        Renames a file id
        """

        file = self.drive.auth.service.files().get(fileId=file_id).execute()
        file['title']=new_name
        update = self.drive.auth.service.files().update(fileId=file_id,body=file).execute()

    def clean_folder(self,folder_id:str):

        """
        Delete all files from a folder
        """

        list_files = self.get_files_list(folder_id)

        if len(list_files)>0:
            for file_ in  list_files:
                file_id = file_.attr.get('metadata').get('id')
                if file_id is not None:
                    self.delete_file(file_id)