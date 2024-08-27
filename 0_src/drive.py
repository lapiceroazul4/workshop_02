from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotUploadedError

credentials_directory = '/home/spider/etl/workshop_02/main/credentials_module.json'

# LOGIN
def login():
    """
    Authenticates the user with Google Drive using the credentials file.
    
    Returns:
        GoogleDrive: An authenticated GoogleDrive instance.
    """
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_directory
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_directory)
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_directory)
    credentials = GoogleDrive(gauth)
    return credentials

def upload_file(file_path, folder_id):
    """
    Uploads a file to a specified folder in Google Drive.
    
    Args:
        file_path (str): The path to the file to be uploaded.
        folder_id (str): The ID of the folder in Google Drive where the file will be uploaded.
    """
    credentials = login()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink", "id": folder_id}]})
    file['title'] = file_path.split("/")[-1]
    file.SetContentFile(file_path)
    file.Upload()

def download_file_by_id(drive_id, download_path):
    """
    Downloads a file from Google Drive using its ID.
    
    Args:
        drive_id (str): The ID of the file in Google Drive.
        download_path (str): The path where the file will be downloaded.
    """
    credentials = login()
    file = credentials.CreateFile({'id': drive_id}) 
    file_name = file['title']
    file.GetContentFile(download_path + file_name)

if __name__ == "__main__":
    login()
    upload_file()