from pydrive2.auth import GoogleAuth

gauth = GoogleAuth()
credentials_file_path = '/home/spider/etl/workshop_02/main/client_secrets.json'
gauth.LoadCredentialsFile(credentials_file_path) # Creates local webserver and auto handles authentication.

# Si no hay credenciales cargadas, realiza la autenticación
if gauth.credentials is None:
    gauth.LocalWebserverAuth()  # Abre una ventana del navegador para autenticación

# Guarda las credenciales (esto actualiza el archivo client_secrets.json si es necesario)
gauth.SaveCredentialsFile(credentials_file_path)