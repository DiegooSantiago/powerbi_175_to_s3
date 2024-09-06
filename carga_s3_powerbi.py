import os
import boto3
from datetime import datetime

# Versión dcruz_240906

def log_message(message, log_file_path):
    """Registra un mensaje en el archivo log."""
    current_time = datetime.now().strftime('%y-%m-%d %H:%M:%S')
    log_entry = f"{current_time} - {message}\n"
    
    try:
        # Crear el archivo log si no existe, y escribir el log
        with open(log_file_path, 'a') as log_file:
            log_file.write(log_entry)
    except Exception as e:
        print(f"Error al registrar el log: {e}")

def upload_file_to_s3(file_path, bucket_name, object_name, log_file_path):
    """Sube un archivo a un bucket de S3."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        
        # Registrar en el archivo log
        log_message(f"Archivo '{object_name}' subido exitosamente a '{bucket_name}'.", log_file_path)
        return True  # Indicar que la subida fue exitosa

    except Exception as e:
        log_message(f"Error al subir el archivo '{object_name}' a S3: {e}", log_file_path)
        return False  # Indicar que ocurrió un error

def move_files(trig_path, csv_path, destination_folder, log_file_path):
    """Mueve archivos al folder de destino."""
    try:
        os.makedirs(destination_folder, exist_ok=True)  # Crea la carpeta si no existe

        # Verificar y eliminar archivos existentes en el destino antes de mover
        destination_trig_path = os.path.join(destination_folder, os.path.basename(trig_path))
        destination_csv_path = os.path.join(destination_folder, os.path.basename(csv_path))

        if os.path.exists(destination_trig_path):
            os.remove(destination_trig_path)
        
        if os.path.exists(destination_csv_path):
            os.remove(destination_csv_path)

        # Mover archivos
        os.rename(trig_path, destination_trig_path)
        os.rename(csv_path, destination_csv_path)
        
        log_message(f"Archivos '{os.path.basename(trig_path)}' y '{os.path.basename(csv_path)}' movidos a '{destination_folder}'.", log_file_path)
        
    except Exception as e:
        log_message(f"Error al mover los archivos '{os.path.basename(trig_path)}' y '{os.path.basename(csv_path)}': {e}", log_file_path)

def monitor_directory(folder_path, bucket_name, destination_folder, log_file_path):
    """Monitorea la carpeta y realiza las operaciones una vez."""
    try:
        # Lista los archivos en la carpeta
        for filename in os.listdir(folder_path):
            if filename.endswith('.trig'):
                trig_path = os.path.join(folder_path, filename)
                csv_path = os.path.join(folder_path, filename.replace('.trig', '.csv'))

                # Verifica si el archivo CSV correspondiente existe
                if os.path.exists(csv_path):
                    # Subir archivo CSV al bucket de S3
                    upload_successful = upload_file_to_s3(csv_path, bucket_name, filename.replace('.trig', '.csv'), log_file_path)
                    
                    # Mover archivos a la carpeta de transferidos solo si la subida fue exitosa
                    if upload_successful:
                        move_files(trig_path, csv_path, destination_folder, log_file_path)
                    else:
                        log_message(f"Subida fallida. Archivos '{filename}' no movidos.", log_file_path)

    except Exception as e:
        log_message(f"Error durante la ejecución: {e}", log_file_path)

if __name__ == '__main__':
    # Definir rutas y nombres de bucket
    folder_path = r'C:\FTP\powerbi'
    bucket_name = 'powerbigim'
    destination_folder = r'C:\FTP\powerbi\transferidos'
    log_file_path = r'C:\FTP\powerbi\log.log'

    # Ejecutar la función de monitoreo una vez
    monitor_directory(folder_path, bucket_name, destination_folder, log_file_path)
