import tweepy
import os
import requests
import io
import json
import time
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# SCOPES para Drive y Sheets (lectura/escritura)
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

# Configuración del entorno
DRIVE_FOLDER_ID = "11kWyy_9aG8mAu7zbmzdNVtKviBk_iX74"  # ID de la carpeta en Drive
SHEET_ID = "15k2OocxcZ4wdSYa8qoPHUa9AA4IFXPAkZcN1E7mrdIk"  # ID de tu Google Sheet
ACCOUNTS_FILE = "twitter_accounts.json"   # Archivo con las credenciales de múltiples cuentas
LAST_ID_FILE = "last_retweeted_id.txt"    # Archivo para guardar el último tweet ID procesado
TARGET_USERNAME = "wallstwolverine"       # Usuario de Twitter a monitorear
STATE_FILE = "state.json"                 # Archivo para guardar el estado del "tambor"

# Tiempo que se bloqueará una cuenta si recibe TooManyRequests (en minutos)
BLOCK_DURATION = 16

def authenticate_google_apis():
    """Autentica con la API de Google Drive y Sheets."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service

def upload_bytes_to_drive(service, file_name, mime_type, file_bytes):
    """Sube datos binarios directamente a Google Drive sin guardar en local."""
    file_metadata = {
        "name": file_name,
        "parents": [DRIVE_FOLDER_ID]
    }
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=True)
    try:
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(f"Archivo subido a Google Drive con ID: {file.get('id')}")
    except Exception as e:
        print(f"Error al subir archivo a Drive: {e}")

def upload_from_url_to_drive(service, file_name, mime_type, url):
    """Descarga el contenido desde una URL a memoria y luego lo sube a Drive."""
    try:
        r = requests.get(url)
        if r.status_code == 200:
            upload_bytes_to_drive(service, file_name, mime_type, r.content)
        else:
            print(f"No se pudo descargar el archivo desde {url} (Status: {r.status_code})")
    except Exception as e:
        print(f"Error al descargar o subir el archivo desde {url}: {e}")

def load_last_retweeted_id():
    """Carga el último ID de tweet procesado desde el archivo."""
    if os.path.exists(LAST_ID_FILE):
        with open(LAST_ID_FILE, "r") as f:
            last_retweeted_id_str = f.read().strip()
            if last_retweeted_id_str.isdigit():
                return int(last_retweeted_id_str)
    return None

def save_last_retweeted_id(tweet_id):
    """Guarda el último ID de tweet procesado en el archivo."""
    try:
        with open(LAST_ID_FILE, "w") as f:
            f.write(str(tweet_id))
    except Exception as e:
        print(f"Error al guardar el último ID procesado: {e}")

def load_accounts():
    """Carga las cuentas de Twitter desde el archivo JSON."""
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"No se encontró el archivo {ACCOUNTS_FILE} con las credenciales de Twitter.")
        return []

    with open(ACCOUNTS_FILE, "r") as f:
        try:
            accounts_data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error al leer {ACCOUNTS_FILE}: {e}")
            return []

    if "accounts" not in accounts_data or not accounts_data["accounts"]:
        print("No se encontraron cuentas en el archivo de credenciales.")
        return []

    return accounts_data["accounts"]

def load_state(accounts):
    """Carga el estado del tambor desde state.json. 
       Si no existe, inicializa el estado por defecto.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                state_data = json.load(f)
            except json.JSONDecodeError:
                state_data = {}
    else:
        state_data = {}

    # Si no existe current_position o no es válido, inicializar a 0
    current_position = state_data.get("current_position", 0)
    if current_position < 0 or current_position >= len(accounts):
        current_position = 0

    # Cargar estados de las APIs
    api_states = state_data.get("api_states", {})

    # Asegurarnos de que todas las cuentas estén representadas
    for i in range(len(accounts)):
        str_i = str(i)
        if str_i not in api_states:
            api_states[str_i] = {"status": "available", "blocked_until": None}

    return current_position, api_states

def save_state(current_position, api_states):
    """Guarda el estado del tambor en state.json."""
    state_data = {
        "current_position": current_position,
        "api_states": api_states
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state_data, f, indent=2)

def is_account_blocked(api_state):
    """Determina si una cuenta está bloqueada actualmente."""
    if api_state["status"] == "blocked":
        if api_state["blocked_until"] is not None:
            blocked_until = datetime.fromisoformat(api_state["blocked_until"])
            if datetime.now() < blocked_until:
                # Sigue bloqueada
                return True
            else:
                # Ya pasó el tiempo de bloqueo, resetear estado
                api_state["status"] = "available"
                api_state["blocked_until"] = None
                return False
        # Si no hay blocked_until, la marcamos como available
        api_state["status"] = "available"
        return False
    return False

def block_account_temporarily(api_state):
    """Bloquea una cuenta temporalmente agregando un tiempo de desbloqueo."""
    api_state["status"] = "blocked"
    unblock_time = datetime.now() + timedelta(minutes=BLOCK_DURATION)
    api_state["blocked_until"] = unblock_time.isoformat()

def advance_position(current_position, total_accounts):
    """Avanza a la siguiente posición del tambor de manera cíclica."""
    return (current_position + 1) % total_accounts

def get_original_tweet_info(client, tweet_id):
    """Dado el ID de un tweet original, obtiene el nombre de usuario y la URL del tweet."""
    try:
        resp = client.get_tweet(
            id=tweet_id,
            expansions=["author_id"],
            tweet_fields=["author_id"]
        )
        if resp.data and resp.includes and "users" in resp.includes:
            original_user = resp.includes["users"][0].username
            original_url = f"https://twitter.com/{original_user}/status/{tweet_id}"
            return original_user, original_url
    except Exception as e:
        print(f"Error al obtener información del tweet original {tweet_id}: {e}")
    return None, None

def main():
    print("Iniciando script...")

    # Autenticar con Google
    drive_service, sheets_service = authenticate_google_apis()

    # Cargar las cuentas
    accounts = load_accounts()
    if not accounts:
        print("No hay cuentas disponibles en twitter_accounts.json. Saliendo...")
        return

    # Cargar estado del tambor
    current_position, api_states = load_state(accounts)

    last_retweeted_id = load_last_retweeted_id()
    candidate_tweet = None
    tweet_encontrado = False

    total_accounts = len(accounts)
    attempts = 0

    while attempts < total_accounts:
        idx = current_position
        account = accounts[idx]

        # Verificar si la cuenta está bloqueada
        if is_account_blocked(api_states[str(idx)]):
            print(f"Cuenta en posición {idx} está bloqueada temporalmente. Pasando a la siguiente.")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue

        print(f"\nUsando cuenta {idx+1}/{total_accounts}: {account.get('CONSUMER_KEY')[:5]}***")

        # Crear cliente con las credenciales de esta cuenta
        try:
            client = tweepy.Client(
                bearer_token=account["BEARER_TOKEN"],
                consumer_key=account["CONSUMER_KEY"],
                consumer_secret=account["CONSUMER_SECRET"],
                access_token=account["ACCESS_TOKEN"],
                access_token_secret=account["ACCESS_SECRET"]
            )
        except Exception as e:
            print(f"Error al crear el cliente de Twitter: {e}")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue

        # Obtener el usuario
        try:
            user_response = client.get_user(username=TARGET_USERNAME, user_fields=['pinned_tweet_id'])
            if user_response.data:
                print(f"Usuario encontrado: {user_response.data.username}")
            else:
                print(f"No se encontró el usuario {TARGET_USERNAME} con esta cuenta. Avanzando a siguiente.")
                current_position = advance_position(current_position, total_accounts)
                attempts += 1
                continue
        except tweepy.TooManyRequests:
            print("Error: Too Many Requests (429). Bloqueando cuenta temporalmente y rotando a la siguiente.")
            block_account_temporarily(api_states[str(idx)])
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue
        except tweepy.TweepyException as e:
            print(f"Error al obtener el usuario con esta cuenta: {e}")
            if "401" in str(e):
                print("Credenciales no autorizadas para esta cuenta. Rotando a la siguiente cuenta.")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue

        pinned_tweet_id = user_response.data.pinned_tweet_id
        print(f"ID del tweet fijado: {pinned_tweet_id}")

        # Obtener tweets recientes
        try:
            tweets_response = client.get_users_tweets(
                id=user_response.data.id,
                max_results=5,
                tweet_fields=["referenced_tweets", "in_reply_to_user_id", "created_at", "attachments", "text"],
                expansions=["attachments.media_keys","referenced_tweets.id","author_id"],
                media_fields=["url", "preview_image_url", "type", "variants"]
            )
        except tweepy.TooManyRequests:
            print("Error: Too Many Requests (429). Bloqueando cuenta temporalmente y rotando a la siguiente.")
            block_account_temporarily(api_states[str(idx)])
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue
        except tweepy.TweepyException as e:
            print(f"Error al obtener los tweets con esta cuenta: {e}")
            if "401" in str(e):
                print("Credenciales no autorizadas. Rotando a la siguiente cuenta.")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue

        if not tweets_response.data or len(tweets_response.data) == 0:
            print("El usuario no tiene tweets recientes o no se pudieron obtener con esta cuenta.")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1
            continue

        tweets = tweets_response.data
        includes = tweets_response.includes if tweets_response.includes else {}
        media_dict = {}
        if 'media' in includes:
            for m in includes['media']:
                media_dict[m.media_key] = m

        # Buscar tweet candidato
        for t in tweets:
            # Omitir tweet fijado
            if pinned_tweet_id is not None and t.id == pinned_tweet_id:
                print(f"Omitiendo tweet fijado: {t.id}")
                continue

            # Omitir respuestas
            if t.in_reply_to_user_id is not None:
                print(f"Omitiendo respuesta: {t.id}")
                continue

            # Verificar si ya fue procesado
            if last_retweeted_id is not None and t.id <= last_retweeted_id:
                print(f"Omitiendo tweet antiguo o ya procesado: {t.id}")
                break

            # Si es retweet, obtener el tweet original
            original_user = None
            original_url = None
            is_retweet = False
            if t.referenced_tweets:
                for ref in t.referenced_tweets:
                    if ref.type == "retweeted":
                        is_retweet = True
                        original_user, original_url = get_original_tweet_info(client, ref.id)
                        break

            candidate_tweet = t
            if candidate_tweet:
                print(f"Tweet candidato encontrado: {t.id}")
                break

        if candidate_tweet:
            # Procesar el tweet encontrado
            try:
                tweet_text = candidate_tweet.text
                # Si es retweet y tenemos info del original
                if is_retweet and original_user and original_url:
                    tweet_text = f"{tweet_text}\nFrom @{original_user}: {original_url}"

                # Agregar el texto del tweet a la hoja de cálculo
                new_values = [[tweet_text]]
                sheets_request = sheets_service.spreadsheets().values().append(
                    spreadsheetId=SHEET_ID,
                    range="A:A",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": new_values}
                )
                sheets_request.execute()
                print("Texto del tweet agregado a la hoja de cálculo.")

                # Subir multimedia
                if candidate_tweet.attachments and "media_keys" in candidate_tweet.attachments:
                    media_keys = candidate_tweet.attachments["media_keys"]
                    for mk in media_keys:
                        if mk in media_dict:
                            m = media_dict[mk]
                            if m.type == "photo":
                                filename = f"media_{candidate_tweet.id}_{mk}.jpg"
                                upload_from_url_to_drive(drive_service, filename, "image/jpeg", m.url)
                            elif m.type in ["animated_gif", "video"]:
                                variants = m.variants
                                mp4_variants = [v for v in variants if v.get("content_type") == "video/mp4"]
                                if not mp4_variants:
                                    print(f"No se encontraron variantes mp4 para el {m.type}.")
                                    continue
                                best_variant = max(mp4_variants, key=lambda v: v.get("bitrate", 0))
                                filename = f"media_{candidate_tweet.id}_{mk}.mp4"
                                upload_from_url_to_drive(drive_service, filename, "video/mp4", best_variant["url"])
                else:
                    print("Este tweet no contiene archivos multimedia.")
                    # Crear un archivo dummy
                    dummy_content = f"Tweet ID: {candidate_tweet.id}\nTexto: {candidate_tweet.text}\nSin multimedia."
                    if is_retweet and original_user and original_url:
                        dummy_content += f"\nFrom @{original_user}: {original_url}"

                    dummy_file = io.BytesIO(dummy_content.encode("utf-8"))
                    dummy_filename = f"dummy_{candidate_tweet.id}.txt"
                    upload_bytes_to_drive(drive_service, dummy_filename, "text/plain", dummy_file.getvalue())

                # Actualizar último ID procesado
                save_last_retweeted_id(candidate_tweet.id)
                print("Proceso completado con éxito.")
                
                # ---------------------------------------------------------------------
                # AQUÍ SE REALIZA LA LLAMADA A LA API DE MAKE PARA ACTIVAR EL ESCENARIO
                # ---------------------------------------------------------------------
                make_api_key = "83656d3c-d809-41cb-891b-7e761d5e30c6"  # Tu API Key de Make
                scenario_id = "2885800"  # ID del escenario de Make
                make_url = f"https://api.integromat.com/v2/scenarios/{scenario_id}/run"

                make_headers = {
                    "Authorization": f"Token {make_api_key}",
                    "Content-Type": "application/json"
                }

                make_response = requests.post(make_url, headers=make_headers)

                if make_response.status_code == 200:
                    print("Escenario de Make activado exitosamente.")
                else:
                    print(f"Error al activar el escenario de Make: {make_response.status_code}")
                    print(make_response.text)
                # ---------------------------------------------------------------------

                tweet_encontrado = True
                current_position = advance_position(current_position, total_accounts)
                break
            except Exception as e:
                print(f"Error procesando el tweet encontrado: {e}")
                candidate_tweet = None
                current_position = advance_position(current_position, total_accounts)
                attempts += 1
                continue
        else:
            print("No se encontró un tweet candidato con esta cuenta. No hay tweets nuevos para procesar.")
            current_position = advance_position(current_position, total_accounts)
            attempts += 1

    if not tweet_encontrado and candidate_tweet is None:
        print("No se encontraron tweets nuevos en ninguna cuenta disponible.")

    save_state(current_position, api_states)

if __name__ == "__main__":
    main()
