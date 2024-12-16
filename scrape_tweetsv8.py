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
DRIVE_FOLDER_ID = "11kWyy_9aG8mAu7zbmzdNVtKviBk_iX74"
SHEET_ID = "15k2OocxcZ4wdSYa8qoPHUa9AA4IFXPAkZcN1E7mrdIk"
ACCOUNTS_FILE = "twitter_accounts.json"
LAST_ID_FILE = "last_retweeted_id.txt"
TARGET_USERNAME = "wallstwolverine"
STATE_FILE = "state.json"

# Tiempo que se bloqueará una cuenta si recibe TooManyRequests (en minutos)
BLOCK_DURATION = 16

# Webhook de Make
MAKE_WEBHOOK_URL = "https://hook.eu2.make.com/mpzeit7kyquw3oh8p449ijhvjvtqqthn"


def authenticate_google_apis():
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
    try:
        r = requests.get(url)
        if r.status_code == 200:
            upload_bytes_to_drive(service, file_name, mime_type, r.content)
        else:
            print(f"No se pudo descargar el archivo desde {url} (Status: {r.status_code})")
    except Exception as e:
        print(f"Error al descargar o subir el archivo desde {url}: {e}")

def load_last_retweeted_id():
    if os.path.exists(LAST_ID_FILE):
        with open(LAST_ID_FILE, "r") as f:
            last_retweeted_id_str = f.read().strip()
            if last_retweeted_id_str.isdigit():
                return int(last_retweeted_id_str)
    return None

def save_last_retweeted_id(tweet_id):
    try:
        with open(LAST_ID_FILE, "w") as f:
            f.write(str(tweet_id))
    except Exception as e:
        print(f"Error al guardar el último ID procesado: {e}")

def load_accounts():
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
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                state_data = json.load(f)
            except json.JSONDecodeError:
                state_data = {}
    else:
        state_data = {}

    current_position = state_data.get("current_position", 0)
    if current_position < 0 or current_position >= len(accounts):
        current_position = 0

    api_states = state_data.get("api_states", {})
    for i in range(len(accounts)):
        str_i = str(i)
        if str_i not in api_states:
            api_states[str_i] = {"status": "available", "blocked_until": None}

    return current_position, api_states

def save_state(current_position, api_states):
    state_data = {
        "current_position": current_position,
        "api_states": api_states
    }
    with open(STATE_FILE, "w") as f:
        json.dump(state_data, f, indent=2)

def is_account_blocked(api_state):
    if api_state["status"] == "blocked":
        if api_state["blocked_until"] is not None:
            blocked_until = datetime.fromisoformat(api_state["blocked_until"])
            if datetime.now() < blocked_until:
                return True
            else:
                api_state["status"] = "available"
                api_state["blocked_until"] = None
                return False
        api_state["status"] = "available"
        return False
    return False

def block_account_temporarily(api_state):
    api_state["status"] = "blocked"
    unblock_time = datetime.now() + timedelta(minutes=BLOCK_DURATION)
    api_state["blocked_until"] = unblock_time.isoformat()

def advance_position(current_position, total_accounts):
    return (current_position + 1) % total_accounts

def main():
    print("Iniciando script...")

    drive_service, sheets_service = authenticate_google_apis()
    accounts = load_accounts()
    if not accounts:
        print("No hay cuentas disponibles en twitter_accounts.json. Saliendo...")
        return

    current_position, api_states = load_state(accounts)
    last_retweeted_id = load_last_retweeted_id()

    total_accounts = len(accounts)
    attempts = 0
    tweet_encontrado = False

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
            if not user_response.data:
                print(f"No se encontró el usuario {TARGET_USERNAME} con esta cuenta. Deteniendo sin rotar más.")
                break
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
            break

        tweets = tweets_response.data
        includes = tweets_response.includes if tweets_response.includes else {}
        media_dict = {}
        if 'media' in includes:
            for m in includes['media']:
                media_dict[m.media_key] = m

        candidate_tweet = None

        for t in tweets:
            # Omitir tweet fijado
            if pinned_tweet_id is not None and t.id == pinned_tweet_id:
                print(f"Omitiendo tweet fijado: {t.id}")
                continue

            # Omitir respuestas
            if t.in_reply_to_user_id is not None:
                print(f"Omitiendo respuesta: {t.id}")
                continue

            # Omitir retweets
            if t.referenced_tweets:
                is_retweet = False
                for ref in t.referenced_tweets:
                    if ref.type == "retweeted":
                        is_retweet = True
                        break
                if is_retweet:
                    print(f"Omitiendo retweet: {t.id}")
                    continue

            # Omitir tweets ya procesados o antiguos
            if last_retweeted_id is not None and t.id <= last_retweeted_id:
                print(f"Omitiendo tweet antiguo o ya procesado: {t.id}")
                continue

            # Si llegamos aquí, es un tweet normal (sin ser respuesta, retweet o fijado)
            candidate_tweet = t
            print(f"Tweet candidato encontrado: {t.id}")
            break

        if candidate_tweet:
            try:
                tweet_text = candidate_tweet.text

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

                # Subir multimedia (o dummy si no hay)
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
                    dummy_content = f"Tweet ID: {candidate_tweet.id}\nTexto: {candidate_tweet.text}\nSin multimedia."
                    dummy_file = io.BytesIO(dummy_content.encode("utf-8"))
                    dummy_filename = f"dummy_{candidate_tweet.id}.txt"
                    upload_bytes_to_drive(drive_service, dummy_filename, "text/plain", dummy_file.getvalue())

                # Actualizar último ID procesado
                save_last_retweeted_id(candidate_tweet.id)
                print("Proceso completado con éxito.")

                # Llamar al webhook de Make
                try:
                    webhook_payload = {
                        "tweet_id": candidate_tweet.id,
                        "tweet_text": tweet_text
                    }
                    make_response = requests.post(MAKE_WEBHOOK_URL, json=webhook_payload)
                    if make_response.status_code == 200:
                        print("Webhook de Make activado exitosamente.")
                    else:
                        print(f"Error al llamar al webhook de Make: {make_response.status_code}")
                        print(make_response.text)
                except Exception as e:
                    print(f"Error al activar el webhook de Make: {e}")

                tweet_encontrado = True
                # Ya encontramos y procesamos un tweet
                break

            except Exception as e:
                print(f"Error procesando el tweet encontrado: {e}")
                current_position = advance_position(current_position, total_accounts)
                attempts += 1
                continue
        else:
            # No se encontró un tweet candidato válido que no sea reply, retweet o fijado
            print("No se encontró un tweet candidato válido. No se probarán otras cuentas.")
            break

    if not tweet_encontrado:
        print("No se encontraron tweets nuevos en ninguna cuenta disponible.")

    save_state(current_position, api_states)


if __name__ == "__main__":
    main()
