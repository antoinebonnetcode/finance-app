"""
DriveClient — interface Google Drive
Liste les fichiers d'un dossier, telecharge et uploade en bytes.
"""

import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.service_account import Credentials

from config import GOOGLE_SERVICE_ACCOUNT_JSON

SCOPES = [
    "https://www.googleapis.com/auth/drive",      # read + write (requis pour upload)
    "https://www.googleapis.com/auth/spreadsheets",
]


class DriveClient:
    def __init__(self):
        creds = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=creds)

    def list_files(self, folder_id: str, extensions: list) -> list:
        """Retourne tous les fichiers d'un dossier avec les extensions donnees."""
        ext_filters = " or ".join(
            [f"name contains '{ext}'" for ext in extensions]
        )
        query = f"'{folder_id}' in parents and trashed=false and ({ext_filters})"

        results = (
            self.service.files()
            .list(
                q=query,
                fields="files(id, name, createdTime, modifiedTime, size)",
                orderBy="createdTime desc",
            )
            .execute()
        )
        return results.get("files", [])

    def download_file(self, file_id: str) -> bytes:
        """Telecharge un fichier Drive en bytes."""
        request = self.service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue()

    def upload_file(self, folder_id: str, name: str, content: bytes,
                    mime_type: str = "application/octet-stream") -> str:
        """Upload bytes comme nouveau fichier dans le dossier indique. Retourne le file ID."""
        metadata = {"name": name, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=False)
        f = (
            self.service.files()
            .create(body=metadata, media_body=media, fields="id")
            .execute()
        )
        return f.get("id", "")
