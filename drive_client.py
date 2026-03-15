"""
DriveClient — interface Google Drive.
Liste les fichiers d'un dossier, télécharge et uploade en bytes.

Authentification (par ordre de priorité) :
  1. OAuth user credentials  — GOOGLE_OAUTH_CLIENT_ID + CLIENT_SECRET + REFRESH_TOKEN
     → uploade dans le Drive personnel de l'utilisateur (quota illimité)
  2. Service account JSON    — GOOGLE_SERVICE_ACCOUNT_PATH
     → nécessite un Shared Drive (les service accounts n'ont pas de quota personnel)
"""

import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REFRESH_TOKEN,
)


def _build_credentials():
    """Retourne des credentials Google selon ce qui est configuré."""

    # ── OAuth user credentials (prioritaire) ──────────────────────
    if GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET and GOOGLE_OAUTH_REFRESH_TOKEN:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_OAUTH_REFRESH_TOKEN,
            client_id=GOOGLE_OAUTH_CLIENT_ID,
            client_secret=GOOGLE_OAUTH_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
        )
        # Force refresh to get a valid access token immediately
        creds.refresh(Request())
        print("    [Drive] Authentification OAuth utilisateur")
        return creds

    # ── Service account (fallback) ────────────────────────────────
    from google.oauth2.service_account import Credentials as SACredentials
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    print("    [Drive] Authentification service account")
    return SACredentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES
    )


class DriveClient:
    def __init__(self):
        creds = _build_credentials()
        self.service = build("drive", "v3", credentials=creds)

    def list_files(self, folder_id: str, extensions: list) -> list:
        """Retourne tous les fichiers d'un dossier. Compatible My Drive et Shared Drives."""
        ext_filters = " or ".join(f"name contains '{e}'" for e in extensions)
        query = f"'{folder_id}' in parents and trashed=false and ({ext_filters})"
        results = (
            self.service.files()
            .list(
                q=query,
                fields="files(id, name, createdTime, modifiedTime, size)",
                orderBy="createdTime desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return results.get("files", [])

    def download_file(self, file_id: str) -> bytes:
        """Télécharge un fichier Drive en bytes."""
        request = self.service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue()

    def upload_file(self, folder_id: str, name: str, content: bytes,
                    mime_type: str = "application/octet-stream") -> str:
        """Upload bytes comme nouveau fichier dans le dossier indiqué. Retourne le file ID."""
        metadata = {"name": name, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mime_type, resumable=False)
        f = (
            self.service.files()
            .create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return f.get("id", "")
