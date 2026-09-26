"""Wire up the Google Drive connection for the CSV ingestion notebook.

Given a service-account key downloaded from Google Cloud, this:

  1. validates that the file really is a service-account key,
  2. stores it in a Databricks secret scope,
  3. writes the service-account email into connection_config.json,
  4. optionally checks that Drive can actually see your file.

The private key is never printed and never written into the repository.

Usage
-----
    python scripts/setup_gdrive_connection.py --key-file path/to/sa-key.json
    python scripts/setup_gdrive_connection.py --key-file path/to/sa-key.json \
        --verify-file Dataset1.csv
"""

import argparse
import io
import json
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(
    REPO_ROOT, "notebooks", "csv_ingestion", "config", "connection_config.json"
)

REQUIRED_KEY_FIELDS = ["type", "project_id", "private_key", "client_email", "token_uri"]


def load_key(path):
    """Read and sanity-check the service-account key without echoing its contents."""
    if not os.path.exists(path):
        sys.exit("Key file not found: {}".format(path))

    with io.open(path, encoding="utf-8") as fh:
        try:
            key = json.load(fh)
        except ValueError as exc:
            sys.exit("Key file is not valid JSON: {}".format(exc))

    missing = [f for f in REQUIRED_KEY_FIELDS if f not in key]
    if missing:
        sys.exit(
            "This does not look like a service-account key - missing {}.\n"
            "Download the JSON key from the service account, not an OAuth client ID.".format(missing)
        )
    if key.get("type") != "service_account":
        sys.exit("Expected type 'service_account', got {!r}.".format(key.get("type")))

    return key


def put_secret(scope, secret_key, key_path):
    """Store the key via stdin.

    The CLI also takes --string-value, but anything passed as an argument is visible in the
    process list to other users on the machine. stdin keeps the key out of argv entirely.
    """
    with io.open(key_path, encoding="utf-8") as fh:
        key_text = fh.read()

    result = subprocess.run(
        ["databricks", "secrets", "put-secret", scope, secret_key],
        input=key_text,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(
            "Failed to store the secret:\n{}\n"
            "Check that the scope exists: databricks secrets list-scopes".format(
                (result.stderr or result.stdout).strip()
            )
        )
    print("  Stored in secret scope {!r} under key {!r}".format(scope, secret_key))


def update_config(email, scope, secret_key):
    with io.open(CONFIG_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)

    cfg["auth"]["service_account_email"] = email
    cfg["auth"]["secret_scope"] = scope
    cfg["auth"]["secret_key"] = secret_key

    with io.open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print("  Updated {}".format(os.path.relpath(CONFIG_PATH, REPO_ROOT)))


def verify_drive_access(key_path, file_name):
    """Confirm the service account can actually see the file it will be asked to read."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        print("  Skipped: pip install google-api-python-client google-auth to enable this check.")
        return

    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    matches = (
        drive.files()
        .list(
            q="name = '{}' and trashed = false".format(file_name),
            fields="files(id, name, mimeType, size, modifiedTime)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
        .get("files", [])
    )

    if not matches:
        print(
            "  NOT VISIBLE: no file named {!r}.\n"
            "  Share the file (or its folder) with the service-account email above.".format(file_name)
        )
        return

    print("  Visible to the service account:")
    for f in matches:
        print(
            "    id={}  name={}  type={}  modified={}".format(
                f["id"], f["name"], f["mimeType"], f.get("modifiedTime")
            )
        )
    if len(matches) > 1:
        print("  More than one match - put the correct id in source_config.file.file_id.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key-file", required=True, help="Path to the service-account JSON key")
    parser.add_argument("--scope", default="gdrive", help="Databricks secret scope (default: gdrive)")
    parser.add_argument(
        "--secret-key", default="service_account_json", help="Key name within the scope"
    )
    parser.add_argument("--verify-file", help="Drive filename to check visibility for")
    args = parser.parse_args()

    key = load_key(args.key_file)
    print("Service account : {}".format(key["client_email"]))
    print("GCP project     : {}".format(key["project_id"]))
    print()

    put_secret(args.scope, args.secret_key, args.key_file)
    update_config(key["client_email"], args.scope, args.secret_key)

    if args.verify_file:
        print()
        print("Checking Drive visibility for {!r}:".format(args.verify_file))
        verify_drive_access(args.key_file, args.verify_file)

    print()
    print("Done. Keep the key file outside the repository - .gitignore does not cover it by name.")


if __name__ == "__main__":
    main()
