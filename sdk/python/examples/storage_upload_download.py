"""Upload a file then download its bytes back via the 0.3.6 streaming helper.

Demonstrates the StorageService file lifecycle on ``project.storage``:

* ``upload_file`` — RegisterUpload -> HTTP PUT the bytes -> FinalizeUpload.
* ``download_file(file_id, stream=True)`` / ``download_stream`` — stream the
  object bytes back THROUGH the broker via the server-streaming
  ``StorageService.DownloadFile`` RPC (new in 0.3.6), reassembled into the
  full file body. This is the fallback for clients that cannot egress to the
  object store; when egress is available prefer the presigned default
  (``download_file(file_id)`` returns the ``GetDownloadUrlResponse``).
"""

from udb_client import UdbConfig, UdbProject


def main() -> None:
    config = UdbConfig(
        target="127.0.0.1:50051",
        tenant_id="acme",
        project_id="default",
    )

    with UdbProject.connect(config) as project:
        # Login and adopt the canonical tenant/project from the verified
        # principal; installs the bearer across every sub-client.
        project.login_and_adopt_tenant("svc@acme", "password")

        payload = b"hello from the udb python storage example"

        # One-call upload: RegisterUpload -> PUT bytes -> FinalizeUpload.
        finalized = project.storage.upload_file(
            "greeting.txt",
            payload,
            {"content_type": "text/plain", "file_type": "example"},
        )
        file_id = finalized.file_id
        print(f"uploaded file_id={file_id}")

        # Presigned default (no bytes through the broker): returns the
        # GetDownloadUrlResponse carrying a presigned download_url.
        url_response = project.storage.download_file(file_id)
        print(f"presigned download_url={url_response.download_url}")

        # 0.3.6 streaming fallback: stream the bytes back THROUGH the broker.
        downloaded = project.storage.download_file(file_id, stream=True)
        # Equivalent direct call: project.storage.download_stream(file_id)
        assert downloaded == payload
        print(f"downloaded {len(downloaded)} bytes via streaming DownloadFile")


if __name__ == "__main__":
    main()
