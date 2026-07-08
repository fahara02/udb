## VaultService
_proto: core/vault/services/v1/vault_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateTransitKey | MUTATION | CreateTransitKeyRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_create_key_name>", "algorithm": "aes256-gcm-siv" }` | creates a disposable transit key; the read/crypto fixtures use `<seed:vault_key_name>` so this mutation does not collide with the preseeded key. |
| [ ] | Decrypt | READ_ONLY | DecryptRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>", "ciphertext": "<seed:vault_ciphertext>" }` | decrypts the seeded transit ciphertext envelope. |
| [ ] | DeleteSecret | MUTATION | DeleteSecretRequest | `{ "tenant_id": "<seed:tenant_id>", "secret_path": "<seed:vault_delete_secret_path>" }` | soft-deletes the seeded disposable secret path. |
| [ ] | DestroySecret | DESTRUCTIVE | DestroySecretRequest | `{ "tenant_id": "<seed:tenant_id>", "secret_path": "<seed:vault_destroy_secret_path>", "confirmation_token": "destroy" }` | destructive crypto-shred for the seeded disposable secret path. |
| [ ] | Encrypt | MUTATION | EncryptRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>", "plaintext": "perf" }` | encrypts a small transit plaintext. |
| [ ] | GenerateDatabaseCredentials | MUTATION | GenerateDatabaseCredentialsRequest | `{ "tenant_id": "<seed:tenant_id>", "role_name": "<seed:vault_db_role>", "ttl_seconds": 900 }` | requests the seeded dynamic database role. |
| [ ] | GetSecret | READ_ONLY | GetSecretRequest | `{ "tenant_id": "<seed:tenant_id>", "secret_path": "<seed:vault_secret_path>", "version": 0 }` | reads the seeded secret version. |
| [ ] | Hmac | MUTATION | HmacRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>", "input": "perf" }` | computes a transit HMAC over a small input. |
| [ ] | ListSecrets | READ_ONLY | ListSecretsRequest | `{ "tenant_id": "<seed:tenant_id>", "path_prefix": "app/", "page": 1, "page_size": 50 }` | lists secrets under the seeded application prefix. |
| [ ] | PutSecret | MUTATION | PutSecretRequest | `{ "tenant_id": "<seed:tenant_id>", "secret_path": "<seed:vault_put_secret_path>", "secret_value": "perf-secret", "expected_version": 0, "metadata_json": "{}" }` | writes a disposable secret path with compare-and-swap; GetSecret uses `<seed:vault_secret_path>` so PutSecret does not conflict with the preseeded read fixture. |
| [ ] | RotateTransitKey | MUTATION | RotateTransitKeyRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>" }` | rotates the seeded transit key. |
| [ ] | SealStatus | READ_ONLY | SealStatusRequest | `{ "tenant_id": "<seed:tenant_id>" }` | reads the tenant vault seal status. |
| [ ] | Sign | MUTATION | SignRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>", "input": "perf" }` | signs a small transit input. |
| [ ] | Verify | READ_ONLY | VerifyRequest | `{ "tenant_id": "<seed:tenant_id>", "key_name": "<seed:vault_key_name>", "input": "perf", "signature": "<seed:vault_signature>" }` | verifies the seeded transit signature. |
