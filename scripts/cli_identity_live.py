#!/usr/bin/env python3
"""CI-only real CLI/native identity proof; upload the redacted receipt only.

All RPCs use a verified operator, all accounts/keys are test-owned, and every
identity operation is performed by the actual compiled `udb` executable.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import uuid


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    binary = Path(args.bin).resolve(strict=True)
    receipt = {
        "schema": 1, "proof": "real-executable-cli-identity",
        "source_sha": os.environ["GITHUB_SHA"],
        "binary_sha256": sha(binary.read_bytes()), "cases": [],
        "success": False, "cleanup_success": False,
        "runtime_proof": True, "public_rpc_count_changed": False,
    }
    # Imports happen only inside actual CI execution, never for source parsing.
    import grpc
    import psycopg
    from udb.core.authn.services.v1 import core_pb2 as authn
    from udb.core.authn.services.v1 import authn_service_pb2 as grants
    from udb.core.authn.services.v1 import authn_service_pb2_grpc as auth_grpc
    from udb.core.authn.entity.v1 import enums_pb2 as user_enum
    from udb.core.apikey.services.v1 import core_pb2 as api
    from udb.core.apikey.services.v1 import apikey_service_pb2_grpc as api_grpc
    from udb.core.apikey.entity.v1 import enums_pb2 as api_enum
    from udb.core.common.v1 import types_pb2 as common

    env = dict(os.environ)
    target = env["UDB_AUTH_GRPC_TARGET"]
    channel = grpc.insecure_channel(target)
    auth = auth_grpc.AuthnServiceStub(channel)
    keys = api_grpc.ApiKeyServiceStub(channel)
    connection = psycopg.connect(env["UDB_PG_DSN"], autocommit=True)
    private = Path(tempfile.mkdtemp(prefix="udb-o4-proof-"))
    private.chmod(0o700)
    prefix = "o4-" + uuid.uuid4().hex
    passwords = [env["O4_OPERATOR_PASSWORD"], "IdentitySvc1!" + secrets.token_hex(12)]
    sensitive: list[str] = list(passwords)
    owned_accounts: set[str] = set()
    owned_keys: set[str] = set()
    tenant = env["UDB_LIVE_TENANT"]
    project = env["UDB_LIVE_PROJECT"]
    actor = ""
    metadata: tuple = ()
    start = time.monotonic()

    def safe_capture(text: str) -> None:
        assert not any(secret and secret in text for secret in sensitive), "credential reached CLI output"
        assert not re.search(r"udbk_[A-Za-z0-9]+\.[A-Za-z0-9_-]+", text), "plain API key reached CLI output"

    def invoke(command: list[str], expected: int = 0) -> dict | str:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120, check=False)
        safe_capture(result.stdout + result.stderr)
        assert result.returncode == expected, f"CLI exit {result.returncode}; expected {expected}"
        if expected:
            return result.stderr
        parsed = json.loads(result.stdout)
        return parsed

    def cli(document: dict, apply: bool, expected: int = 0, transfer: bool = False) -> dict | str:
        declaration = private / "identities.json"
        declaration.write_text(json.dumps(document), encoding="utf-8")
        declaration.chmod(0o600)
        command = [str(binary), "identity", "apply" if apply else "diff", "-f", str(declaration), "--tenant", tenant]
        if transfer:
            command.append("--allow-transfer")
        parsed = invoke(command, expected)
        if not expected:
            assert parsed["tenant_id"] == tenant and parsed["applied"] is apply
        return parsed

    def state() -> dict:
        # Native tables only, fixture-prefix bounded. Never expose key hashes or
        # credentials: hashes here bind business state, not grant authority.
        rows = {}
        with connection.cursor() as cur:
            cur.execute("SELECT user_id::text,tenant_id,project_id,account_kind,status,username,email FROM udb_authn.users WHERE username LIKE %s ORDER BY user_id", (prefix + "%",))
            users = cur.fetchall()
            ids = [r[0] for r in users]
            rows["accounts"] = users
            if ids:
                cur.execute("SELECT user_id::text,service_identity,tenant_id,project_id,approved_scopes_json::text,status,revision FROM udb_authn.service_account_grants WHERE user_id::text=ANY(%s) ORDER BY user_id", (ids,))
                rows["grants"] = cur.fetchall()
                cur.execute("SELECT key_prefix,owner_id,tenant_id,project_id,name,scopes_json::text,status,key_hash,metadata_json::text,deleted_at::text FROM udb_authn.api_keys WHERE owner_id=ANY(%s) ORDER BY key_prefix", (ids,))
                key_rows = cur.fetchall()
                rows["keys"] = key_rows
                owned_keys.update(r[0] for r in key_rows)
            else:
                rows.update(grants=[], keys=[])
            owned_accounts.update(ids)
        return {"sha256": sha(json.dumps(rows, sort_keys=True).encode()), "accounts": len(rows["accounts"]), "grants": len(rows["grants"]), "keys": len(rows["keys"])}

    def case(name: str, **evidence: object) -> None:
        receipt["cases"].append({"name": name, "success": True, **evidence})

    def rpc_ctx():
        return common.RequestContext(principal_id=actor, user_id=actor, tenant=common.TenantContext(tenant_id=tenant, project_id=project))

    def assert_noop(report: dict) -> None:
        assert report["refused"] == 0
        assert all(row["account_action"] == "unchanged" and row["steps"] == [{"action": "unchanged"}]
                   and all(k["step"]["action"] == "unchanged" for k in row["api_keys"]) for row in report["plan"])

    try:
        login = auth.Login(authn.LoginRequest(username=env["UDB_LIVE_USERNAME"], password=passwords[0], tenant_hint=tenant, project_hint=project), timeout=30)
        sensitive.extend([login.access_token, login.refresh_token])
        verified = auth.Authenticate(authn.AuthnRequest(bearer_token=login.access_token), timeout=30).principal
        actor = str(uuid.UUID(verified.subject))
        assert verified.subject == actor and verified.tenant_id == tenant and verified.project_id == project
        metadata = (("authorization", "Bearer " + login.access_token), ("x-tenant-id", tenant), ("x-udb-project-id", project))
        env.update(UDB_AUTH_TOKEN=login.access_token, UDB_TENANT_ID=tenant, UDB_PROJECT_ID=project, O4_SERVICE_PASSWORD=passwords[1])
        env.pop("UDB_USER_ID", None)
        env.pop("UDB_PRINCIPAL_ID", None)
        env.pop("UDB_SCOPES", None)
        env.pop("UDB_SERVICE_IDENTITY", None)
        service_name = prefix + "-service"
        definition = {"tenant": tenant, "service_accounts": [{
            "identity": prefix + "-identity", "project": project, "scopes": ["data:read", "data:write"],
            "provision": {"username": service_name, "email": service_name + "@example.invalid", "password_ref": {"env": "O4_SERVICE_PASSWORD"}},
            "api_keys": [{"name": "runtime", "scopes": ["data:read"], "secret_output": "runtime.key"}],
        }]}
        empty = state()
        diff = cli(definition, False)
        assert diff["plan"][0]["account_action"] == "create" and diff["plan"][0]["api_keys"][0]["step"]["action"] == "create"
        assert state() == empty, "diff changed owned business rows"
        case("diff-before-mutation", state=empty)
        bad = copy.deepcopy(definition)
        bad["service_accounts"].append(copy.deepcopy(bad["service_accounts"][0]))
        assert "duplicate" in cli(bad, True, 1)
        assert state() == empty
        case("duplicate-declaration-refused-before-mutation")
        foreign = copy.deepcopy(definition)
        foreign["tenant"] = str(uuid.uuid4())
        assert "tenant" in cli(foreign, True, 1)
        assert state() == empty
        foreign_project = copy.deepcopy(definition)
        foreign_project["service_accounts"][0]["project"] = "foreign-project"
        assert "operator project" in cli(foreign_project, True, 1)
        assert state() == empty
        case("foreign-tenant-and-project-refused-before-mutation")
        plain = copy.deepcopy(definition)
        plain["service_accounts"][0]["provision"]["password"] = passwords[1]
        assert "invalid identity declaration" in cli(plain, True, 1)
        assert state() == empty
        case("plaintext-declaration-refused-without-credential-output")
        forbidden = copy.deepcopy(definition)
        forbidden["service_accounts"][0]["scopes"] = ["udb:admin"]
        assert "scope" in cli(forbidden, True, 1)
        assert state() == empty
        case("canonical-forbidden-scope-refused-before-mutation")

        cli(definition, True)
        first = state()
        assert (first["accounts"], first["grants"], first["keys"]) == (1, 1, 1)
        account = auth.GetUser(authn.GetUserRequest(username=service_name), metadata=metadata, timeout=30).user
        assert account.account_kind == user_enum.ACCOUNT_KIND_SERVICE_ACCOUNT and account.status == user_enum.USER_STATUS_ACTIVE
        assert account.created_by == actor and account.tenant_id == tenant and account.project_id == project
        owner = str(uuid.UUID(account.user_id))
        assert owner == account.user_id and owner != actor
        grant = auth.GetServiceAccountGrant(grants.GetServiceAccountGrantRequest(tenant_id=tenant, user_id=owner), metadata=metadata, timeout=30).grant
        assert grant.user_id == owner and grant.service_identity == definition["service_accounts"][0]["identity"] and grant.status == "ACTIVE"
        secret = (private / "runtime.key").read_text()
        sensitive.append(secret)
        key_receipt = json.loads((private / "runtime.key.udb-receipt.json").read_text())
        key_id = key_receipt["key_id"]
        assert key_receipt["account"] == owner and secret.startswith(key_id + ".")
        valid = keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret, required_scope="data:read"), metadata=metadata, timeout=30)
        assert valid.valid and valid.owner_id == owner and valid.key_id == key_id
        assert not keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret, required_scope="data:write"), metadata=metadata, timeout=30).valid
        case("server-generated-account-and-scoped-key", state=first)

        assert_noop(cli(definition, False))
        assert_noop(cli(definition, True))
        assert state() == first, "second apply changed durable account/grant/key state"
        case("apply-twice-noop", state=first, grant_revision=grant.revision)

        # A malformed live document is not translated into an empty grant and
        # then overwritten. Poison only this exact owned row and restore finally.
        with connection.cursor() as cur:
            cur.execute("UPDATE udb_authn.service_account_grants SET approved_scopes_json='{}'::jsonb WHERE tenant_id=%s AND user_id=%s::uuid", (tenant, owner))
        poisoned = state()
        try:
            assert "scope" in cli(definition, True, 1)
            assert state() == poisoned
        finally:
            with connection.cursor() as cur:
                cur.execute("UPDATE udb_authn.service_account_grants SET approved_scopes_json=%s::jsonb WHERE tenant_id=%s AND user_id=%s::uuid", (grant.approved_scopes_json, tenant, owner))
        case("malformed-live-grant-refused-before-mutation")

        updated = copy.deepcopy(definition)
        updated["service_accounts"][0]["api_keys"][0]["scopes"] = ["data:read", "data:write"]
        cli(updated, True)
        reviewed = state()
        assert keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret, required_scope="data:write"), metadata=metadata, timeout=30).valid
        assert_noop(cli(updated, True))
        assert state() == reviewed
        case("scopes-reviewed-and-replay-noop")

        # Another verified operator can change this grant. The existing key then
        # fails authentication until its scopes are explicitly re-reviewed under
        # native operator authority; local receipts alone cannot restore it.
        changed = auth.ReplaceServiceAccountGrant(grants.ReplaceServiceAccountGrantRequest(
            tenant_id=tenant, user_id=owner, project_id=project,
            approved_scopes=["data:read", "data:write", "profile:read"],
            expected_revision=grant.revision, reason="owned external O4 grant change"),
            metadata=metadata, timeout=30).grant
        assert changed.revision > grant.revision
        assert not keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret), metadata=metadata, timeout=30).valid
        stale = state()
        planned_review = cli(updated, False)
        assert planned_review["plan"][0]["api_keys"][0]["step"]["action"] == "review_scopes"
        assert state() == stale
        cli(updated, True)
        assert keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret, required_scope="data:write"), metadata=metadata, timeout=30).valid
        reviewed = state()
        assert_noop(cli(updated, True))
        assert state() == reviewed
        case("stale-key-native-rereview-and-replay", state=reviewed)

        rotated = copy.deepcopy(updated)
        rotated["service_accounts"][0]["api_keys"][0].update(rotate_from=key_id, secret_output="rotated.key")
        cli(rotated, True)
        rotation = state()
        new_secret = (private / "rotated.key").read_text()
        sensitive.append(new_secret)
        new_receipt = json.loads((private / "rotated.key.udb-receipt.json").read_text())
        assert new_receipt["previous_key_id"] == key_id and new_receipt["key_id"] != key_id
        assert not keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=secret), metadata=metadata, timeout=30).valid
        assert keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=new_secret, required_scope="data:write"), metadata=metadata, timeout=30).valid
        assert_noop(cli(rotated, True))
        assert state() == rotation, "rotation declaration replay minted another key"
        case("explicit-rotation-and-replay-noop", state=rotation)

        # Wrong output binding and a pending lost-result intent each fail before
        # touching the server. No plaintext is written into those intent files.
        pending = copy.deepcopy(rotated)
        pending_key = pending["service_accounts"][0]["api_keys"][0]
        pending_key.update(secret_output="pending.key", rotate_from=new_receipt["key_id"])
        intent = private / "pending.key.udb-intent.json"
        intent.write_text(json.dumps({"schema": 1, "account": owner, "name": "runtime"}), encoding="utf-8")
        intent.chmod(0o600)
        assert "pending" in cli(pending, True, 1)
        assert state() == rotation
        wrong = copy.deepcopy(rotated)
        (private / "rotated.key.udb-receipt.json").write_text(json.dumps({**new_receipt, "account": actor}), encoding="utf-8")
        try:
            assert "another declaration or owner" in cli(wrong, True, 1)
            assert state() == rotation
        finally:
            (private / "rotated.key.udb-receipt.json").write_text(json.dumps(new_receipt), encoding="utf-8")
        case("pending-and-foreign-output-binding-refused-before-mutation")

        # An existing protected path without a receipt cannot be overwritten.
        occupied = copy.deepcopy(rotated)
        occupied["service_accounts"][0]["api_keys"] = [{"name": "occupied", "scopes": ["data:read"], "secret_output": "occupied.key"}]
        (private / "occupied.key").write_text("operator-owned", encoding="utf-8")
        (private / "occupied.key").chmod(0o600)
        assert "already exists" in cli(occupied, True, 1)
        assert (private / "occupied.key").read_text() == "operator-owned" and state() == rotation
        case("existing-output-never-overwritten")

        hijack = copy.deepcopy(definition)
        hijack["service_accounts"][0]["provision"]["username"] = prefix + "-hijacker"
        hijack["service_accounts"][0]["provision"]["email"] = prefix + "-hijacker@example.invalid"
        hijack["service_accounts"][0]["api_keys"] = []
        assert cli(hijack, False)["refused"] == 1
        assert "UDB_GRANT_OWNED_BY_OTHER" in cli(hijack, True, 1)
        assert state() == rotation, "hijack created a new account or moved the grant"
        case("bootstrap-hijack-refused-before-account-creation")

        # Explicit transfer is tested separately, with no key replay receipt
        # pretending to authorize a different account. Old secrets stop working.
        cli(hijack, True, transfer=True)
        transferred = state()
        destination = auth.GetUser(authn.GetUserRequest(username=prefix + "-hijacker"), metadata=metadata, timeout=30).user
        moved = auth.GetServiceAccountGrant(grants.GetServiceAccountGrantRequest(tenant_id=tenant, user_id=destination.user_id), metadata=metadata, timeout=30).grant
        assert moved.user_id != owner and moved.service_identity == grant.service_identity and moved.revision > grant.revision
        assert not keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=new_secret), metadata=metadata, timeout=30).valid
        assert_noop(cli(hijack, True, transfer=True))
        assert state() == transferred
        case("explicit-transfer-only-and-replay-noop", state=transferred)

        # Exercise the existing `up` bridge with two different original source
        # directories. Its temporary combined identity file must not redefine
        # either relative password input or one-time output location.
        up_root = private / "project"
        up_external = up_root / "accounts"
        up_external.mkdir(parents=True, mode=0o700)
        up_root.chmod(0o700)
        up_external.chmod(0o700)
        for directory in [up_root, up_external]:
            (directory / "password.txt").write_text(passwords[1], encoding="utf-8")
            (directory / "password.txt").chmod(0o600)
        inline_account = copy.deepcopy(definition["service_accounts"][0])
        inline_account.update(identity=prefix + "-up-inline", api_keys=[{"name": "up-inline", "scopes": ["data:read"], "secret_output": "inline.key"}])
        inline_account["provision"].update(username=prefix + "-up-inline", email=prefix + "-up-inline@example.invalid", password_ref={"file": "password.txt"})
        external_account = copy.deepcopy(inline_account)
        external_account.update(identity=prefix + "-up-external", api_keys=[{"name": "up-external", "scopes": ["data:read"], "secret_output": "external.key"}])
        external_account["provision"].update(username=prefix + "-up-external", email=prefix + "-up-external@example.invalid")
        external_file = up_external / "identities.json"
        external_file.write_text(json.dumps({"tenant": tenant, "service_accounts": [external_account]}), encoding="utf-8")
        external_file.chmod(0o600)
        up_file = up_root / "udb.json"
        up_file.write_text(json.dumps({"tenant": tenant, "project": project, "service_accounts": [inline_account], "service_accounts_file": "accounts/identities.json"}), encoding="utf-8")
        up_file.chmod(0o600)
        before_up = state()
        diff_up = invoke([str(binary), "up", "-f", str(up_file), "--dry-run"])
        assert diff_up["dry_run"] is True and state() == before_up
        applied_up = invoke([str(binary), "up", "-f", str(up_file)])
        assert applied_up["dry_run"] is False
        after_up = state()
        assert after_up["accounts"] == before_up["accounts"] + 2 and after_up["keys"] == before_up["keys"] + 2
        for directory, filename in [(up_root, "inline.key"), (up_external, "external.key")]:
            saved = (directory / filename).read_text()
            sensitive.append(saved)
            saved_receipt = json.loads((directory / (filename + ".udb-receipt.json")).read_text())
            verified_key = keys.ValidateApiKey(api.ValidateApiKeyRequest(plain_key=saved, required_scope="data:read"), metadata=metadata, timeout=30)
            assert verified_key.valid and verified_key.owner_id == saved_receipt["account"]
            assert (directory / filename).stat().st_mode & 0o777 == 0o600
        replay_up = invoke([str(binary), "up", "-f", str(up_file)])
        assert_noop(replay_up["identities"])
        assert state() == after_up
        case("up-preserves-inline-and-external-relative-references", state=after_up)
        receipt["success"] = True
    except (AssertionError, KeyError, ValueError, subprocess.TimeoutExpired, OSError, grpc.RpcError, psycopg.Error) as error:
        # Never serialize exception text: raw RPC/CLI messages may carry secrets.
        receipt["failure"] = {"type": type(error).__name__, "completed_cases": len(receipt["cases"])}
        if isinstance(error, grpc.RpcError):
            receipt["failure"]["grpc_code"] = error.code().name
    finally:
        cleanup_errors = []
        try:
            state()  # Finds partially created fixture-owned rows as well.
            for key_id in sorted(owned_keys):
                try:
                    key = keys.GetApiKey(api.GetApiKeyRequest(key_id=key_id), metadata=metadata, timeout=30).key
                    if key.status == api_enum.API_KEY_STATUS_ACTIVE:
                        keys.RevokeApiKey(api.RevokeApiKeyRequest(key_id=key_id, revoke_reason="owned O4 proof cleanup", context=rpc_ctx()), metadata=metadata, timeout=30)
                except grpc.RpcError as error:
                    if error.code() != grpc.StatusCode.NOT_FOUND:
                        cleanup_errors.append("key:" + error.code().name)
            for account_id in sorted(owned_accounts):
                try:
                    grant_row = auth.GetServiceAccountGrant(grants.GetServiceAccountGrantRequest(tenant_id=tenant, user_id=account_id), metadata=metadata, timeout=30).grant
                    if grant_row.status == "ACTIVE":
                        auth.RevokeServiceAccountGrant(grants.RevokeServiceAccountGrantRequest(tenant_id=tenant, user_id=account_id, reason="owned O4 proof cleanup"), metadata=metadata, timeout=30)
                except grpc.RpcError as error:
                    if error.code() != grpc.StatusCode.NOT_FOUND:
                        cleanup_errors.append("grant:" + error.code().name)
                try:
                    user = auth.GetUser(authn.GetUserRequest(user_id=account_id), metadata=metadata, timeout=30).user
                    if user.status != user_enum.USER_STATUS_DEACTIVATED:
                        auth.ChangeUserStatus(authn.ChangeUserStatusRequest(user_id=account_id, new_status=user_enum.USER_STATUS_DEACTIVATED, reason="owned O4 proof cleanup", context=rpc_ctx()), metadata=metadata, timeout=30)
                except grpc.RpcError as error:
                    cleanup_errors.append("account:" + error.code().name)
            with connection.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM udb_authn.users WHERE username LIKE %s AND status <> 'DEACTIVATED'", (prefix + "%",))
                assert cur.fetchone()[0] == 0
                cur.execute("SELECT COUNT(*) FROM udb_authn.service_account_grants WHERE user_id::text=ANY(%s) AND status='ACTIVE'", (sorted(owned_accounts),))
                assert cur.fetchone()[0] == 0
                cur.execute("SELECT COUNT(*) FROM udb_authn.api_keys WHERE owner_id=ANY(%s) AND status='ACTIVE' AND deleted_at IS NULL", (sorted(owned_accounts),))
                assert cur.fetchone()[0] == 0
            receipt["cleanup_success"] = not cleanup_errors
        except (AssertionError, OSError, psycopg.Error):
            cleanup_errors.append("cleanup-verification")
        receipt["cleanup_errors"] = cleanup_errors
        receipt["elapsed_ms"] = round((time.monotonic() - start) * 1000, 3)
        channel.close()
        connection.close()
        # Remove only this mkdtemp-owned directory, including all one-time
        # secrets. It is never in the upload glob or rendered to stdout.
        shutil.rmtree(private)
        output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"proof": receipt["proof"], "success": receipt["success"], "cleanup_success": receipt["cleanup_success"], "cases": len(receipt["cases"])}))
    return 0 if receipt["success"] and receipt["cleanup_success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
