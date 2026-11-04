// The tenant code to canonical UUID transition.
//
// A human tenant code ("acme") is only a pre-login hint. After login the broker
// resolves and verifies the canonical tenant UUID, and that value is what every
// tenant-scoped record and filter must carry. Row-level security compares it to
// the tenant column, so a code or an upper-case form misses rows or stamps an
// unusable value. This type keeps the two apart and fails fast on a mismatch.

const UUID_SHAPE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

export class TenantState {
  /** The human code supplied before login; never written into tenant-scoped data. */
  tenantCode: string;
  /** The verified canonical tenant UUID; empty until `adopt` succeeds. */
  canonicalId = "";
  /** Whether a canonical id has been adopted after login. */
  isVerified = false;

  constructor(code = "") {
    this.tenantCode = code;
  }

  /** Record the canonical tenant UUID from the verified principal. A value that
   *  is not a lowercase hyphenated UUID is refused and changes nothing. */
  adopt(canonicalId: string): void {
    const value = (canonicalId ?? "").trim();
    if (!value) throw new Error("udb: cannot adopt an empty canonical tenant_id");
    if (!UUID_SHAPE.test(value) || value !== value.toLowerCase()) {
      throw new Error(`udb: canonical tenant_id "${canonicalId}" is not a canonical (lowercase hyphenated) UUID`);
    }
    this.canonicalId = value;
    this.isVerified = true;
  }

  /** The canonical tenant UUID. Throws before login so a tenant-scoped request
   *  cannot silently fall back to the human code. */
  getVerified(): string {
    if (!this.isVerified || !this.canonicalId) {
      throw new Error(
        "udb: tenant not verified; call loginAndAdoptTenant or connectEnterprise before tenant-scoped operations",
      );
    }
    return this.canonicalId;
  }

  /** Refuse a record or filter whose tenant differs from the verified tenant,
   *  naming both. An empty id passes: the caller fills it from `getVerified()`. */
  validateTenantId(recordTenantId: string): void {
    const canonical = this.getVerified();
    if (recordTenantId && recordTenantId !== canonical) {
      throw new Error(
        `udb: tenant_id mismatch: record/filter has "${recordTenantId}" but the authenticated tenant is "${canonical}"`,
      );
    }
  }
}
