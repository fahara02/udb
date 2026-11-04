// One connected client per tenant, for a service that serves several tenants,
// each with its own service-account key. Clients are created on first use, shared
// afterwards, and replaced when their credential stops refreshing.

import { UdbProject, UdbProjectConfig } from "./project";

/** What the pool needs from a client. `UdbProject` satisfies it. */
export interface TenantPoolClient {
  credentialError(): Error | null;
  close(): void | Promise<void>;
}

/** Builds a tenant's client. `signal` aborts when the first caller gives up, the
 *  tenant is evicted, or the pool closes. */
export type TenantPoolConnect<C> = (tenantId: string, signal: AbortSignal) => Promise<C>;

interface PoolDial<C> {
  done: Promise<void>;
  finish: () => void;
  controller: AbortController;
  unlink: () => void;
  completed: boolean;
  client?: C;
  error?: unknown;
}

function aborted(signal: AbortSignal): unknown {
  return signal.reason ?? new Error("udb: the operation was aborted");
}

function evicted(tenantId: string): Error {
  return new Error(`udb: tenant pool session for tenant "${tenantId}" was evicted`);
}

function poolClosed(): Error {
  return new Error("udb: tenant pool is closed");
}

// A failed client's cleanup must not hold up its replacement, and a failed close
// has nobody to report to.
function closeDetached(client: TenantPoolClient): void {
  void Promise.resolve()
    .then(() => client.close())
    .catch(() => undefined);
}

export class TenantSessionPool<C extends TenantPoolClient = UdbProject> {
  private readonly sessions = new Map<string, C>();
  private readonly pending = new Map<string, PoolDial<C>>();
  private closed = false;

  constructor(private readonly connect: TenantPoolConnect<C>) {}

  /** The tenant's client, connecting it on first use. Concurrent first calls share
   *  one connect. A client whose credential refresh is failing is closed and
   *  replaced. The first caller's signal owns the connect; a later caller's signal
   *  only abandons its own wait. */
  async get(tenantId: string, options: { signal?: AbortSignal } = {}): Promise<C> {
    const { signal } = options;
    if (signal?.aborted) throw aborted(signal);
    if (this.closed) throw poolClosed();
    const live = this.sessions.get(tenantId);
    if (live) {
      if (live.credentialError() === null) return live;
      this.sessions.delete(tenantId);
      closeDetached(live);
    }
    const shared = this.pending.get(tenantId);
    if (shared) return this.waitDial(tenantId, shared, signal);
    if (!this.connect) throw new Error("udb: tenant pool connect is required");
    return this.waitDial(tenantId, this.startDial(tenantId, signal), signal);
  }

  /** The tenants with a live client, sorted. */
  tenants(): string[] {
    return [...this.sessions.keys()].sort();
  }

  /** Close and forget one tenant's client; the next `get` reconnects. */
  async evict(tenantId: string): Promise<void> {
    const client = this.sessions.get(tenantId);
    this.sessions.delete(tenantId);
    const dial = this.pending.get(tenantId);
    if (dial) this.retireDial(tenantId, dial, evicted(tenantId));
    if (client) await client.close();
  }

  /** Close every client. Later `get`s fail. Throws the first close failure, after
   *  every client has had its turn. */
  async close(): Promise<void> {
    this.closed = true;
    for (const [tenant, dial] of [...this.pending]) this.retireDial(tenant, dial, poolClosed());
    const clients = [...this.sessions.values()];
    this.sessions.clear();
    let first: unknown;
    let failed = false;
    for (const client of clients) {
      try {
        await client.close();
      } catch (err) {
        if (!failed) {
          failed = true;
          first = err;
        }
      }
    }
    if (failed) throw first;
  }

  private startDial(tenantId: string, signal?: AbortSignal): PoolDial<C> {
    let finish = () => {};
    const done = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const controller = new AbortController();
    const dial: PoolDial<C> = { done, finish, controller, unlink: () => {}, completed: false };
    if (signal) {
      // The first caller owns the connect: giving up cancels it and frees the slot
      // for a new attempt, whatever the connector does with its own signal.
      const onAbort = () => this.retireDial(tenantId, dial, aborted(signal));
      signal.addEventListener("abort", onAbort, { once: true });
      dial.unlink = () => signal.removeEventListener("abort", onAbort);
    }
    this.pending.set(tenantId, dial);
    void this.runDial(tenantId, dial);
    return dial;
  }

  private async runDial(tenantId: string, dial: PoolDial<C>): Promise<void> {
    let client: C | undefined;
    let error: unknown;
    let failed = false;
    try {
      client = await this.connect(tenantId, dial.controller.signal);
      if (client === undefined || client === null) {
        failed = true;
        error = new Error("udb: tenant pool connect returned a nil client");
      }
    } catch (err) {
      failed = true;
      error = err;
    }
    // A late result of a retired attempt belongs to nobody: it must not install
    // into a newer generation, and its client must not leak.
    if (dial.completed) {
      if (client) closeDetached(client);
      return;
    }
    if (failed) {
      this.completeDial(tenantId, dial, undefined, error);
      return;
    }
    this.sessions.set(tenantId, client as C);
    this.completeDial(tenantId, dial, client);
  }

  private completeDial(tenantId: string, dial: PoolDial<C>, client: C | undefined, error?: unknown): void {
    if (dial.completed) return;
    dial.completed = true;
    if (this.pending.get(tenantId) === dial) this.pending.delete(tenantId);
    dial.client = client;
    dial.error = error;
    dial.unlink();
    dial.finish();
  }

  private retireDial(tenantId: string, dial: PoolDial<C>, error: unknown): void {
    this.completeDial(tenantId, dial, undefined, error);
    dial.controller.abort(error);
  }

  private async waitDial(tenantId: string, dial: PoolDial<C>, signal?: AbortSignal): Promise<C> {
    if (signal) {
      let onAbort = () => {};
      const abandoned = new Promise<never>((_, reject) => {
        onAbort = () => reject(aborted(signal));
        signal.addEventListener("abort", onAbort, { once: true });
      });
      try {
        await Promise.race([dial.done, abandoned]);
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    } else {
      await dial.done;
    }
    if (dial.client === undefined) throw dial.error;
    if (this.closed) throw poolClosed();
    if (this.sessions.get(tenantId) !== dial.client) throw evicted(tenantId);
    return dial.client;
  }
}

/** A pool where every tenant connects with `base` and its own API key from `keys`
 *  (tenant UUID to key). A tenant without a key is refused, and a key that verifies
 *  as a different tenant is refused and its connection closed. */
export function newApiKeyTenantPool(
  base: Omit<UdbProjectConfig, "tenantId" | "credentials">,
  keys: Record<string, string>,
): TenantSessionPool<UdbProject> {
  // The pool owns a snapshot: changing the caller's map later changes nothing.
  const owned = new Map(Object.entries(keys));
  return new TenantSessionPool<UdbProject>(async (tenantId) => {
    const key = owned.get(tenantId);
    if (!key) throw new Error(`udb: tenant pool has no API key for tenant "${tenantId}"`);
    const udb = await UdbProject.connect({ ...base, tenantId, credentials: { apiKey: key } });
    try {
      udb.verify({ tenantId });
    } catch (err) {
      udb.close();
      throw err;
    }
    return udb;
  });
}
