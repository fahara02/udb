-- UDB:migration_kind=bootstrap
-- UDB:schema=billing
-- UDB:table=invoices
-- UDB:proto_manifest_checksum=sha256:c404ea5faae33c700b4d7da77831ce24fabbb6203e972e6e73a99bdd1cbe2b06
-- UDB:source_proto=proto/acme/billing/v1/acme_billing_v1.proto
-- UDB:generator=udb

SET lock_timeout = '5s';
SET statement_timeout = '120s';

CREATE SCHEMA IF NOT EXISTS "billing";

CREATE TABLE IF NOT EXISTS "billing"."invoices" (
    "invoice_id" TEXT,
    "org_id" TEXT,
    "customer_name" TEXT,
    "customer_email" TEXT,
    "amount_cents" BIGINT,
    "currency" TEXT,
    "status" TEXT,
    "created_at" TEXT,
    "updated_at" TEXT,
    CONSTRAINT "pk_invoices" PRIMARY KEY ("invoice_id")
);

ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "invoice_id" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "org_id" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "customer_name" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "customer_email" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "amount_cents" BIGINT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "currency" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "status" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "created_at" TEXT;
ALTER TABLE "billing"."invoices" ADD COLUMN IF NOT EXISTS "updated_at" TEXT;

ALTER TABLE "billing"."invoices" ENABLE ROW LEVEL SECURITY;

COMMENT ON TABLE "billing"."invoices" IS 'Customer invoices managed by ACME Billing';
