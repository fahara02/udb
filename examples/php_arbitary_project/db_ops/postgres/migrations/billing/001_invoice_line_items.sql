-- UDB:migration_kind=bootstrap
-- UDB:schema=billing
-- UDB:table=invoice_line_items
-- UDB:proto_manifest_checksum=sha256:c404ea5faae33c700b4d7da77831ce24fabbb6203e972e6e73a99bdd1cbe2b06
-- UDB:source_proto=proto/acme/billing/v1/acme_billing_v1.proto
-- UDB:generator=udb

SET lock_timeout = '5s';
SET statement_timeout = '120s';

CREATE SCHEMA IF NOT EXISTS "billing";

CREATE TABLE IF NOT EXISTS "billing"."invoice_line_items" (
    "line_item_id" TEXT,
    "org_id" TEXT,
    "invoice_id" TEXT,
    "description" TEXT,
    "unit_price" BIGINT,
    "quantity" INTEGER,
    CONSTRAINT "pk_invoice_line_items" PRIMARY KEY ("line_item_id")
);

ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "line_item_id" TEXT;
ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "org_id" TEXT;
ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "invoice_id" TEXT;
ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "description" TEXT;
ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "unit_price" BIGINT;
ALTER TABLE "billing"."invoice_line_items" ADD COLUMN IF NOT EXISTS "quantity" INTEGER;

