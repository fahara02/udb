-- UDB:migration_kind=bootstrap
-- UDB:schema=billing
-- UDB:table=products
-- UDB:proto_manifest_checksum=sha256:c404ea5faae33c700b4d7da77831ce24fabbb6203e972e6e73a99bdd1cbe2b06
-- UDB:source_proto=proto/acme/billing/v1/acme_billing_v1.proto
-- UDB:generator=udb

SET lock_timeout = '5s';
SET statement_timeout = '120s';

CREATE SCHEMA IF NOT EXISTS "billing";

CREATE TABLE IF NOT EXISTS "billing"."products" (
    "product_id" TEXT,
    "name" TEXT,
    "description" TEXT,
    "price_cents" BIGINT,
    "sku" TEXT,
    CONSTRAINT "pk_products" PRIMARY KEY ("product_id")
);

ALTER TABLE "billing"."products" ADD COLUMN IF NOT EXISTS "product_id" TEXT;
ALTER TABLE "billing"."products" ADD COLUMN IF NOT EXISTS "name" TEXT;
ALTER TABLE "billing"."products" ADD COLUMN IF NOT EXISTS "description" TEXT;
ALTER TABLE "billing"."products" ADD COLUMN IF NOT EXISTS "price_cents" BIGINT;
ALTER TABLE "billing"."products" ADD COLUMN IF NOT EXISTS "sku" TEXT;

