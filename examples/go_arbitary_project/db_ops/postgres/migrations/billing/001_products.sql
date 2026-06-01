-- UDB:migration_kind=bootstrap
-- UDB:schema=billing
-- UDB:table=products
-- UDB:proto_manifest_checksum=sha256:d260e5340ef90c3fefeae64d58c1226cef2b53c696b032950b8d2310ffd7e37d
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

