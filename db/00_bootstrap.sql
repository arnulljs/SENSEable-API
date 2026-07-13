-- 00_bootstrap.sql
-- Run ONCE, as the postgres superuser, to create the database and the
-- least-privilege role set. This is the only file you run as a superuser;
-- everything after this runs as senseable_owner (migrations/seed) or
-- senseable_app (the running backend).
--
--   psql -U postgres -f db/00_bootstrap.sql
--
-- Change the three passwords below before running in anything but local dev.

-- ── Roles ────────────────────────────────────────────────────────────────────
-- senseable_owner : owns all schema objects, runs migrations (DDL). NOT used
--                   by the running app.
-- senseable_app   : the backend's runtime login. DML only, and constrained by
--                   Row-Level Security (added in the RLS migration).
-- senseable_ro    : read-only login for reporting / thesis data pulls / BI.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_owner') THEN
    CREATE ROLE senseable_owner LOGIN PASSWORD 'admin';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_app') THEN
    CREATE ROLE senseable_app LOGIN PASSWORD 'app';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_ro') THEN
    CREATE ROLE senseable_ro LOGIN PASSWORD 'ro';
  END IF;
END $$;

-- ── Database ─────────────────────────────────────────────────────────────────
-- Created idempotently: CREATE DATABASE can't run inside a DO block, so we
-- guard it with a \gexec trick — this line is a no-op if the db already exists.
SELECT 'CREATE DATABASE senseable OWNER senseable_owner'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'senseable')\gexec

-- Everything below runs INSIDE the senseable database.
\connect senseable

-- Trusted extensions — creatable by the owner, but we do them here as
-- superuser so there's zero privilege ambiguity later.
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid(), crypt()/gen_salt()
CREATE EXTENSION IF NOT EXISTS citext;     -- case-insensitive email column

-- ── Schema privilege model ───────────────────────────────────────────────────
-- Postgres 15+ already revokes CREATE on public from PUBLIC, but we make the
-- whole model explicit so it's identical on 14/15/16/17.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT  USAGE  ON SCHEMA public TO senseable_app, senseable_ro;
GRANT  ALL    ON SCHEMA public TO senseable_owner;

-- Default privileges: any table/sequence the OWNER creates later (i.e. via
-- migrations) is automatically granted to the app + read-only roles. This is
-- what stops you from having to re-run GRANTs after every migration.
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO senseable_app;
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT USAGE, SELECT                  ON SEQUENCES TO senseable_app;
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT SELECT                         ON TABLES    TO senseable_ro;
