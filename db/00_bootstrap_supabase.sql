-- 00_bootstrap_supabase.sql
-- Cloud counterpart to 00_bootstrap.sql. Run ONCE against a fresh Supabase
-- project, connected as the `postgres` role, BEFORE running any migrations.
--
--   psql "$SUPABASE_ADMIN_URL" -f db/00_bootstrap_supabase.sql
--
-- WHAT'S DIFFERENT FROM THE LOCAL BOOTSTRAP, AND WHY
--
--  1. No CREATE DATABASE / \connect. Supabase hands you a ready database named
--     `postgres`. We create our roles and objects inside it.
--
--  2. No blanket `REVOKE ALL ON SCHEMA public FROM PUBLIC`. On a self-hosted box
--     that's good hygiene; on Supabase it can break the managed services
--     (PostgREST, Auth, Realtime, Storage) that share this database. We grant
--     what our roles need and leave Supabase's own grants alone.
--
--  3. Passwords are PROMPTED, not hardcoded. The local bootstrap shipped
--     placeholder literals ('admin'/'app'/'ro'), which is exactly how a .env
--     ends up out of sync with the actual role password. Here psql asks you for
--     each one and the ALTER ROLE below is idempotent, so re-running this file
--     RESETS the passwords to whatever you type — making "auth failed" trivially
--     recoverable: just run it again.
--
--     Note: the prompts are interpolated via \gexec + format(%L) rather than
--     inside a DO $$ ... $$ block, because psql does NOT substitute :variables
--     inside dollar-quoted strings.
--
--  4. Supabase's `postgres` role is privileged but NOT a superuser. Everything
--     below stays inside what it's actually allowed to do.

\set ON_ERROR_STOP on

\prompt 'Password for senseable_owner (migrations/DDL): ' owner_pw
\prompt 'Password for senseable_app   (backend runtime): ' app_pw
\prompt 'Password for senseable_ro    (read-only/BI)   : ' ro_pw

-- ── Extensions ───────────────────────────────────────────────────────────────
-- gen_random_uuid() is core in PG13+, so pgcrypto is only needed for
-- crypt()/gen_salt(). citext backs the case-insensitive email column.
-- Supabase keeps extensions in the `extensions` schema; we add it to the role
-- search_path further down so unqualified `citext` resolves in migrations.
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;
CREATE EXTENSION IF NOT EXISTS citext   WITH SCHEMA extensions;

-- ── Roles (same three-tier least-privilege model as local) ───────────────────
-- senseable_owner : owns schema objects, runs migrations (DDL). Not the runtime.
-- senseable_app   : backend runtime login. DML only, constrained by RLS.
-- senseable_ro    : read-only login for reporting / thesis data pulls.
SELECT format('CREATE ROLE senseable_owner LOGIN PASSWORD %L', :'owner_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_owner')\gexec

SELECT format('CREATE ROLE senseable_app LOGIN PASSWORD %L', :'app_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_app')\gexec

SELECT format('CREATE ROLE senseable_ro LOGIN PASSWORD %L', :'ro_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'senseable_ro')\gexec

-- Idempotent password reset: re-running this file re-syncs the roles to the
-- passwords you just typed. Put these SAME values in .env.cloud.
ALTER ROLE senseable_owner WITH LOGIN PASSWORD :'owner_pw';
ALTER ROLE senseable_app   WITH LOGIN PASSWORD :'app_pw';
ALTER ROLE senseable_ro    WITH LOGIN PASSWORD :'ro_pw';

-- Let `postgres` administer objects owned by senseable_owner (drop/inspect via
-- the dashboard). Without this, Studio can't manage our tables.
GRANT senseable_owner TO postgres;

-- Resolve unqualified extension types (citext) during migrations.
ALTER ROLE senseable_owner SET search_path = public, extensions;
ALTER ROLE senseable_app   SET search_path = public, extensions;
ALTER ROLE senseable_ro    SET search_path = public, extensions;

-- ── Schema privileges ────────────────────────────────────────────────────────
GRANT USAGE  ON SCHEMA public     TO senseable_app, senseable_ro;
GRANT ALL    ON SCHEMA public     TO senseable_owner;
GRANT USAGE  ON SCHEMA extensions TO senseable_owner, senseable_app, senseable_ro;

-- Default privileges: anything the OWNER creates later (i.e. every migration)
-- is automatically granted to the app + read-only roles, so you never re-run
-- GRANTs after a migration. This is what makes the `commands` table from
-- migration 003 usable by senseable_app with no extra step.
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO senseable_app;
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT USAGE, SELECT                  ON SEQUENCES TO senseable_app;
ALTER DEFAULT PRIVILEGES FOR ROLE senseable_owner IN SCHEMA public
  GRANT SELECT                         ON TABLES    TO senseable_ro;

-- ── Report ───────────────────────────────────────────────────────────────────
SELECT rolname, rolcanlogin, rolsuper
FROM pg_roles
WHERE rolname IN ('postgres','senseable_owner','senseable_app','senseable_ro')
ORDER BY rolname;

\echo ''
\echo '✓ Supabase bootstrap complete.'
\echo '  Next: run migrations as senseable_owner, then verify RLS is fail-closed.'
\echo '  Remember the pooler username format is  role.PROJECT_REF'
\echo '  e.g.  senseable_app.abcdefghijklmnop'
