-- =============================================================================
-- init_db.sql  —  Credit Fraud DWH initialisation
-- Runs once when the postgres-dwh container starts for the first time.
-- =============================================================================

-- Medallion schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- RBAC roles (mirrors Snowflake role pattern for portfolio parity)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'raw_loader') THEN
        CREATE ROLE raw_loader;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'transformer') THEN
        CREATE ROLE transformer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reporter') THEN
        CREATE ROLE reporter;
    END IF;
END
$$;

-- Schema-level privileges
GRANT USAGE ON SCHEMA raw    TO raw_loader, transformer;
GRANT USAGE ON SCHEMA silver TO transformer, reporter;
GRANT USAGE ON SCHEMA gold   TO reporter;

GRANT ALL PRIVILEGES ON SCHEMA raw    TO raw_loader;
GRANT ALL PRIVILEGES ON SCHEMA silver TO transformer;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO reporter;

-- Give the application user all three roles
GRANT raw_loader, transformer, reporter TO fraud_user;

-- Default privileges so future tables inherit role access
ALTER DEFAULT PRIVILEGES IN SCHEMA raw    GRANT ALL ON TABLES TO raw_loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO transformer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO reporter;
