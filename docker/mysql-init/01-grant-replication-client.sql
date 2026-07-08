-- A canonical MySQL store reads the binary-log position for its durability
-- token (`SHOW BINARY LOG STATUS` on 8.4+, `SHOW MASTER STATUS` on < 8.4),
-- which requires the REPLICATION CLIENT privilege. The MYSQL_USER created by
-- the image only gets privileges on its own database, so grant it here. This
-- runs once on a fresh data directory (docker-entrypoint-initdb.d).
GRANT REPLICATION CLIENT ON *.* TO 'udb'@'%';
-- Live conformance (B.11) provisions throwaway `udb_conf_<uuid>` databases for
-- per-run isolation; allow the udb user to create/drop and fully use them.
GRANT CREATE, DROP ON *.* TO 'udb'@'%';
GRANT ALL PRIVILEGES ON `udb\_conf\_%`.* TO 'udb'@'%';
-- XA live tests/smokes provision throwaway `udb_xa_<uuid>` databases and the
-- recovery worker calls XA RECOVER through the same udb account.
GRANT ALL PRIVILEGES ON `udb\_xa\_%`.* TO 'udb'@'%';
GRANT XA_RECOVER_ADMIN ON *.* TO 'udb'@'%';
FLUSH PRIVILEGES;
