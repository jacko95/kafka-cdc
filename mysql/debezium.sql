GRANT SELECT, RELOAD, FLUSH_TABLES, SHOW DATABASES,
          REPLICATION SLAVE, REPLICATION CLIENT
      ON *.* TO 'cdc_user'@'%';

FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS db_cdc.utenti (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255),
    PRIMARY KEY (id)
);

-- Inserisci dati di test
INSERT INTO db_cdc.utenti (name) VALUES ('ciccio');