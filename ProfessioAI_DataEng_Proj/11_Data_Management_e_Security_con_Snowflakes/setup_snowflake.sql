-- ###########################################
-- 1. INTEGRAZIONE CON GCS (Google Cloud Storage)
-- ###########################################

-- Crea l'integrazione di storage con GCS
CREATE OR REPLACE STORAGE INTEGRATION gcs_integrazione
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://health-data-bucket-sg');

-- Visualizza configurazione dell’integrazione
DESC INTEGRATION gcs_integrazione;


-- ###########################################
-- 2. CREAZIONE DATABASE E SCHEMA
-- ###########################################

CREATE OR REPLACE DATABASE health_data;
USE DATABASE health_data;

CREATE OR REPLACE SCHEMA raw_data;
USE SCHEMA raw_data;


-- ###########################################
-- 3. STAGE PER IMPORTAZIONE FILE CSV
-- ###########################################

CREATE OR REPLACE STAGE health_data_stage
  URL = 'gcs://health-data-bucket-sg'
  STORAGE_INTEGRATION = gcs_integrazione
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Elenca i file presenti nello stage
LIST @health_data_stage;


-- ###########################################
-- 4. MODELLAZIONE TABELLE CLINICHE
-- ###########################################

-- Pazienti
CREATE OR REPLACE TABLE pazienti (
  ID_Paziente INTEGER,
  Nome STRING,
  Cognome STRING,
  Data_Nascita DATE,
  Sesso STRING,
  Codice_Fiscale STRING,
  Indirizzo STRING
);

-- Ricoveri
CREATE OR REPLACE TABLE ricoveri (
  ID_Ricovero INTEGER,
  ID_Paziente INTEGER,
  Data_Inizio DATE,
  Data_Fine DATE,
  Reparto STRING,
  Motivo_Ricovero STRING
);

-- Visite
CREATE OR REPLACE TABLE visite (
  ID_Visita INTEGER,
  ID_Paziente INTEGER,
  Data_Visita DATE,
  Tipo_Visita STRING,
  Medico STRING,
  Note STRING
);

-- Terapie
CREATE OR REPLACE TABLE terapie (
  ID_Terapia INTEGER,
  ID_Paziente INTEGER,
  Data_Inizio DATE,
  Data_Fine DATE,
  Farmaco STRING,
  Dosaggio STRING,
  Medico_Prescrittore STRING
);

-- Sensori IoT
CREATE OR REPLACE TABLE sensori_iot (
  ID_Rilevazione INTEGER,
  ID_Paziente INTEGER,
  Timestamp TIMESTAMP,
  Tipo_Sensore STRING,
  Valore STRING,
  Unita_Misura STRING
);

-- Utenti
CREATE OR REPLACE TABLE utenti (
  ID_Utente INTEGER,
  Nome STRING,
  Cognome STRING,
  Email STRING,
  Ruolo STRING,
  Reparto STRING
);

-- Log Accessi
CREATE OR REPLACE TABLE accessi_log (
  ID_Accesso INTEGER,
  ID_Utente INTEGER,
  ID_Paziente INTEGER,
  Timestamp TIMESTAMP,
  Azione STRING
);


-- ###########################################
-- 5. IMPORTAZIONE DATI DA GCS
-- ###########################################

COPY INTO pazienti FROM @health_data_stage/pazienti.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO ricoveri FROM @health_data_stage/ricoveri.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO visite FROM @health_data_stage/visite.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO terapie FROM @health_data_stage/terapie.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO sensori_iot FROM @health_data_stage/sensori_iot.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO utenti FROM @health_data_stage/utenti.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
COPY INTO accessi_log FROM @health_data_stage/accessi_log.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


-- ###########################################
-- 6. GESTIONE RUOLI E ACCESSI (RBAC)
-- ###########################################

-- Ruoli
CREATE ROLE medico;
CREATE ROLE infermiera;
CREATE ROLE amministrativo;

-- Permessi per ruolo "medico"
GRANT SELECT ON TABLE pazienti TO ROLE medico;
GRANT SELECT ON TABLE ricoveri TO ROLE medico;
GRANT SELECT ON TABLE visite TO ROLE medico;
GRANT SELECT ON TABLE terapie TO ROLE medico;
GRANT SELECT ON TABLE sensori_iot TO ROLE medico;

-- Permessi per ruolo "infermiera"
GRANT SELECT ON TABLE pazienti TO ROLE infermiera;
GRANT SELECT ON TABLE ricoveri TO ROLE infermiera;

-- Permessi per ruolo "amministrativo"
GRANT SELECT ON TABLE accessi_log TO ROLE amministrativo;
GRANT SELECT ON TABLE utenti TO ROLE amministrativo;


-- ###########################################
-- 7. CREAZIONE UTENTI E ASSEGNAZIONE RUOLI
-- ###########################################

-- Medico
CREATE USER luca_verdi PASSWORD = 'PasswordSicura123!' LOGIN_NAME = 'luca_verdi'
EMAIL = 'l.verdi@ospedale.it' DEFAULT_ROLE = medico MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE medico TO USER luca_verdi;

-- Infermiera
CREATE USER anna_russo PASSWORD = 'PasswordSicura123!' LOGIN_NAME = 'anna_russo'
EMAIL = 'a.russo@ospedale.it' DEFAULT_ROLE = infermiera MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE infermiera TO USER anna_russo;

-- Amministrativo
CREATE USER marco_conti PASSWORD = 'PasswordSicura123!' LOGIN_NAME = 'marco_conti'
EMAIL = 'm.conti@ospedale.it' DEFAULT_ROLE = amministrativo MUST_CHANGE_PASSWORD = TRUE;
GRANT ROLE amministrativo TO USER marco_conti;


-- ###########################################
-- 8. VISTE E MASCHERAMENTO (GDPR COMPLIANCE)
-- ###########################################

-- View anonimizzata per ruoli non clinici
CREATE OR REPLACE VIEW vista_anonimizzata_pazienti AS
SELECT ID_Paziente, Sesso, Data_Nascita FROM pazienti;

-- View con JOIN per report clinici
CREATE OR REPLACE VIEW vista_visite_completa AS
SELECT v.ID_Visita, p.Nome, p.Cognome, v.Data_Visita, v.Tipo_Visita, v.Medico
FROM visite v JOIN pazienti p ON v.ID_Paziente = p.ID_Paziente;

-- Policy per mascherare Codice Fiscale
CREATE OR REPLACE MASKING POLICY mask_codice_fiscale AS (val STRING) 
RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('medico', 'infermiera') THEN val
    ELSE '***********'
  END;

-- Applica la policy alla tabella pazienti
ALTER TABLE pazienti MODIFY COLUMN Codice_Fiscale SET MASKING POLICY mask_codice_fiscale;


-- ###########################################
-- 9. TASK AUTOMATICI: AGGIORNAMENTO DATI
-- ###########################################

-- Task per aggiornare le tabelle ogni ora
CREATE OR REPLACE TASK task_aggiorna_dati_gcs
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 HOUR'
AS
BEGIN
  COPY INTO pazienti FROM @health_data_stage/pazienti.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO ricoveri FROM @health_data_stage/ricoveri.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO visite FROM @health_data_stage/visite.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO terapie FROM @health_data_stage/terapie.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO sensori_iot FROM @health_data_stage/sensori_iot.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO utenti FROM @health_data_stage/utenti.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
  COPY INTO accessi_log FROM @health_data_stage/accessi_log.csv FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
END;

-- Avvia il task
ALTER TASK task_aggiorna_dati_gcs RESUME;


-- ###########################################
-- 10. TASK ANALITICO: REPORT AUTOMATICO
-- ###########################################

-- Tabella per raccogliere il numero di ricoveri
CREATE OR REPLACE TABLE report_ricoveri (
  Timestamp_Rilevazione TIMESTAMP,
  Totale_Ricoveri INTEGER
);

-- Task per aggiornare report ogni ora
CREATE OR REPLACE TASK aggiorna_report_ricoveri
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
INSERT INTO report_ricoveri
SELECT CURRENT_TIMESTAMP, COUNT(*) FROM ricoveri;

-- Avvia il task
ALTER TASK aggiorna_report_ricoveri RESUME;