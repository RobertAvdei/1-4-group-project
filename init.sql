DROP TABLE IF EXISTS patients,encounters,procedure_types, observations, observation_types ,diagnoses ,diagnose_type, medications, drugs;

CREATE TABLE patients (address varchar(255),
                       city varchar(255),
                       date_of_birth date,
                       first_name varchar(255),
                       gender varchar(255),
                       last_name varchar(255),
                       patient_id uuid NOT NULL PRIMARY KEY,
                       phone_number varchar(255),
                       state varchar(255),
                       zip_code varchar(255));

CREATE TABLE encounters (admission_date date,
                        attending_physician_id varchar(255),
                        discharge_date date,
                        encounter_id uuid NOT NULL PRIMARY KEY,
                        patient_id uuid);

CREATE TABLE procedures (date_performed date,
                         encounter_id uuid,
                         procedure_code int,
                         procedure_id uuid NOT NULL PRIMARY KEY);

CREATE TABLE procedure_types (procedures_code int NOT NULL PRIMARY KEY,
                              procedure_description varchar(255));


CREATE TABLE observations (encounter_id uuid,
                           observation_code varchar(255),
                           observation_datetime date,
                           observation_id uuid NOT NULL PRIMARY KEY,
                           units varchar(255),
                           value varchar(255));

CREATE TABLE observation_types (observation_code varchar(255) NOT NULL PRIMARY KEY,
                                 observation_description varchar(255));

CREATE TABLE diagnoses (diagnosis_id uuid NOT NULL PRIMARY KEY,
                        encounter_id uuid,
                        date_recorded date);

CREATE TABLE diagnose_types (diagnosis_code varchar(255) NOT NULL PRIMARY KEY,
                            diagnosis_description varchar(255));

CREATE TABLE medications (medication_order_id uuid NOT NULL PRIMARY KEY,
                          encounter_id uuid,
                          dosage varchar(255),
                          frequency varchar(255),
                          start_date date,
                          end_date date);

CREATE TABLE drugs (drug_code int NOT NULL PRIMARY KEY,
                    drug_name varchar(255));
