import json
import re
import numbers
from db_connection import run_single_query

def handle_message(msg):
    topic = msg.topic
    raw_values = msg.value.decode('utf-8')
    match topic:
        case 'patients':
            handle_patients(raw_values, topic)
        case 'encounters':
            handle_encounters(raw_values, topic)
        case 'diagnoses':
            handle_diagnoses(raw_values, topic)
        case 'observations':
            handle_observations(raw_values, topic)
        case 'medications':
            handle_medications(raw_values, topic)
        case 'procedures':
            handle_procedures(raw_values, topic)
        case _:
            print('Wrong Topic')

def split_by_comma(string):
    return re.split(',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)',string)

def format_str(string):
    return f'{string.replace("\"", "\'")}'

def is_number(string):
    return string.replace('.','',1).isdigit()

# Insert only unique rows based on primary key clause
def insert(rows,table,columns,casted_columns,p_key_clause):
    query = (f"INSERT INTO {table} {columns} "
        f"SELECT DISTINCT {casted_columns} "
        f"FROM (VALUES {','.join(rows)}) "
        f"AS NewValues {columns} "
        f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {table}.{p_key_clause})")
    print("Table: ",table)
    run_single_query(query)


def handle_csv(raw_values, table, columns, casted_columns, p_key_clause, indexes=None, merge=None):
    if merge is None:
        merge = []
        
    rows = []
    raw_rows: list[str] = raw_values.splitlines()
    del raw_rows[0] # remove columns

    if indexes is None:
        indexes = list(range(len(split_by_comma(raw_rows[0]))))
        
    for raw_row in raw_rows:
        values_list = split_by_comma(raw_row)
        row = []
        for index in range(len(values_list)):
            if index in indexes :
                value = values_list[index]
                row.append(format_str(value))
            elif (index in merge and values_list[index] != "\"\""):
                value = values_list[index]
                if is_number(value):
                    value = f"'{value}'"
                row.append(format_str(value))
        row =f"({','.join(row)})"
        
        rows.append(row)
        
    insert(rows,table,columns,casted_columns,p_key_clause)

def format_record(value):
    return "NULL" if value is None else f"'{value}'"

def handle_json(raw_values, table, columns,casted_columns,p_key_clause):
    records = json.loads(raw_values)
    rows = []
    for record in records:
        row = []
        row.extend(format_record(record[column]) for column in columns)
        rows.append(f"({','.join(row)})")
    
    insert(rows,table,f"({','.join(columns)})",casted_columns,p_key_clause)


def handle_patients(raw_values, topic):
    print('\nHandling patients')
    columns = "(address, city, date_of_birth, first_name, gender, last_name, patient_id, phone_number, state, zip_code)"
    casted_columns = 'address, city, CAST(date_of_birth as date), first_name, gender, last_name, CAST(patient_id AS uuid), phone_number, state, zip_code'
    p_key_clause = 'patient_id = CAST(NewValues.patient_id AS uuid)'
    handle_csv(raw_values, topic, columns,casted_columns,p_key_clause)


def handle_procedures(raw_values, topic):
    print('\nHandling procedures')
    # save procedures
    procedures_columns = "(date_performed, encounter_id, procedure_code, procedure_id)"
    p_casted_columns = "CAST(date_performed as date), CAST(encounter_id AS uuid), CAST(procedure_code AS int ), CAST(procedure_id AS uuid)"
    p_key_clause = "procedure_id = CAST(NewValues.procedure_id AS uuid)"
    procedures_indexes = [0,1,3,5]
    handle_csv(raw_values, topic, procedures_columns,p_casted_columns,p_key_clause, procedures_indexes)
    
    # save procedure types
    p_types_columns = "(procedures_code, procedure_description)"
    type_cast_columns = "CAST(procedures_code as int), procedure_description"
    t_key_clause = "procedures_code = CAST(NewValues.procedures_code AS int)"
    p_types_indexes = [3,4]
    handle_csv(raw_values, 'procedure_types', p_types_columns,type_cast_columns,t_key_clause, p_types_indexes)


def handle_observations(raw_values, topic):
    print('\nHandling observations')
    # save observations
    observations_columns = "(encounter_id, observation_code, observation_datetime, observation_id, units, value)"
    o_casted_columns = 'CAST(encounter_id as uuid), observation_code, CAST(observation_datetime as date),CAST(observation_id as uuid),units, value'
    o_key_clause = 'observation_id = CAST(NewValues.observation_id AS uuid)'
    observations_indexes = [0,1,2,4,6]
    o_merge = [7,8]
    handle_csv(raw_values, topic, observations_columns,o_casted_columns,o_key_clause,observations_indexes,o_merge)
    
    # save observation types
    o_types_columns = "(observation_code, observation_description)"
    t_casted_columns = 'observation_code, observation_description'
    t_key_clause = "observation_code = NewValues.observation_code"
    o_types_indexes = [1,3]
    handle_csv(raw_values, 'observation_types', o_types_columns,t_casted_columns,t_key_clause,o_types_indexes)


def handle_diagnoses(raw_values, topic):
    print('\nHandling diagnoses')
    diagnoses_columns = ['diagnosis_id','encounter_id','date_recorded','diagnosis_code']
    d_casted_columns = "CAST(diagnosis_id as uuid),CAST(encounter_id as uuid),CAST(date_recorded as date), diagnosis_code"
    d_key_clause = "diagnosis_id = CAST(NewValues.diagnosis_id AS uuid)"
    handle_json(raw_values, topic, diagnoses_columns,d_casted_columns,d_key_clause)
    
    d_type_columns = ['diagnosis_code','diagnosis_description']
    t_casted_columns = 'diagnosis_code, diagnosis_description'
    t_key_clause = 'diagnosis_code = NewValues.diagnosis_code'
    handle_json(raw_values, 'diagnose_types', d_type_columns,t_casted_columns,t_key_clause)

def handle_medications(raw_values, topic):    
    print('\nHandling medications')
    medications_columns = ['medication_order_id','encounter_id','dosage','frequency','start_date','end_date']
    m_casted_columns = 'CAST(medication_order_id as uuid),CAST(encounter_id as uuid),dosage,frequency,CAST(start_date as date),CAST(end_date as date)'
    m_key_clause = 'medication_order_id = CAST(NewValues.medication_order_id AS uuid)'
    handle_json(raw_values, topic, medications_columns,m_casted_columns,m_key_clause)
    
    drugs_columns = ['drug_code','drug_name']
    d_casted_columns = 'CAST(drug_code as int), drug_name'
    d_key_clause = 'drug_code = CAST(NewValues.drug_code AS int)'
    handle_json(raw_values, 'drugs', drugs_columns,d_casted_columns,d_key_clause)

def handle_encounters(raw_values, topic):
    print('\nHandling encounters')
    encounters_columns = ['admission_date','attending_physician_id','discharge_date','encounter_id','patient_id']
    casted_columns = "CAST(admission_date as date), attending_physician_id, CAST(discharge_date as date), CAST(encounter_id AS uuid), CAST(patient_id AS uuid)"
    p_key_clause = "encounter_id = CAST(NewValues.encounter_id AS uuid)"
    handle_json(raw_values, topic, encounters_columns,casted_columns ,p_key_clause)

