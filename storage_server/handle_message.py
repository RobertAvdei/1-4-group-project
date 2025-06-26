import json
from db_connection import run_single_query

def handle_message(msg):
    topic = msg.topic
    raw_values = msg.value.decode('utf-8')
    match topic:
        case 'patients':
            print('Reading patients message')
            handle_patients(raw_values, topic)
        case 'encounters':
            print('Reading encounters message')
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


def handle_csv(raw_values, table, columns, indexes=None):
    if indexes is None:
        indexes = range(len(raw_values))
        
    rows = []
    raw_rows: list[str] = raw_values.splitlines()
    
    del raw_rows[0] # remove columns
    for index in  range(len(raw_rows)):
        if index in indexes:
            value = raw_rows[index]
            if value is not None:
                row = f'({value})'
                rows.append(row)
    query =f"INSERT INTO {table} {columns} VALUES {','.join(rows)}"
    print(query)
    # TODO: Remove comment later
    # run_single_query(query)


def handle_json(raw_values, table, columns):
    records = json.loads(raw_values)
    rows = []
    for record in records:
        row = []
        row.extend(record[column] for column in columns)
        rows.append(f"({','.join(row)})")
    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES {','.join(rows)}"
    print(query)
    # TODO: Remove comment later
    # run_single_query(query)


def handle_patients(raw_values, topic):
    print('Handling patients')
    columns = "(patient_id, address, city, date_of_birth, first_name, gender, last_name, phone_number, state, zip_code)"
    handle_csv(raw_values, topic, columns)


def handle_procedures(raw_values, topic):
    print('Handling procedures')
    # save procedures
    procedures_columns = "(date_performed, encounter_id, procedure_code, procedure_id)"
    procedures_indexes = [0,1,3,5]
    handle_csv(raw_values, topic, procedures_columns,procedures_indexes)
    
    # save procedure types
    p_types_columns = "(procedures_code, procedure_description)"
    p_types_indexes = [3,4]
    handle_csv(raw_values, 'procedure_types', p_types_columns,p_types_indexes)


def handle_observations(raw_values, topic):
    print('Handling observations')
    # save observations
    observations_columns = "(encounter_id, observation_code, observation_datetime, observation_id, units, value)"
    observations_indexes = [0,1,2,4,6,7,8]
    # o_merge = [7,8]
    handle_csv(raw_values, topic, observations_columns,observations_indexes)
    
    # save observation types
    o_types_columns = "(observation_code, observation_description)"
    o_types_indexes = [1,3]
    handle_csv(raw_values, 'observation_types', o_types_columns,o_types_indexes)


def handle_diagnoses(raw_values, topic):
    print('Handling diagnoses')
    diagnoses_columns = ['diagnosis_id','encounter_id','date_recorded']
    handle_json(raw_values, topic, diagnoses_columns)
    
    d_type_columns = ['diagnosis_code','diagnosis_description']
    handle_json(raw_values, 'diagnose_types', d_type_columns)

def handle_medications(raw_values, topic):    
    print('Handling medications')
    medications_columns = ['medication_order_id','encounter_id','dosage','frequency','start_date','end_date']
    handle_json(raw_values, topic, medications_columns)
    
    m_type_columns = ['drug_code','drug_name']
    handle_json(raw_values, 'drugs', m_type_columns)

def handle_encounters(raw_values, topic):
    print('Handling encounters')
    encounters_columns = ['admission_date','attending_physician_id','discharge_date','encounter_id','patient_id']
    handle_json(raw_values, topic, encounters_columns)

