import psycopg2
from db_connection import fetch_one, connect

QUERIES =[
    # What is the average age and age range of the patients in the data?
    ("SELECT EXTRACT(YEAR FROM AVG(AGE(date_of_birth))) as average_age , "
    "EXTRACT(YEAR FROM MIN(AGE(date_of_birth))) as min_age, "
    "EXTRACT(YEAR FROM MAX(AGE(date_of_birth))) as max_age from patients;"),
    # How many patients does a physician see on average over the span of the data?
    ("SELECT AVG(visit_count) from ( "
    "SELECT COUNT(patient_id) AS visit_count FROM encounters "
    "GROUP BY attending_physician_id) a;"),
    # Most Common Diagnose type
    ("SELECT COUNT(diagnoses.diagnosis_id) AS count,t.diagnosis_description, diagnoses.diagnosis_code FROM diagnoses "
    "JOIN diagnose_types t ON diagnoses.diagnosis_code = t.diagnosis_code "
    "GROUP BY diagnoses.diagnosis_code, t.diagnosis_description "
    "ORDER BY count DESC;"),
    # Most Commonly Administered Procedure
    ("SELECT COUNT(p.procedure_id) AS count, t.procedure_description, t.procedures_code FROM procedures p "
    "JOIN procedure_types t ON p.procedure_code = t.procedures_code "
    "group by  t.procedures_code, t.procedure_description "
    "ORDER BY count DESC "
    "LIMIT 1;"),
    # Number of Procedures Per Patient
    ("SELECT COUNT(p.procedure_id) AS count , e.patient_id FROM procedures p "
    "JOIN encounters e ON p.encounter_id = e.encounter_id "
    "group by e.encounter_id "
    "ORDER BY count DESC "
    "LIMIT 5;")
]


def fetch_data():
    conn= None
    try:
        conn= connect()
        results = []
        results.extend(fetch_one(conn,query) for query in QUERIES)
        print(results)
        print('TEST',results[4])
        return results
    except (Exception, psycopg2.DatabaseError) as error:
        print('Query Error:')
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

if __name__ == "__main__":
    fetch_data()