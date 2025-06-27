from SecureHospitalSender import SecureHospitalSender

def run():
    patients = './raw_data/patients.csv'
    encounters = './raw_data/ehr_journeys_database.sqlite'
    procedures = './raw_data/procedures.csv'
    observations = './raw_data/observations.csv'
    diagnoses = './raw_data/diagnoses.json'
    medications = './raw_data/medications.json'
    
    node_1 = SecureHospitalSender()
    node_2 = SecureHospitalSender(use_json=True)
    node_3 = SecureHospitalSender(use_json=True)
    
    
    node_1.send_csv(patients)
    node_2.send_sqlite(encounters)
    node_1.send_csv(procedures)
    node_1.send_csv(observations)
    node_3.send_json(diagnoses)
    node_3.send_json(medications)

if __name__ == "__main__":
    run()