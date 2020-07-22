from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from sqlalchemy import create_engine
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestClassifier


default_args = {
    'owner': 'test_user',
    'start_date': datetime(2020, 7, 21)
}


def transform_data():
    engine = create_engine('postgresql://usr:passw@127.0.0.1:5432')
    conn = engine.connect()
    
    df_raw = pd.read_sql('select * from churn', conn)
    df_raw['gender'] = df_raw['gender'].apply(lambda x: 1 if x=='Male' else 0)
    df_raw['Partner'] = df_raw['Partner'].apply(lambda x: 1 if x=='Yes' else 0)
    df_raw['Dependents'] = df_raw['Dependents'].apply(lambda x: 1 if x=='Yes' else 0)
    df_raw['PhoneService'] = df_raw['PhoneService'].apply(lambda x: 1 if x=='Yes' else 0)
    df_raw['PaperlessBilling'] = df_raw['PaperlessBilling'].apply(lambda x: 1 if x=='Yes' else 0)
    df_raw['TotalCharges'] = df_raw['TotalCharges'].apply(lambda x: -1 if x==' ' else float(x))
    
    conn.execute('drop table if exists churn_features')

    with open('path/features.pickle', 'rb') as f:
    	features = pickle.load(f)

    df_raw[['customerID', 'date']+features].to_sql('churn_features', conn, index=False, if_exists='append')



def predict():
    engine = create_engine('postgresql://usr:passw@127.0.0.1:5432')
    conn = engine.connect()
    
    df = pd.read_sql('select * from churn_features', conn)

    with open('path/clf.pickle', 'rb') as f:
        clf = pickle.load(f)

    with open('path/features.pickle', 'rb') as f:
    	features = pickle.load(f)

    df['score'] = clf.predict_proba(df[features])[:,1]
    conn.execute('drop table if exists churn_score')
    df[['date', 'customerID', 'score']].to_sql('churn_score', conn, index=False, if_exists='append')



dag = DAG(
    dag_id='predict_churn',
    default_args=default_args,
    start_date=datetime(2020, 7, 21),
    schedule_interval='30 5 * * *'
)

get_features = PythonOperator(
    task_id='get_features',
    python_callable=transform_data,
    dag=dag,
)

predict = PythonOperator(
    task_id='predict',
    python_callable=predict,
    dag=dag,
)


get_features >> predict