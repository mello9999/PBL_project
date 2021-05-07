from airflow import settings
from airflow.models import Connection

import configparser
config = configparser.ConfigParser()
config.read_file(open('config/config.cfg'))

aws_conn = Connection(
        conn_id=conf1ig.get('AWS', 'CONN_ID'),
        conn_type=config.get('AWS','CONN_TYPE'),
        login=config.get('AWS','LOGIN'),
        password=config.get('AWS','PASSWORD'),
) 

redshift_conn = Connection(
        conn_id=config.get('REDSHIFT', 'CONN_ID'),
        conn_type=config.get('REDSHIFT','CONN_TYPE'),
        host=config.get('REDSHIFT','HOST'),
        schema=config.get('REDSHIFT','DB_NAME'),
        login=config.get('REDSHIFT','DB_USER'),
        password=config.get('REDSHIFT','DB_PASSWORD'),
        port=config.get('REDSHIFT','DB_PORT')
) 

session = settings.Session() 
session.add(aws_conn)
session.add(redshift_conn)
session.commit() 