from airflow.models import Variable
import configparser

config = configparser.ConfigParser()
config.read_file(open('config/config.cfg'))

S3_SONG_KEY = config.get('S3','S3_SONG_KEY')
S3_LOG_KEY = config.get('S3','S3_LOG_KEY')
S3_BUCKET = config.get('S3','S3_BUCKET')

Variable.set("S3_SONG_KEY", S3_SONG_KEY)
Variable.set("S3_LOG_KEY", S3_LOG_KEY)
Variable.set("S3_BUCKET", S3_BUCKET)