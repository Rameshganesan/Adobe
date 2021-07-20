import os
import io
import boto3
import logging
from time import sleep
import pandas as pd
import sys
import datetime

logging.basicConfig(level=logging.INFO)

class AnalyzeClientData():
    def __init__(self, filename):
        self.filename = filename
        self.session = boto3.Session(profile_name='saml')
        self.bucket_name = 'rganesan-athena-bucket'
        self.sleep_time = 30 #seconds wait time
        self.try_limit = 10  #num of times to Check for the query status
        self.log = logging.getLogger('AnalyzeClientData')
        self.s3_client = self.session.client('s3')
        self.athena_conn = self.session.client('athena')

    def upload_hit_data(self, file_name):
        try:
            self.s3_client.upload_file(file_name, self.bucket_name, 'incoming/' + os.path.basename(file_name))
        except Exception as e:
            self.log.error('Something went wrong'+str(e))
            raise Exception(str(e))
        return os.path.basename(file_name).split('_')[0]

    def execute_athena_query(self, file_date):

        query = """SELECT split_part(z.referrer, '/', 3) as search_engine_domain,
                    substr(substr(z.referrer,strpos(z.referrer,'q=')+2),1,strpos(substr(z.referrer,strpos(z.referrer,'q=')+2),'&')-1) as search_keyword,
                    sum(cast(split_part(a.value, ';', 4) as real)) as revenue
                    FROM (Select x.ip, x.product_list, y.referrer from rganesan_db.hit_data x
                    inner join (Select *, row_number() over(partition by ip order by date_time) rn from rganesan_db.hit_data 
                    where date(date_time) = date('{file_date}')) y
                    on (x.ip = y.ip)
                    where date(x.date_time) = date('{file_date}') 
                     and x.event_list = '1'
                     and y.rn = 1) z
                    CROSS JOIN UNNEST(split(z.product_list, ',')) as a(value)
                    group by 1,2
                    order by 3 desc;""".format(file_date=file_date)
        self.log.info(query)
        res = self.athena_conn.start_query_execution(QueryString=query,
                                                QueryExecutionContext={'Database': 'rganesan_db'},
                                                ResultConfiguration={
                                                    'OutputLocation': 's3://{bucket}/output/'.format(bucket=self.bucket_name)},
                                                WorkGroup='primary')
        return res['QueryExecutionId']

    def poll_query_completion(self, query_id):
        try_iterator = 0
        while True:
            try_iterator = try_iterator + 1
            response = self.athena_conn.get_query_execution(QueryExecutionId=query_id)
            if response['QueryExecution']['Status']['State'] is None:
                self.log.info('No query state, waiting to try...')
            elif response['QueryExecution']['Status']['State'] in ('QUEUED', 'RUNNING'):
                self.log.info('query is still running')
            elif response['QueryExecution']['Status']['State'] in ('FAILED'):
                self.log.info('query failed')
                raise Exception('Query failed with error: ' + response['QueryExecution']['Status']['StateChangeReason'])
            else:
                self.log.info('query completed!')
                break
            if try_iterator >= self.try_limit:
                self.log.info('After {x} attempts,query might be still running'.format(x=self.try_limit))
                break
            sleep(self.sleep_time)
        self.log.info(response['QueryExecution']['Status']['State'])
        return response['QueryExecution']['Status']['State']

    def write_output_file(self, query_id, file_date):
        if self.poll_query_completion(query_id) == 'SUCCEEDED':
            s3_conn = self.session.resource('s3')
            key='output/{qid}.csv'.format(qid=query_id)
            res = s3_conn.Bucket(self.bucket_name).Object(key=key).get()['Body'].read()
            df = pd.read_csv(io.BytesIO(res), encoding='utf8')
            self.log.info(df)
            target_filename = 'outgoing/{file_date}_SearchKeywordPerformance.tab'.format(file_date=file_date)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep='\t', header=True, doublequote=True, index=False)
            s3_conn.Object(self.bucket_name, target_filename).put(Body=csv_buffer.getvalue())
            self.s3_client.download_file(self.bucket_name, target_filename, target_filename.split('/')[1])
        else:
            self.log.info('Query might be failed or still running, exiting.. try again later with {qid}'.format(qid=query_id))

    def run(self):
        file_date = self.upload_hit_data(self.filename)
        query_id = self.execute_athena_query(file_date)
        self.write_output_file(query_id, file_date)

if __name__ == "__main__":
    file_path_with_name = sys.argv[1]
    logging.getLogger('main')

    dt = os.path.basename(file_path_with_name)[0:10]
    try:
        dobj = datetime.datetime.strptime(dt, '%Y-%m-%d')
        obj = AnalyzeClientData(file_path_with_name)
        obj.run()
    except ValueError:
        logging.error("File date is wrong or missing, File name should start with 'YYYY-MM-DD' date format")
        exit(1)
