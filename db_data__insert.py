import os
from tqdm import tqdm
import pandas as pd
import shutil
import toloka.client as toloka
import psycopg2

# YANDEX TOLOKA CREDENTIALS
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = 'OAUTH_TOKEN'
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

# DB CONNECT AND PUT ALL DATA TO DF
conn = psycopg2.connect("""
    host=host
    port=port
    sslmode=require
    dbname=dbname
    user=user
    password=password
    target_session_attrs=read-write
""")

q = conn.cursor()

q.execute('''SELECT assignment_id, worker_id, assignment_nation, assignment_month,
                                              assignment_send_date, assignment_toloka_date, toloka_status,
                                              reward, account, pool_type, innodata_decision, reject_reason,
                                              hashes, gender FROM public.sets ''')
all_sets_in_db_df = pd.DataFrame(q.fetchall(), columns = ['assignment_id', 'worker_id', 'assignment_nation', 'assignment_month', 'assignment_send_date', 'assignment_toloka_date', 'toloka_status', 'reward', 'account', 'pool_type', 'innodata_decision', 'reject_reason', 'hashes', 'gender'])

# GET DATE AND MONTH FROM TABLE FILE NAME
for file in os.listdir('descriptors/credentials/table'):
    if 'db_sets_for' in file:
        daily_df = pd.read_excel(f'descriptors/credentials/table/{file}', sheet_name='Sheet1')
        date = file.split('_')[4].replace('.xlsx', '')
        month = file.split('_')[3]

print(daily_df)

pool_id_1 = 1
assignment_id_1 = 1

full_df = pd.DataFrame()

# GETTING ALL SET DATA FROM YANDEX TOLOKA TO DF
for assignment_id in daily_df['assignment_id']:
    try:
        print(assignment_id)
        account = 'account1'
        assignment_nation = daily_df[daily_df['assignment_id']==assignment_id]['ethnicity'].values[0]
        assignment_gender = daily_df[daily_df['assignment_id']==assignment_id]['gender'].values[0]
        assignment_hashes = daily_df[daily_df['assignment_id']==assignment_id]['hashes'].values[0]
        assignment_month = month
        assignment_send_date = date
        if assignment_id != assignment_id_1:
            if '--' in assignment_id:
                print('Getting data')
                assignment_request = toloka_client.get_assignment(assignment_id=assignment_id)
                pool_id = assignment_request.pool_id
                if pool_id_1 != pool_id:
                    df_toloka = toloka_client.get_assignments_df(pool_id, status=['APPROVED', 'SUBMITTED', 'REJECTED'])
                    pool_id_1 = pool_id
                assignment_toloka_date = df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:started'].values[0].split('T')[0]
                worker_id = df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:worker_id'].values[0]
                toloka_status = df_toloka[df_toloka['ASSIGNMENT:assignment_id'] == assignment_id]['ASSIGNMENT:status'].values[0]
                reward = assignment_request.reward
                pool_data = toloka_client.get_pool(pool_id=assignment_request.pool_id)
                pool_name = pool_data.private_name
                if 'new' in pool_name.lower() and not 'retry' in pool_name.lower() and not 'родствен' in pool_name.lower():
                    pool_type = 'new'
                elif 'retry' in pool_name.lower():
                    pool_type = 'retry'
                elif 'родствен' in pool_name.lower():
                    pool_type = 'родственники'
                else:
                    pool_type = None
            else:
                assignment_toloka_date = 'inhouse'
                worker_id = 'inhouse'
                toloka_status = 'inhouse'
                reward = 'inhouse'
                account = 'inhouse'
                pool_type = 'inhouse'
            innodata_decision = 'IN WORK'
            reject_reason = None
            data = {'assignment_id':[assignment_id], 'worker_id':[worker_id], 'assignment_nation':[assignment_nation], 'assignment_month':[assignment_month], 'assignment_send_date':[assignment_send_date], 'assignment_toloka_date':[assignment_toloka_date], 'toloka_status':[toloka_status], 'reward':[reward], 'account':[account], 'pool_type':[pool_type], 'innodata_decision':[innodata_decision], 'reject_reason':[reject_reason], 'hashes':[assignment_hashes], 'gender':[assignment_gender]}
            df = pd.DataFrame(data=data)
            full_df = pd.concat([full_df, df])
            assignment_id_1 = assignment_id
        else:
            pass
# IF EXCEPTION - CHANGE ACCOUNT TO ANOTHER AND TRY AGAIN
    except Exception as e:
        if 'DoesNotExistApiError' in str(e):
            print('Change account')
            if OAUTH_TOKEN == 'OAUTH_TOKEN1':
                OAUTH_TOKEN = 'OAUTH_TOKEN2'
                account = 'account2'
            elif OAUTH_TOKEN == 'OAUTH_TOKEN2':
                OAUTH_TOKEN = 'OAUTH_TOKEN1'
                account = 'account1'
            HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
            toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
        else:
            print(e)

df = full_df

q = conn.cursor()

# PUT ALL DATA FROM OUR DF TO DB
for i in tqdm(df['assignment_id']):
    worker_id = str(df[df['assignment_id']==i]['worker_id'].values[0])
    assignment_nation = str(df[df['assignment_id']==i]['assignment_nation'].values[0])
    assignment_month = str(df[df['assignment_id']==i]['assignment_month'].values[0])
    assignment_send_date = str(df[df['assignment_id']==i]['assignment_send_date'].values[0])
    assignment_toloka_date = str(df[df['assignment_id']==i]['assignment_toloka_date'].values[0])
    innodata_decision = str(df[df['assignment_id']==i]['innodata_decision'].values[0])
    reject_reason = str(df[df['assignment_id']==i]['reject_reason'].values[0])
    toloka_status = str(df[df['assignment_id']==i]['toloka_status'].values[0])
    reward = str(df[df['assignment_id']==i]['reward'].values[0])
    account = str(df[df['assignment_id']==i]['account'].values[0])
    pool_type = str(df[df['assignment_id']==i]['pool_type'].values[0])
    hashes = str(df[df['assignment_id']==i]['hashes'].values[0])
    gender = str(df[df['assignment_id']==i]['gender'].values[0])
    # descriptor = str(df[df['assignment_id']==i]['descriptor'].values[0])
    print(f'{i}, {worker_id}, {assignment_nation}, {assignment_month}, {assignment_send_date}, {assignment_toloka_date}, {innodata_decision}, {reject_reason}')
    try:
        if i in all_sets_in_db_df['assignment_id'].unique():
            q.execute(f"UPDATE public.sets SET worker_id = '{worker_id}', assignment_nation = '{assignment_nation}', assignment_month = '{assignment_month}', assignment_send_date = '{assignment_send_date}', assignment_toloka_date = '{assignment_toloka_date}', toloka_status = '{toloka_status}', reward = '{reward}', account = '{account}', pool_type = '{pool_type}', innodata_decision = '{innodata_decision}', reject_reason = '{reject_reason}', hashes = '{hashes}', gender = '{gender}' WHERE assignment_id = '{i}';")
            print('Update set in DB')
        else:
            q.execute(f"INSERT INTO public.sets (assignment_id, worker_id, assignment_nation, assignment_month, assignment_send_date, assignment_toloka_date, toloka_status, reward, account, pool_type, innodata_decision, reject_reason, hashes, gender) VALUES ('{i}', '{worker_id}', '{assignment_nation}', '{assignment_month}', '{assignment_send_date}', '{assignment_toloka_date}', '{toloka_status}', '{reward}', '{account}', '{pool_type}', '{innodata_decision}', '{reject_reason}', '{hashes}', '{gender}');")
            print('Add set to DB')
    except Exception as e:
        print(e)
        pass
conn.commit()

conn.close()

# MOVING ALL FILES TO STORAGE IF OK
finish = input('All right? ')
destination_path = 'destination_path'

if finish == '+':
    for root, subdirectories, files in os.walk('.\\descriptors\\credentials\\table'):
        for subdirectory in subdirectories:
            pass
        for file in files:
            try:
                shutil.move(os.path.join(root, file), destination_path)
            except:
                pass