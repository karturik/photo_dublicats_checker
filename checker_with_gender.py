import os
import io
import shutil
import hashlib
import datetime
import time
import requests
import json
import concurrent.futures

from tqdm import tqdm
import pandas as pd
from PIL import Image
import numpy as np

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

import psycopg2

# DATABASE CONNECTION AND GET ALL SETS AS DF
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
q.execute('''SELECT assignment_id, gender FROM public.sets ''')
all_sets_in_db_df = pd.DataFrame(q.fetchall(), columns = ['assignment_id', 'gender'])

# GET ALL DESCRIPTORS FROM FILE TO DF
if not os.path.exists('descriptors/descriptors_base.csv'):
    os.makedirs('descriptors/descriptors_base.csv')
df_with_descriptors = pd.read_csv('descriptors/descriptors_base.csv', sep=',').drop_duplicates(
    subset='assignment_id')
df_with_descriptors1 = df_with_descriptors

# GOOGLE DRIVE API SETTINGS
CLIENT_SECRET = "descriptors/credentials/token1.json"

SCOPES='https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'
store = file.Storage('descriptors/credentials/token.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
    creds = tools.run_flow(flow, store)
SERVICE = build('drive', 'v3', http=creds.authorize(Http()))
SS_SERVICE = build('sheets', 'v4', http=creds.authorize(Http()))

# GETTING STARTED
retry = False

new_df_for_table = pd.DataFrame(columns=['link', 'assignment_id', 'gender', 'age'], data = None)

server_uri = 'server_uri'

open('log_new.txt', 'w').close()

e = datetime.datetime.now()
date = f"{'%s_%s_%s_%s_%s_%s' % (e.day, e.month, e.year, e.hour, e.minute, e.second)}"

new_dublicats = False
old_dublicats = False
no_new_dublicats = False
no_old_dublicats = False

if os.path.exists('hashes_base.tsv'):
    full_hashes_df = pd.read_csv('hashes_base.tsv', sep='\t')
else:
    full_hashes_df = pd.DataFrame(columns=['File_path', 'File_hash'], data=None)

hash_dict = {}

extract_dir = "photos/photos_from_drive"
copy_extract_dir = "copy_extract_dir"
destination = "destination"

# SELECT PREFIX WITH ETHNOCITY AND MONTH FOR PATH ON DISK
prefix = input('Path prefix: \n 1.Africans\\January\\\n 2.Arabians\\January\\\n3.Caucasians\\January\\\n4.East Asia\\January\\\n5.Hispanic\\January\\\n6.South Asia\\January\\\n7.Caucasians2\\January\\\n8.retry\\\n')
if prefix == '1':
    prefix = 'Africans\\January\\'
elif prefix == '2':
    prefix = 'Arabians\\January\\'
elif prefix == '3':
    prefix = 'Caucasians\\January\\'
elif prefix == '4':
    prefix = 'East Asia\\January\\'
elif prefix == '5':
    prefix = 'Hispanic\\January\\'
elif prefix == '6':
    prefix = 'South Asia\\January\\'
elif prefix == '7':
    prefix = 'Caucasians2\\January\\'
elif prefix == '8':
    retry = True
    retry_prefix = input('Префикс пути:\n 1.Africans\\January\\\n 2.Arabians\\January\\\n3.Caucasians\\January\\\n4.East Asia\\January\\\n5.Hispanic\\January\\\n6.South Asia\\January\\\n7.Caucasians2\\January\\\n')
    if retry_prefix == '1':
        prefix = 'retry\\' + 'Africans\\January\\'
    elif retry_prefix == '2':
        prefix = 'retry\\' + 'Arabians\\January\\'
    elif retry_prefix == '3':
        prefix = 'retry\\' + 'Caucasians\\January\\'
    elif retry_prefix == '4':
        prefix = 'retry\\' + 'East Asia\\January\\'
    elif retry_prefix == '5':
        prefix = 'retry\\' + 'Hispanic\\January\\'
    elif retry_prefix == '6':
        prefix = 'retry\\' + 'South Asia\\January\\'
    elif retry_prefix == '7':
        prefix = 'retry\\' + 'Caucasians2\\January\\'

# UNPACK NEW ARCHIVES FROM DIR TO "PHOTOS_FROM_DRIVE" DIR
for root, subdirectories, files in os.walk('photos/new_archives'):
    for file in tqdm(files):
        archive_format = file.split('.')[1]
        shutil.unpack_archive(os.path.join(root, file), extract_dir, archive_format)
        print(file, "archive unpacked successfully.")

list_of_new_descriptors = []

# SELECT DIR NUM = DATE
dir_num = os.listdir('photos/photos_from_drive')[0]

# SAVE ALL DIR DATA FROM GOOGLE DRIVE FOR NOT REQUESTING IT EVERY TIME
if not os.path.exists(f'descriptors/credentials/table/drive_data_for_{dir_num}.txt'):
    results = SERVICE.files().list(q="mimeType='application/vnd.google-apps.folder' and trashed=false", fields="nextPageToken, files(id, name)", pageSize=1000, spaces='drive').execute()
    drive_file = open(f'descriptors/credentials/table/drive_data_for_{dir_num}.txt', 'w', encoding='utf-8')
    drive_file.write(str(results))
    drive_file.close()
else:
    results = open(f'descriptors/credentials/table/drive_data_for_{dir_num}.txt', 'r', encoding='utf-8').read()
    results = json.loads(results.replace("'", '"').replace('""', '"'))

# GOING THROUGH DIRS AND GET SET AGE, GENDER, GOOGLE DRIVE LINK TO ONE DF
for root, subdirectories, files in os.walk('photos/photos_from_drive'):
    for file in files:
        data = []
        if '.xlsx' in file:
            path = os.path.join(root, file).replace('/', '\\')
            print(path)
            assignment_id = path.split('\\')[-2]
            print(assignment_id)
            try:
                df = pd.read_excel(path, sheet_name='Sheet1')
            except:
                df = pd.read_excel(path, sheet_name='Лист1')
            try:
                gender = df['Unnamed: 5'][4]
                age = int(df['Unnamed: 6'][4])
                if age >= 13 and age <= 19:
                    age = '13-19'
                elif age >= 20 and age <= 40:
                    age = '20-40'
                elif age >= 41 and age <= 60:
                    age = '41-60'
                elif age >= 61 and age <= 80:
                    age = '61-80'
            except:
                gender = ""
                age = ""
            for i in results['files']:
                if assignment_id in i['name']:
                    drive_id = i['id']
                    drive_link = 'https://drive.google.com/drive/folders/' + str(drive_id)
                    data = {'link': [drive_link],
                            'assignment_id': [assignment_id.replace('@', '')], 'gender': [gender.lower()], 'age': [age], 'hashes': None}
                    print(data)
                    new_df1 = pd.DataFrame(data=data)
                    new_df_for_table = pd.concat([new_df_for_table, new_df1])

full_df_hash_for_db = pd.DataFrame()

# HASH CREATE FUNCTION
def hash_create(full_df_hash_for_db):
    full_df1_hash_for_db = pd.DataFrame()
    for i in full_df_hash_for_db['assignment_id'].unique():
        df_one_set_hash_for_db = full_df_hash_for_db[full_df_hash_for_db['assignment_id']==i]
        assignment_id = i
        set_hashes = {}
        for file_name in df_one_set_hash_for_db['file_name']:
            set_hashes[file_name] = df_one_set_hash_for_db[df_one_set_hash_for_db['file_name']==file_name]['hash'].values[0]
        df1_hash_for_db = pd.DataFrame(data={'assignment_id':[assignment_id], 'files_hashes':[set_hashes]})
        full_df1_hash_for_db = pd.concat([full_df1_hash_for_db, df1_hash_for_db])
    print('----------------------------------------------------')
    return full_df1_hash_for_db

# GOING THROUGH DIRS AND GET DESCRIPTOR FROM SERVER AND HASH, SAVE THEM TO DF AND DB
data_to_db = ''
df_for_local_hash_base = pd.DataFrame()
for root, subdirectories, files in os.walk('photos/photos_from_drive'):
    for subdirectory in subdirectories:
        pass
    for file in files:
        hash_dict[str(prefix) + root.replace('photos/photos_from_drive\\', '')] = hash
        file_name_lower = file.lower()
        if "phone" in file_name_lower and "outd" in file_name_lower and "01" in file_name_lower and not "Trainingdata" in root:
            descriptor_file_name = str(prefix).replace("\\", "%") + os.path.join(root, file)\
                .replace('photos/photos_from_drive\\', '').replace(".jpeg", "").replace(".JPG","")\
                .replace(".jpg","").replace(".heic", "").replace(".PNG", "").replace(".png", "").replace("\\", "%")\
                                   + ".txt"
            print(descriptor_file_name)
            row_for_descriptor = descriptor_file_name.split('%')
            nation_for_descriptor = row_for_descriptor[0]
            month_for_descriptor = row_for_descriptor[1]
            date_for_descriptor = row_for_descriptor[2]
            assignment_id_for_descriptor = row_for_descriptor[3]
            tries = 0
            success = False
            while success != True:
                try:
                    img = Image.open(os.path.join(root, file), 'r')
                    maxsize = 512, 512
                    with io.BytesIO() as out:
                        img.thumbnail(maxsize, Image.ANTIALIAS)
                        img.convert('RGB').save(out, format='jpeg')
                        res = out.getvalue()
                    url = server_uri + "/get_descriptor"
                    f = {'image': res}
                    start_time = time.time()
                    r = requests.post(url, files=f)
                    if not '200' in str(r.status_code):
                        raise NameError('Code not 200')
                    descriptor = str(list(bytes(r.content))).replace("'", "").replace("[", "").replace("]", "")
                    data_to_db = descriptor
                    data = {'assignment_id': [assignment_id_for_descriptor], 'nation': [nation_for_descriptor], 'month': [month_for_descriptor], 'date': [date_for_descriptor],'descriptor': [descriptor]}
                    tmp_df = pd.DataFrame(data=data)
                    df_with_descriptors1 = pd.concat([df_with_descriptors1, tmp_df])
                    print(descriptor_file_name, ' descriptor finished')
                    list_of_new_descriptors.append(assignment_id_for_descriptor)
                    success = True
                    break
                except Exception as e:
                    tries += 1
                    if tries == 10:
                        with open('log_new.txt', 'a', encoding='utf-8') as log:
                            log.write(os.path.join(root, file) + '\t' + str(e) + '\n')
                            log.close()
                        success = True
        path = os.path.join(root, file).replace('/', '\\')
        assignment_id = path.split('\\')[-2]
        hash = hashlib.md5(open(os.path.join(root, file), 'rb').read()).hexdigest()
        df_hash_for_db = pd.DataFrame(data={'assignment_id': [assignment_id], 'file_name': [file], 'hash': [hash], 'descriptor': [data_to_db]})
        df_for_local_hash_base_1 = pd.DataFrame(data={'File_path': [str(prefix) + root.replace('photos/photos_from_drive\\', '') + '/' + file], 'File_hash':[hash]})
        df_for_local_hash_base = pd.concat([df_for_local_hash_base, df_for_local_hash_base_1])
        full_df_hash_for_db = pd.concat([full_df_hash_for_db, df_hash_for_db])
    if not full_df_hash_for_db.empty:
        full_df1_hash_for_db = hash_create(full_df_hash_for_db)
        work_dict_hash_for_db = full_df1_hash_for_db['files_hashes'].values[0]
        if len(work_dict_hash_for_db) == 11:
            work_dict_hash_for_db = str(work_dict_hash_for_db).replace("'", '"')
            q.execute(f"UPDATE public.sets SET hashes = '{work_dict_hash_for_db}' WHERE assignment_id = '{assignment_id}';")
            conn.commit()
            print(f'Add to DB for {assignment_id} hashes: {work_dict_hash_for_db}')
            new_df_for_table.loc[new_df_for_table['assignment_id']==assignment_id, ['hashes']] = work_dict_hash_for_db
        else:
            with open('not_11_files.tsv', 'a', encoding='utf-8') as file_no_11:
                file_no_11.write(assignment_id + '\n')
                file_no_11.close()
    full_df_hash_for_db = pd.DataFrame()

# SAVE NEW DESCRIPTORS LOCAL TO CSV FILE
df_with_descriptors1 = df_with_descriptors1.drop_duplicates(subset='assignment_id')
df_with_descriptors1.to_csv('descriptors/descriptors_base.csv', sep=',', index=False)

# SAVE ALL SETS DATA TO EXCEL TABS
if retry == False:
    national = prefix.replace("\\January\\", "")
    new_df_for_table = new_df_for_table.drop_duplicates(subset=['assignment_id'])
    new_df_for_table.to_excel(f'descriptors/credentials/table/table_for_{national}_January_{dir_num}.xlsx', index=True)
else:
    national = prefix.replace("\\January\\", "").replace("retry\\", "")
    new_df_for_table = new_df_for_table.drop_duplicates(subset=['assignment_id'])
    new_df_for_table.to_excel(f'descriptors/credentials/table/table_for_retry_{national}_January_{dir_num}.xlsx', index=True)

print(hash_dict)

df = df_for_local_hash_base

# FINDING DUBLICATS IN SETS BY HASHES WITHING NEW DIRS
try:
    new_hashes_copy = pd.concat(g for _, g in df.groupby("File_hash") if len(g) > 1)
    new_dublicats = 'Dublicats with new folders: '+ str(len(new_hashes_copy['File_path']))
    new_hashes_copy['File_path'].to_csv(f'new_copy_{date}.tsv', sep='\t', index=False)
    print('File with dublicats ', f'new_copy_{date}.tsv')
except ValueError:
    no_new_dublicats = 'There are no dublicats in new folders'

df.drop_duplicates(subset=['File_hash'], inplace=True)

full_hashes_df.drop_duplicates(subset=['File_hash'], inplace=True)

full_hashes_df = pd.concat([full_hashes_df, df])

# FINDING DUBLICATS IN SETS BY HASHES WITH OLD DIRS
try:
    old_hashes_copy = pd.concat(g for _, g in full_hashes_df.groupby("File_hash") if len(g) > 1)
    old_dublicats = 'Dublicats with old folders: '+ str(len(old_hashes_copy['File_path']))
    old_hashes_copy['File_path'].to_csv(f'old_copy_{date}.tsv', sep='\t', index=False)
    print('File with dublicats ', f'old_copy_{date}.tsv')
# print(copy)
except ValueError:
    no_old_dublicats = 'There are no dublicats with old folders'

full_hashes_df.drop_duplicates(inplace=True)

full_hashes_df.to_csv('hashes_base.tsv', sep='\t', index=False)

# MOVE ARCHIVES TO STORAGE
for root, subdirectories, files in os.walk('photos/new_archives'):
    for subdirectory in subdirectories:
        pass
    for file in tqdm(files):
        shutil.move(os.path.join(root, file), destination)

sets_number = 0

# CONSIDER NUMBER OF SETS IN EVERY ETHNOCITY
for root, subdirectories, files in os.walk('photos/photos_from_drive/'+dir_num):
    for subdirectory in subdirectories:
        if not 'Other' in subdirectory and not 'Toloka' in subdirectory:
            sets_number += 1

full_data = {}
full_data[prefix + dir_num] = sets_number

if os.path.exists('sets_count.tsv'):
    original_count_df = pd.read_csv('sets_count.tsv', sep='\t')
else:
    original_count_df = pd.DataFrame(columns=['Data_dir', 'Sets_count'], data=None)

count_df = pd.DataFrame(columns=['Data_dir', 'Sets_count'], data=full_data.items())

count_df = pd.concat([original_count_df, count_df])

count_df.to_csv('sets_count.tsv', sep='\t', index=False)

file = open('new_descriptors.txt', 'w', encoding='utf-8')
file.write(str(list_of_new_descriptors))
file.close()

# CREATE COPIES BY DESCRIPTORS FILE
copies_file = open(f'ai_dublicats.tsv', 'w', encoding='utf-8')
copies_file.write("new_file_path" + '\t' + "old_file_path" + "\t" + "similarity_score")
copies_file.close()

# FUNCTION FOR LOCAL DESCRIPTOR COMPARE
def comparer(desc_1, desc_2):
    d_1_a = np.array(list(desc_1))
    d_2_a = np.array(list(desc_2))
    d_1_a = (d_1_a - np.mean(d_1_a))/ np.std(d_1_a)
    d_2_a = (d_2_a - np.mean(d_2_a))/ np.std(d_2_a)
    dot_product = sum(v1*v2 for v1, v2 in zip(d_1_a, d_2_a))
    norm_a = sum([x**2 for x in d_1_a])**(1/2)
    norm_b = sum([x**2 for x in d_2_a])**(1/2)
    cos_sim = dot_product/(norm_a * norm_b)
    return cos_sim

# FUNCTION FOR COMPARE DESCRIPTOR ON SERVER AND LOCALY
def descriptor_checK(old_files, new_files):
        for new_file in tqdm(new_files):
            for old_file in old_files:
                success = False
                tries = 0
                while success != True:
                    try:
                        desc1 = df_with_descriptors1.loc[(df_with_descriptors1['assignment_id']==new_file), 'descriptor'].values[0]
                        desc1 = bytes([int(x) for x in str(desc1).replace("b'", "").replace("'", "").split(', ')])
                        desc2 = df_with_descriptors1.loc[(df_with_descriptors1['assignment_id'] == old_file), 'descriptor'].values[0]
                        desc2 = bytes([int(x) for x in str(desc2).replace("b'", "").replace("'", "").split(', ')])
                        url = server_uri + "/compare_descriptors"
                        f = {'desc_left': desc1, 'desc_right': desc2}
                        start_time = time.time()
                        r = requests.post(url, files=f, timeout=15)
                        if not '200' in str(r.status_code):
                            r = requests.post(url, files=f, timeout=15)
                            if not '200' in str(r.status_code):
                                raise NameError('Code not 200')
                        similarity_score = float(r.json()['similarity_score'])
                        print(new_file, " ", old_file, similarity_score)
                        similarity_score_by_comparer = comparer(desc1, desc2)
                        if similarity_score > 0.5:
                            with open(f'ai_dublicats.tsv', 'a', encoding='utf-8') as copies_file:
                                copies_file.write("\n" + new_file)
                                copies_file.close()
                        if similarity_score_by_comparer > 0.3:
                            with open(f'ai_dublicats.tsv', 'a', encoding='utf-8') as copies_file:
                                copies_file.write("\n" + new_file)
                                copies_file.close()
                        success = True
                    except Exception as e:
                        tries += 1
                        if tries == 10:
                            with open('log_new.txt', 'a', encoding='utf-8') as log:
                                log.write(date + " " + old_file + '\t' + str(e) + '\n')
                                log.close()
                            success = True


# ##############################################################################################
ls_files = []


old_files_with_genders_male = []
new_files_with_genders_male = []
old_files_with_genders_female = []
new_files_with_genders_female = []

df_with_gender = pd.read_excel(f'descriptors/credentials/table/table_for_{national}_January_{dir_num}.xlsx', sheet_name='Sheet1')

if retry == False:
    for new_descriptor_assignment_id in tqdm(list_of_new_descriptors):
        assignment_id = new_descriptor_assignment_id
        try:
        # print(assignment_id)
            gender = df_with_gender[df_with_gender['assignment_id']==assignment_id]['gender'].values[0]
        except:
            gender = ''
        print(assignment_id, ' ', gender)
        if gender.lower() == 'male':
            new_files_with_genders_male.append(assignment_id)
        elif gender.lower() == 'female':
            new_files_with_genders_female.append(assignment_id)
        else:
            new_files_with_genders_male.append(assignment_id)
            new_files_with_genders_female.append(assignment_id)


    for old_file_assignment_id in df_with_descriptors.loc[(df_with_descriptors['nation']==nation_for_descriptor), 'assignment_id']:
        if not old_file_assignment_id in list_of_new_descriptors:
            try:
                old_file_gender = all_sets_in_db_df.loc[(all_sets_in_db_df['assignment_id']==old_file_assignment_id), 'gender'].values[0]
                print(old_file_assignment_id, ' ', old_file_gender)
                if old_file_gender.lower() == 'male':
                    old_files_with_genders_male.append(old_file_assignment_id)
                elif old_file_gender.lower() == 'female':
                    old_files_with_genders_female.append(old_file_assignment_id)
                else:
                    print(old_file_assignment_id, ' ', old_file_gender)
                    old_files_with_genders_male.append(old_file_assignment_id)
                    old_files_with_genders_female.append(old_file_assignment_id)
            except Exception as e:
                print(e)
                old_files_with_genders_male.append(old_file_assignment_id)
                old_files_with_genders_female.append(old_file_assignment_id)



    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for new_file in tqdm(list_of_new_descriptors):
            futures.append(executor.submit(descriptor_checK, new_file=new_file, old_files_with_genders_male=old_files_with_genders_male, new_files_with_genders_male=new_files_with_genders_male, old_files_with_genders_female=old_files_with_genders_female, new_files_with_genders_female=new_files_with_genders_female))
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

# ##############################################################################################

# MOVING ALL SETS TO DESTINATION BY PREFIX
copy_move_path = 'copy_move_path' + prefix
print(copy_move_path)

if not os.path.exists(copy_move_path):
    os.makedirs(copy_move_path)

original_move_path = 'photos/photos_from_drive/' + dir_num
shutil.move(original_move_path, copy_move_path)

for root, subdirectories, files in os.walk('photos/photos_from_drive'):
    for subdirectory in subdirectories:
        try:
            shutil.move(os.path.join(root, subdirectory), copy_move_path)
        except:
            pass

ai_dublicats = pd.read_csv(f'ai_dublicats.tsv', sep='\t')
if len(ai_dublicats['new_file_path']) > 0:
    print('Dublicats by descriptors: ', len(ai_dublicats['new_file_path']))
else:
    print('There are no dublicats by descriptors')

# WRITE SHORT RESULTS
if new_dublicats != False:
    print(new_dublicats)

if old_dublicats != False:
    print(old_dublicats)

if no_new_dublicats != False:
    print(no_new_dublicats)

if no_old_dublicats != False:
    print(no_old_dublicats)