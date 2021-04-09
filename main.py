"""
CTG Importtool POC
(c) 2021 ilionx

Versie: 0.1 (11-03-2021)
Auteur: R. Jonker, P. Groot

Script om ruwe CTG data uit MOSOS te importeren, verwerken en op te slaan in een Microsoft SQL Database.
Gebaseerd op de importfunctionaliteit uit de STVcalc applicatie van Hans Wolf (UMC Amsterdam)
"""
import os
import datetime
import pyodbc
import pandas as pd
import binaryctgfunctions
import sqlalchemy

from typing import List, Dict, Tuple
from numbers import Number
from sqlalchemy import create_engine

# Parameters
data_dirs = [
    '\\\\mosos-z03\\mosos$\\MososCom\\Signals',
    '\\\\mosos-z03\\mosos$\\MososCom\\Archived\\Mosos'
]
db_server = 'SQL2019HIX-H02,1533\\ILIONX.'
db_driver = 'SQL Server Native Client 11.0'
target_db_name = 'VOORVERWERKING_MOSOS'
source_db_name = 'STAGING_MOSOS'
table_name = 'MOSOS_CTG_TEST'
step_size = 100
incremental = True
dry_run = False


class CtgMeting:
    def __init__(self, FileName, FilePath, Extension, MeasureDate, Size, Twins=False, HeartRateChannel_1='',
                 HeartRateCountChannel_1=0, QualityChannel_1='', HeartRateChannel_2='', HeartRateCountChannel_2=0,
                 QualityChannel_2='', UterusPressure='', uterusPressureCount=0):
        self.FileName = FileName
        self.FilePath = FilePath
        self.Extension = Extension
        self.MeasureDate = MeasureDate
        self.Size = Size
        self.Twins = Twins
        self.HeartRateChannel_1 = HeartRateChannel_1
        self.HeartRateCountChannel_1 = HeartRateCountChannel_1
        self.QualityChannel_1 = QualityChannel_1
        self.HeartRateChannel_2 = HeartRateChannel_2
        self.HeartRateCountChannel_2 = HeartRateCountChannel_2
        self.QualityChannel_2 = QualityChannel_2
        self.UterusPressure = UterusPressure
        self.uterusPressureCount = uterusPressureCount

    def to_dict(self):
        return {
            'FileName': self.FileName,
            'FilePath': self.FilePath,
            'Extension': self.Extension,
            'MeasureDate': self.MeasureDate,
            'Size': self.Size,
            'Twins': self.Twins,
            'HeartRateChannel_1': self.HeartRateChannel_1,
            'HeartRateCountChannel_1': self.HeartRateCountChannel_1,
            'QualityChannel_1': self.QualityChannel_1,
            'HeartRateChannel_2': self.HeartRateChannel_2,
            'HeartRateCountChannel_2': self.HeartRateCountChannel_2,
            'QualityChannel_2': self.QualityChannel_2,
            'UterusPressure': self.UterusPressure,
            'uterusPressureCount': self.uterusPressureCount
        }


class Registration:
    def __init__(self, RegisKey, FileName, Twins):
        self.RegisKey = RegisKey
        self.FileName = FileName
        self.Twins = Twins


def find_registrations():
    regis_files = {}

    sql = 'select r.REGIS_KEY, r.FILENAME, case when f.FETUS_KEY is not null then CAST(1 as bit) else CAST(0 as bit) end as TWINS from STAGING_MOSOS.dbo.REGISTR r join STAGING_MOSOS.dbo.EVENT e on r.REGIS_KEY = e.OTHER_KEY and e.EVENT_KIND = 0 left join STAGING_MOSOS.dbo.FETUS f on e.PREG_KEY = f.PREG_KEY	and f.FETUS_NR = 2'

    connection = pyodbc.connect(trusted_Connection='yes', driver=db_driver, server=db_server, database=source_db_name)
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        regis_files[row.FILENAME] = row.TWINS
    return regis_files


def find_files(data_dir: str) -> Dict[str, str]:
    """
    Zoeken van .ctg/.dat/.txt/.prn bestanden in data_dir

    data_dir:   De directory waarin de bestanden worden gezocht
    return:     Een dictionary die een lijst met bestanden bevat
    """

    input_files = {}
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            ext = os.path.splitext(f)[1].lower()
            if ext == '.ctg':
                input_files[os.path.join(root, f)] = 'CTG'
            #elif ext == '.dat':
            #    input_files[os.path.join(root, f)] = 'DAT'
            #elif ext == '.txt' or ext == '.prn':
            #    input_files[os.path.join(root, f)] = 'TXT'

    return input_files


def read_data(file_paths: Dict[str, str], ctgregistrations):
    """
    Lezen van bestanden gespecificeerd in file_paths met als resultaat een lijst met
    CtgMeting objecten.

    file_paths:     De bestanden en hun bestandsformaat
    return:         CtgMeting objecten
    """

    ctg_files = []

    for filePath, ftype in file_paths.items():
        print('Reading ' + ftype + ' file ' + filePath)
        fileName = os.path.splitext(os.path.basename(filePath))[0]
        extension = os.path.splitext(filePath)[1].lower()
        modDate = datetime.datetime.fromtimestamp(os.path.getmtime(filePath))
        fileSize = os.path.getsize(filePath)
        twins = ctgregistrations[fileName]

        heartRateChannel_1 = ''
        heartRateCountChannel_1 = 0
        qualityChannel_1 = ''
        heartRateChannel_2 = ''
        heartRateCountChannel_2 = 0
        qualityChannel_2 = ''
        maternalheartRate = ''
        maternalheartRateCount = 0
        maternalQuality = ''
        uterusPressure = ''
        uterusPressureCount = 0

        if ftype == 'CTG':
            heartRateChannel_1, heartRateCountChannel_1, qualityChannel_1, heartRateChannel_2, heartRateCountChannel_2,\
            qualityChannel_2, maternalheartRate, maternalheartRateCount, maternalQuality,\
            uterusPressure, uterusPressureCount = binaryctgfunctions.ReadCTG(filePath)
        elif ftype == 'DAT':
            heartRateChannel_1, heartRateCountChannel_1, qualityChannel_1, heartRateChannel_2, heartRateCountChannel_2,\
            qualityChannel_2, uterusPressure, uterusPressureCount = binaryctgfunctions.ReadDAT(filePath)

        ctgMeting = CtgMeting(fileName, filePath, extension, modDate, fileSize, twins, heartRateChannel_1,
                              heartRateCountChannel_1, qualityChannel_1, heartRateChannel_2, heartRateCountChannel_2,
                              qualityChannel_2, uterusPressure, uterusPressureCount)
        ctg_files.append(ctgMeting)

    return ctg_files


def make_dataframe(ctg_data):
    """
    Pandas DataFrame opbouwen uit de verwerkte CTG metingen

    :param ctg_metingen:   Lijst met CtgMeting objecten
    :return:         Pandas DataFrame met CtgMetingen
    """
    df = pd.DataFrame.from_records([c.to_dict() for c in ctg_data])
    df['DateAdded'] = datetime.datetime.now()
    return df


def write_to_db(ctg_data, sql_engine):
    """
    Opslaan van de CTG metingen in de database.

    :param ctg_data:
    :param sql_engine:
    :return:
    """
    ctg_data.to_sql(table_name, con=sql_engine, index=False, if_exists='append', schema='dbo')


def get_existing_files(sql_engine) -> List[str]:
    """
    Get a list of existing (stripped) file names from the database

    sql_engine: An SQLAlchemy handle to the database connection
    return:     A list of file names (without extension)
    """
    query = 'SELECT DISTINCT [FILENAME] FROM [' + table_name + '] ORDER BY [FILENAME]'
    try:
        return pd.read_sql(query, con=sql_engine)['FILENAME'].to_list()
    except sqlalchemy.exc.OperationalError as e:
        return []


# Alle CTG-registraties opvragen uit MOSOS
print('Verzamelen CTG-registraties in MOSOS...')
ctgRegistrations = find_registrations()
print('Aantal CTG-registraties: ', len(ctgRegistrations))

# Alle leesbare CTG-bestanden zoeken
print('Zoeken CTG-bestanden...')
files = {}
for data_dir in data_dirs:
    print('Zoeken in: ' + data_dir)
    files = {**files, **find_files(data_dir)}

# Database connectie opbouwen
conn_str = ('mssql+pyodbc:///?odbc_connect=Driver={' + db_driver + '}; Server='
            + db_server + '; Database=' + target_db_name + '; Trusted_Connection=yes;'
            )
sql_engine = create_engine(conn_str)

# Bij incrementele load alle bestanden die al in de database staan uitsluiten
if incremental:
    print('Lijst opbouwen van bestanden die reeds zijn geimporteerd')
    existing_files = get_existing_files(sql_engine)
    files = {fpath: ftype for fpath, ftype in files.items() if
             os.path.splitext(os.path.basename(fpath))[0] not in existing_files}

if dry_run:
    print('Dit is een DRY RUN. Er wordt geen data opgeslagen. De lijst met te verwerken bestanden is als volgt:')
    for f in files:
        print(f)
    exit(0)

# Inlezen CTG bestanden en opslaan in de database
for i in range(0, len(files), step_size):
    step_files = dict(list(files.items())[i:i + step_size])
    step_ctg_data = read_data(step_files, ctgRegistrations)
    step_df = make_dataframe(step_ctg_data)
    write_to_db(step_df, sql_engine)
    print('Er zijn ', len(step_files), ' bestanden opgeslagen in ', table_name)

print('Importeren en opslaan MOSOS CTG bestanden gereed')
