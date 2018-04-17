#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2016-2018 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# source XML files is plased at
# http://data.gov.ua/passport/73cfe78e-89ef-4f06-b3ab-eb5f16aea237

# TODO:
# stan dictionary according to SFS ?
# records as NamedTuples


import argparse
import logging
import os.path
import re
import requests
import sqlite3
import sys
import tempfile
import time
import uuid
import zipfile
from datetime import datetime
from lxml import etree
from os import curdir, remove
from pathlib import Path

error_list = []

__version__ = '0.5'
DATASET_XML_INFO = 'http://data.gov.ua/view-dataset/dataset-file/218357'
DATA_FILES = {
    'uo': '15.1-EX_XML_EDR_UO.xml',
    'fop': '15.2-EX_XML_EDR_FOP.xml'
    }


class Error(Exception):
    pass


class WrongCommitIntervalError(Error):
    def __init__(self, commit_interval):
        if commit_interval < 2000 and commit_interval > 0:
            msg = "занадто малий"
        else:
            msg = "від'ємний або нульовий"
        sys.stderr.write(
            f'Вказаний інтервал запису змін в базу даних {msg}, що '
            'недопустимо.\nВстановлено значення за замовчуванням, запис буде '
            'здійснюватись через кожні 2 тисячі оброблених елементів.\n'
            )


class DownloadXMLFileError(Error):
    def __init__(self, type_=None):
        s = ''
        if type_:
            s = 'файлу опису '
        sys.stderr.write(
            'Помилка завантаження ' + s + 'набору даних.\nПродовження '
            'роботи неможливо\n'
            )


class DateTimeString(str):
    '''Converts  '21.03.2018 17:19'
    to '2018-03-21T17:19'
    with method to8601
    '''
    def __init__(self, string):
        self.dtstring = string
        dt_xml = '%d.%m.%Y %H:%M'
        dt_8601 = '%Y-%m-%dT%H:%M'

    def to8601(self):
        datetime_value = datetime.strptime(self.dtstring, '%d.%m.%Y %H:%M')
        return datetime_value.strftime('%Y-%m-%dT%H:%M')


def process_element(elem, is_fop):
    founders = []
    if not is_fop:
        # 8 - із засновниками
        # 7 - без засновників
        p = elem[-1].tag
        if p == 'FOUNDERS':
            l = [elem[n].text.strip() if elem[n].text is not None else
                 elem[n].text for n in range(7)]
            founders = [f.text.strip() if f.text is not None else f.text
                        for f in elem[7]]
        else:
            l = [elem[n].text.strip() if elem[n].text is not None else
                 elem[n].text for n in range(7)]
    else:
        l = [elem[n].text.strip() if elem[n].text is not None else
             elem[n].text for n in range(4)]
    l.append(founders)
    return l


def process_edrpou(xml_files, use_curdir):
    total_records = 0
    for x in xml_files:
        try:
            process_fop = False if x == DATA_FILES['uo'] else True
            working_directory = tempfile.gettempdir() if not use_curdir \
                else curdir
            context = etree.iterparse(
                os.path.join(working_directory, x), events=('end',),
                tag='RECORD'
                )
            file_records = fast_iter(
                context, process_element, db=db, is_fop=process_fop,
                commit_after=args.commit
                )
            db.commit()
            total_records += file_records
        except:
            raise
        else:
            remove(os.path.join(working_directory, x))
    # indexing
    print('\nІндексація...\n', end='', flush=True)
    db.execute('CREATE INDEX IF NOT EXISTS `address` ON `edr` (`address` '
               'ASC);')
    db.execute('CREATE INDEX IF NOT EXISTS `tin` ON `edr` (`tin` ASC);')
    db.execute('CREATE INDEX IF NOT EXISTS `fname` ON `edr` (`full_name` '
               'ASC);')
    db.execute('CREATE INDEX IF NOT EXISTS `uuid` ON `founders` '
               '(`uuid` ASC);')
    db.execute('CREATE INDEX IF NOT EXISTS `founder` ON `founders`'
               '(`founder` ASC);')
    sys.stdout.write('\nЗавершено.\n')
    return total_records


def guess_sex(name):
    if name[-4:].strip().upper() in ["IВНА", "ІВНА", "ЇВНА", "ОВНА", "КИЗИ"]:
        return "Ж"
    elif name[-2:].strip().upper() in ["ИЧ", "IЧ", "ІЧ"] or \
            name[-4:].strip().upper() in ["ОГЛИ", "ОГЛЫ"]:
        return "Ч"
    else:
        return


def guess_active(active):
    return 1 if active == "зареєстровано" else 0


def insert(*args, **kwargs):
    c = db.cursor()
    try:
        founders = args[0][-1]
        uid = uuid.uuid4().hex
        face = 1 if args[1] else 0
        qry_list = [uid, face]
        qry_list.extend(args[0][:-1])
        if face == 0:
            sex = guess_sex(args[0][4]) if args[0][4] else None
            kved = args[0][5].split(' ', 1)[0] if args[0][5] else None
        else:
            sex = guess_sex(args[0][0]) if args[0][0] else None
            kved = args[0][2].split(' ', 1)[0] if args[0][2] else None
        active = guess_active(args[0][-2]) if args[0][-2] else None

        qry_list.append(sex)  # + sex
        qry_list.append(kved)
        qry_list.append(active)

        if face == 0:
            c.execute('''insert into edr values (?,?,?,?,?,?,?,?,?,?,?,?);''',
                      qry_list)
            if founders:
                for f in founders:
                    c.execute('''insert into founders values (?,?);''',
                              [uid, f])
        else:
            c.execute('''insert into edr (uuid, facemode, name, address,
                         kved_desc, stan, sex, kved, active) values
                         (?,?,?,?,?,?,?,?,?);''', qry_list)
    except (sqlite3.ProgrammingError, sqlite3.OperationalError):
        print('>>> ARGS:', args, '\n>>> KWARGS:', kwargs,
              '>>> QRY_LIST', qry_list)
        raise


def fast_iter(context, func, db, is_fop=None, commit_after=None):
    counter = 0
    # fast_iter is useful if you need to free memory while iterating
    # through a very large XML file.
    #
    # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    # Author: Liza Daly
    try:
        for event, elem in context:
            has_founders = 'FOUNDERS' in [e.tag for e in elem]
            insert(func(elem, is_fop), is_fop, has_founders=has_founders)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            counter += 1
            if counter % 10000 == 0:
                print('.', end='', flush=True)
            if counter % commit_after == 0:
                db.commit()
        del context
    except:
        raise
    else:
        return counter


def show_time(records_processed, exec_time):
    exec_time = int(round(exec_time, 0))
    hours = minutes = seconds = 0
    hours, remain = exec_time//3600, exec_time % 3600
    if remain < 60:
        seconds = remain
    else:
        minutes, seconds = remain//60, remain % 60
    print(f'Оброблено {records_processed} елементів за {hours:02d} год., '
          f'{minutes:02d} хв. і {seconds:02d} сек.\n')


def main(args):
    records_processed = 0
    if args.fop:
        xml_files = [DATA_FILES['uo'], DATA_FILES['fop']]
    else:
        xml_files = [DATA_FILES['uo']]
    start_time = time.time()
    records_processed += process_edrpou(xml_files, args.curdir)
    db.commit()
    end_time = time.time()
    return records_processed, end_time - start_time


def get_dataset_info():
    tempdir = tempfile.gettempdir()
    try:
        res = requests.get(DATASET_XML_INFO)
        if res.status_code != 200:
            raise DownloadXMLFileError(type_=1)
    except DownloadXMLFileError:
        sys.exit(1)
    else:
        temp_xml = tempfile.NamedTemporaryFile()
        with open(temp_xml.name, 'wb') as f:
            for chunk in res.iter_content(1024):
                f.write(chunk)
        with open(temp_xml.name, 'rb') as f:
            g = f.read()

        result = etree.fromstring(g)
        fileinfo = {
            'title': result.xpath('./title/text()')[0],
            'created': DateTimeString(
                           result.xpath('./created/text()')[0]
                           ).to8601(),
            'filemime': result.xpath('./filemime/text()')[0],
            'format': result.xpath('./format/text()')[0],
            'filesize': int(result.xpath('./filesize/text()')[0])
        }
        url = result.xpath('./url/text()')[0]
        return url, fileinfo


def create_database():
    # c.execute('PRAGMA synchronous = 0')
    # c.execute('PRAGMA journal_mode = OFF')
    c.execute('create table if not exists edr ('
              'uuid text PRIMARY KEY,'
              'facemode integer,'
              'full_name text,'
              'name text,'
              'tin integer,'
              'address text,'
              'boss text,'
              'kved_desc text,'
              'stan text,'
              'sex text, '
              'kved text, '
              'active integer);')

    c.execute('''create table if not exists founders (
        uuid text,
        founder text);''')

    c.execute('''create table if not exists fileinfo (
        title text,
        created text,
        filemime text,
        format text,
        filesize integer);''')


def extract_XML(archive_name, extract_fop=None, use_curdir=None):
    if not zipfile.is_zipfile(archive_name):
        print('Файл не є файлом ZIP-архіву, зась!')
        sys.exit(1)
    with zipfile.ZipFile(archive_name) as zf:
        # debug level:
        # from 0 - no output to 3 - the most output
        zip_content = zf.namelist()
        zf.debug = 3
        unpack_dir = tempfile.gettempdir() if not use_curdir else curdir
        try:
            print('Видобуваю... ', end='', flush=True)
            if extract_fop:
                zf.extractall(path=unpack_dir)
            else:
                zf.extract(DATA_FILES['uo'], path=unpack_dir)
            print('OK :)\n')
            return zip_content
        except zipfile.BadZipFile:
            print('Поганий ZIP-архів, виходжу...\n')
            sys.exit(1)
        except OSError as e:
            if e.errno == 28:
                print('Недостатньо місця для роіізпакування файлів')
                sys.exit(1)


def download_file(url, **kwargs):
    try:
        print('Завантажую, це може тривати певний час... ',
              end='', flush=True)
        res = requests.get(url)
        if res.status_code != 200:
            raise DownloadXMLFileError()
    except DownloadXMLFileError:
        print('Not OK :(\n')
        sys.exit(1)
    else:
        dataset_zip = tempfile.NamedTemporaryFile()
        with open(dataset_zip.name, 'wb') as f:
            for chunk in res.iter_content(8388608):
                f.write(chunk)
        print('OK :)\n')
        # unzip here
        names = extract_XML(
            dataset_zip.name, extract_fop=kwargs['extract_fop'],
            use_curdir=kwargs['use_curdir']
            )
        return names


def fill_fileinfo(fileinfo):
    try:
        c.execute(
            "INSERT INTO fileinfo VALUES (:title, :created, :filemime,"
            ":format, :filesize);", fileinfo
            )
    except:
        raise


if __name__ == "__main__":
    logging.basicConfig(filename="parse_edr.log", level=logging.INFO)
    try:
        sys.stdout.write(f'Обробка ЄДРПОУ, версія {__version__}\n')
    except UnicodeEncodeError:
        logging.warning('Wrong terminal encoding, show English version')
        sys.stdout.write(f'EDROU prosessor utility, v. \n{__version__}')
    parser = argparse.ArgumentParser(description='Process some XML.')
    parser.add_argument(
        '-f', '--fop', help='обробляти дані фізичних осіб', action='store_true'
        )
    parser.add_argument(
        '-c', '--commit', type=int, default=2000, help='Кількість оброблених '
        'елементів, після якої виконуватиметься операція запису до БД (2000 '
        'за замовчуванням)')
    parser.add_argument(
        '--curdir', action='store_true', help='Використати поточну директорію '
        'для розпаковки файлів замість системної тимчасової')

    try:
        args = parser.parse_args()
        # print(args)
        if args.commit < 2000:
            raise WrongCommitIntervalError(args.commit)
    except WrongCommitIntervalError:
        args.commit = 2000
    try:
        url, fileinfo = get_dataset_info()
        if not (url and fileinfo):
            raise DownloadXMLFileError(type_=1)
    except DownloadXMLFileError:
        exit()
    else:
        xml_files = download_file(
            url, extract_fop=args.fop, use_curdir=args.curdir
            )
        # xml_files = ['15.2-EX_XML_EDR_FOP.xml','15.1-EX_XML_EDR_UO.xml',]
        db = sqlite3.connect(
            f'edr_3_{fileinfo["created"].split("T")[0]}.sqlite'
            )
        c = db.cursor()
        create_database()
        fill_fileinfo(fileinfo)

    try:
        records_processed, exec_time = main(args)
    except KeyboardInterrupt:
        print('\nПерервано користувачем, все одно запишемо зміни...')
        db.commit()
    else:
        show_time(records_processed, exec_time)
