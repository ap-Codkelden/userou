#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016-2018 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# dataset URL is
# https://data.gov.ua/dataset/1c7f3815-3259-45e0-bdf1-64dca07ddc10

# TODO:
# stan dictionary according to SFS ?
# records as NamedTuples
# make guess_sex optional

import argparse
import hashlib
import json
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
from contextlib import suppress
from datetime import datetime
from lxml import etree
from os import getcwd, remove
from pathlib import Path


__version__ = '0.8'

# constants
BUF_SIZE = 1048576
DATASET_ID = '1c7f3815-3259-45e0-bdf1-64dca07ddc10'
# chunk size for writing ZIP file
WRITE_ZIP_CHUNK = 8388608  # 8Mb


class Error(Exception):
    pass


class WrongSHA1ChecksumError(Error):
    '''Клас помилки порівняння конрольних сум завантаженого ZIP-файлу
    '''
    def __init__(self):
        sys.stderr.write(
            'неправильна контрольна SHA1 сума файлу, він пошкоджений.\n'
            'Продовження роботи неможливе.\n'
            )


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
        s = 'файлу опису ' if type_ else ''
        sys.stderr.write(
            f'Помилка завантаження {s}набору даних.\nПродовження '
            'роботи неможливе.\n'
            )


class DownloadMetainfoError(Error):
    def __init__(self, error_msg, type_):
        res_type = 'мераінформації' if type_ == 'meta' else \
            'інформації про ресурс'
        sys.stderr.write(
            f'Замість {res_type} отримано наступне повідомлення про '
            f'помилку:\n{error_msg}\n'
            )


class WrongFilesCountError(Error):
    '''Клас помилки неправильної кількості файлів у ZIP-архіві. Має
    бути 2: один для юридичних, інший для фізичних.
    '''
    def __init__(self, files_count):
        self.files_count = files_count
        sys.stderr.write(
            f'В архіві не два файли, а {self.files_count}.\n'
            'Можливо варто перевірити його вміст і повідомити розробників.\n'
            )


class UnknownError(Error):
    def __init__(self):
        sys.stderr.write(
            f'Невідома помилка.\nПо можливості повідомте '
            'про неї розробникам.\n'
            )
        sys.exit(1)


class DateTimeString(str):
    '''Converts  "21.03.2018 17:19" to "2018-03-21T17:19" with method to8601
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


def process_edrpou(xml_files, process_fop, curdir):
    total_records = 0
    for x in xml_files:
        try:
            working_directory = tempfile.gettempdir() if not curdir \
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
    print('\nІндексація…\n', end='', flush=True)
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
        print(
            '>>> Error stack:\n',
            {'args': args, 'kwargs': kwargs,
             'qry_list': qry_list})
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


def main(args, xml_files):
    records_processed = 0
    if args.fop:
        xml_file_list = xml_files.values()
    else:
        xml_file_list = [xml_files['u']]
    start_time = time.time()
    records_processed += process_edrpou(xml_file_list, args.fop, args.curdir)
    db.commit()
    end_time = time.time()
    return records_processed, end_time - start_time


def get_dataset_metainfo():
    tempdir = tempfile.gettempdir()
    try:
        metainfo = requests.get(
            'https://data.gov.ua/api/3/action/package_show',
            params={'id': DATASET_ID}
            )
        metainfo_json = metainfo.json()
        if not metainfo_json['success']:
            raise DownloadMetainfoError(response_json['error']['message'],
                                        type_='meta')
        last_resourse_id = metainfo_json['result']['resources'][-1]['id']
        resourse_data = requests.get(
            'https://data.gov.ua/api/3/action/resource_show',
            params={'id': last_resourse_id})
        res_info = resourse_data.json()
        if not res_info['success']:
            raise DownloadMetainfoError(response_json['error']['message'],
                                        type_='resourse')
        res = res_info['result']
        resourse_url = res['url']
        fileinfo = {
            'name': res['name'],
            'created': res['archiver']['updated'].split("T")[0],
            'filemime': res['mimetype'],
            'format': res['format'],
            'filesize': res['size'],
            'sha1sum': res['archiver']['hash']
        }
    except DownloadMetainfoError:
        sys.exit(1)
    except:
        raise  # UnknownError
    else:
        sys.stdout.write('Метаінформацію успішно отримано.\n')
        return resourse_url, fileinfo


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
        name text,
        created text,
        filemime text,
        format text,
        filesize integer,
        sha1sum text);''')


def extract_XML(archive_name, extract_fop=None, use_curdir=None):
    if not zipfile.is_zipfile(archive_name):
        print('Файл не є файлом ZIP-архіву, зась!')
        sys.exit(1)
    with zipfile.ZipFile(archive_name) as zf:
        # debug level:
        # from 0 - no output to 3 - the most output
        try:
            zip_content = zf.namelist()
            zip_count = len(zip_content)
            if zip_count != 2:
                raise WrongFilesCountError(zip_count)
        except WrongFilesCountError:
            pass
        DATA_FILES = dict(
            [z for z in map(
                lambda xml_filename: ('u' if 'UO' in xml_filename else 'f',
                                      xml_filename), zip_content)])
        zf.debug = 0
        unpack_dir = tempfile.gettempdir() if not use_curdir else getcwd()
        try:
            print('Видобуваю… ', end='', flush=True)
            if extract_fop:
                zf.extractall(path=unpack_dir)
            else:
                zf.extract(DATA_FILES['u'], path=unpack_dir)
            print('OK\n')
            return DATA_FILES
        except zipfile.BadZipFile:
            print('Поганий ZIP-архів, виходжу…\n')
            sys.exit(1)
        except OSError as e:
            if e.errno == 28:
                print('Недостатньо місця для розпаковування файлів')
                sys.exit(1)


def checksum(zipfile_name):
    sha1 = hashlib.sha1()
    with open(zipfile_name, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def download_file(url, **kwargs):
    try:
        print('Завантажую, це може тривати певний час… ',
              end='', flush=True)
        res = requests.get(url)
        if res.status_code != 200:
            raise DownloadXMLFileError()
        print('OK')
    except DownloadXMLFileError:
        print('Not OK :(\n')
        sys.exit(1)
    else:
        dataset_zip = tempfile.NamedTemporaryFile()
        with open(dataset_zip.name, 'wb') as f:
            for chunk in res.iter_content(WRITE_ZIP_CHUNK):
                f.write(chunk)
    if kwargs['checksha1']:
        try:
            # check SHA1
            downloaded_sha1 = checksum(dataset_zip.name)
            if not downloaded_sha1 == kwargs['sha1sum']:
                raise WrongSHA1ChecksumError()
            else:
                sys.stdout.write(
                    'Контрольна сума SHA1 співпадає, продовжуємо…\n'
                    )
        except WrongSHA1ChecksumError:
            sys.exit(1)
    # unzip here
    names = extract_XML(
            dataset_zip.name, extract_fop=kwargs['extract_fop'],
            use_curdir=kwargs['use_curdir']
            )
    return names


def fill_fileinfo(fileinfo):
    try:
        c.execute(
            "INSERT INTO fileinfo VALUES (:name, :created, :filemime,"
            ":format, :filesize, :sha1sum);", fileinfo
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
    parser = argparse.ArgumentParser(
        description='Обробка XML-файлів з даними Єдиного державного реєстру '
        'юридичних осіб, фізичних осіб-підприємців та громадських формувань.')
    parser.add_argument(
        '-f', '--fop', help='обробляти також дані фізичних осіб',
        action='store_true')
    parser.add_argument(
        '-c', '--commit', type=int, default=2000, help='Кількість оброблених '
        'елементів, після якої виконуватиметься операція запису до БД (2000 '
        'за замовчуванням)')
    parser.add_argument(
        '--curdir', action='store_true', help='Використати поточну директорію'
        ' для видобування файлів із ZIP-архіву замість системної тимчасової')
    parser.add_argument(
        '--checksha1', action='store_true', help='Перевіряти контрольну суму '
        'завантаженого ZIP-архіву (за замовчуванням -- ні)')

    try:
        args = parser.parse_args()
        if args.commit < 2000:
            raise WrongCommitIntervalError(args.commit)
    except WrongCommitIntervalError:
        args.commit = 2000
    try:
        url, fileinfo = get_dataset_metainfo()
        if not (url and fileinfo):
            raise DownloadXMLFileError(type_=1)
    except DownloadXMLFileError:
        exit()
    else:
        xml_files = download_file(
            url, sha1sum=fileinfo['sha1sum'], extract_fop=args.fop,
            use_curdir=args.curdir, checksha1=args.checksha1
            )

        db = sqlite3.connect(
            f'edr_3_{fileinfo["created"]}.sqlite'
            )
        c = db.cursor()
        create_database()
        fill_fileinfo(fileinfo)

    try:
        records_processed, exec_time = main(args, xml_files)
    except KeyboardInterrupt:
        print('\nПерервано користувачем, все одно запишемо зміни…')
        db.commit()
        working_directory = tempfile.gettempdir() if not args.curdir \
            else args.curdir
        # приберемося після себе
        with suppress(FileNotFoundError):
            for filename in xml_files:
                os.remove(os.path.join(working_directory, filename))
    else:
        show_time(records_processed, exec_time)
