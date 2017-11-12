#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2016-2017 Renat Nasridinov
# This software may be freely distributed under the MIT license.
# https://opensource.org/licenses/MIT The MIT License (MIT)
# source XML files are plased at
# http://data.gov.ua/passport/73cfe78e-89ef-4f06-b3ab-eb5f16aea237

# TODO:
# stan dictionary according to SFS

import argparse
import json
import logging
import os.path
import re
import sqlite3
import sys
# import time
import uuid
from lxml import etree
from pathlib import Path

error_list = []

__version__ = '0.3'


def process_element(elem, type_):
    founders = []
    if type_ == 'u':
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
        # 0 ПІБ
        # 1 Місце_проживання
        # 2 Основний_вид_діяльності
        # 3 Стан
        l = [elem[n].text.strip() if elem[n].text is not None else
                 elem[n].text for n in range(4)]
    l.append(founders)
    return l


def process_edrpou(args):
    try:
        sys.stdout.write('Processing file {}\n'.format(args.xml))
        context = etree.iterparse(args.xml, events=('end',), tag='RECORD')
        fast_iter(context, process_element, db=db, type_=args.type)
        db.commit()
        # indexing
        print('\nIndexing...\n', end='', flush=True)
        db.execute('CREATE INDEX IF NOT EXISTS `address` ON `edr` (`address` '
                   'ASC);')
        db.execute('CREATE INDEX IF NOT EXISTS `tin` ON `edr` (`tin` ASC);')
        db.execute('CREATE INDEX IF NOT EXISTS `fname` ON `edr` (`full_name` '
                   'ASC);')
        db.execute('CREATE INDEX IF NOT EXISTS `uuid` ON `founders` '
                   '(`uuid` ASC);')
        db.execute('CREATE INDEX IF NOT EXISTS `founder` ON `founders`'
                   '(`founder` ASC);')
        sys.stdout.write('\nCompleted.\n')
    except KeyboardInterrupt:
        print('\nПерервано користувачем.')


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
    # u
    # ['ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ "ГОРБУДІНДУСТРІЯ"',
    # 'ТОВ "ГОРБУДІНДУСТРІЯ"', '34567528', '19501, Черкаська обл.,
    # Городищенський район, місто Городище, ВУЛИЦЯ ІНДУСТРІАЛЬНА, будинок 18',
    # "СТОРОЖУК В'ЯЧЕСЛАВ ПЕТРОВИЧ",
    # '14.11.0 ДОБУВАННЯ ДЕКОРАТИВНОГО ТА БУДІВЕЛЬНОГО КАМЕНЮ',
    # 'в стані припинення'],
    # facemode integer, name text, shortname text, tin integer, address text,
    # boss text, kved text, stan text
    c = db.cursor()
    try:
        founders = args[0][-1]
        uid = uuid.uuid4().hex
        face = 0 if args[1] == 'u' else 1
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
            # 0 ПІБ
            # 1 Місце_проживання
            # 2 Основний_вид_діяльності
            # 3 Стан
            ''''
            uuid text
            facemode
            fullname
            name
            tin
            address
            boss
            kved
            stan
            loc1
            loc2
            loc3
            loc4
            addr_code
            '''
            # print(qry_list)
            # time.sleep(1)
            c.execute('''insert into edr (uuid, facemode, name, address,
                         kved_desc, stan, sex, kved, active) values
                         (?,?,?,?,?,?,?,?,?);''', qry_list)
    except sqlite3.ProgrammingError:
        print(args)
        raise
    except sqlite3.OperationalError:
        print(qry_list)
        raise


def fast_iter(context, func, db, type_):
    counter = 0
    # fast_iter is useful if you need to free memory while iterating
    # through a very large XML file.
    #
    # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
    # Author: Liza Daly
    try:
        for event, elem in context:
            has_founders = 'FOUNDERS' in [e.tag for e in elem]
            insert(func(elem, type_), type_, has_founders=has_founders)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            counter += 1
            if counter % 10000 == 0:
                print('.', end='', flush=True)
            if counter % 2000 == 0:
                db.commit()
        del context
    except:
        raise


def main():
    logging.basicConfig(filename="parse_edr.log", level=logging.INFO)
    try:
        sys.stdout.write('Обробка ЄДРПОУ, версія {}\n'.format(__version__))
    except UnicodeEncodeError:
        logging.warning('Wrong terminal encoding, show English version')
        sys.stdout.write('EDROU prosessor utility, '
                         'v. \n{}'.format(__version__))
    parser = argparse.ArgumentParser(description='Process some XML.')
    parser.add_argument('xml', type=str, help='XML file name')
    parser.add_argument(
        '-t', '--type', type=str, help='тип: u - юридичні, '
        'f - фізичні'
        )
    args = parser.parse_args()
    # print(args)
    # sys.exit()

    p = Path(args.xml)
    if not p.is_file():
        sys.stdout.write('Файл {} не знайдений\n'.format(args.xml))
        sys.exit()
    if args.type not in ['f', 'u']:
        parser.error('Зазначено невірний тип осіб.\n'
                     'Використовуйте -u або -t\n')
        sys.exit()
    process_edrpou(args)
    db.commit()


if __name__ == "__main__":
    db = sqlite3.connect('edr_3_test.sqlite')
    c = db.cursor()
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

    c.execute('''create table if not exists founders (uuid text,
                 founder text);''')
    main()
