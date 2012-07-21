#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import csv
import json
import httplib
import datetime
from time import localtime

raw_chip_dir = os.path.join('RegistrationAndOfficialTiming', 'raw_chip')
bib_to_chip_file = os.path.join(raw_chip_dir,
                                'Bib.to.Chip.ID.Data.OSCON_BIB_CHIPID.TXT')
participant_to_bib_file = os.path.join(raw_chip_dir,
                                       'Participant.to.Bib.no.participant.txt')
start_data_file = os.path.join(raw_chip_dir,
                          'Raw.Start.Chip.Data.TAGDATA1_OSCON.TXT')
finish_data_file = os.path.join(raw_chip_dir,
                           'Raw.Finish.Chip.Data.TAGDATA_OSCON.TXT')

bib_to_chip = csv.DictReader(open(bib_to_chip_file, 'rb'))
participant_to_bib = list(csv.reader(open(participant_to_bib_file, 'rb')))
start_data = list(csv.reader(open(start_data_file, 'rb')))
finish_data = list(csv.reader(open(finish_data_file, 'rb')))

def parse_chip_data(info):
    tag_id = info[0][4:16]
    date_str = info[0][20:26]
    date = datetime.date(int('20' + date_str[0:2]), int(date_str[2:4]),
                int(date_str[4:6]))
    time = localtime(int(info[0][26:33]) / 10)
    dt = datetime.datetime(date.year, date.month, date.day, time.tm_hour,
                           time.tm_min, time.tm_sec).isoformat()
    return tag_id, dt

# convert tagdata to a dictionary
tags = {}
for info in start_data:
    if len(str(info)) > 36:
        tag_id, dt = parse_chip_data(info)
        tags[tag_id] = {}
        tags[tag_id]['start'] = dt

for info in finish_data:
    if len(str(info)) > 36:
        tag_id, dt = parse_chip_data(info)
        if not tag_id in tags:
            tags[tag_id] = {}
        tags[tag_id]['finish'] = dt

# CouchDB _bulk_docs ready output:
# http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
runners = {'docs': []}
for row in bib_to_chip:
    # only going to add a runner we have info about
    if int(row['Num']) <= len(participant_to_bib):
        participant = participant_to_bib[int(row['Num'])-1]
        runner = {'_id': row['Num'],
                  'type': 'runner',
                  'chip_id': row['Tag'],
                  'first_name': participant[1],
                  'last_name': participant[2],
                  'gender': participant[3],
                  'age': participant[4]}
        for key in ['start', 'finish']:
            if row['Tag'] in tags and key in tags[row['Tag']]:
                runner[key] = tags[row['Tag']][key]
        runners['docs'].append(runner)

h = httplib.HTTPConnection('localhost', 5984);
h.request('POST', '/c2q5k/_bulk_docs', json.dumps(runners),
          {'Content-Type': 'application/json'})
res = h.getresponse()
print res.status, res.reason
