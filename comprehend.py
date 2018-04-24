#!python

from __future__ import print_function
import argparse
import collections
import boto3
import json
import csv
import hashlib
import os


ENTITY_TYPES = (
    'PERSON | LOCATION | ORGANIZATION | COMMERCIAL_ITEM | EVENT | DATE | QUANTITY | TITLE | ' +
    'OTHER').split(' | ')


class Question(object):
    def __init__(self, linenum, row):
        self.linenum = linenum
        self.id, \
            self.queue_name, \
            self.ticket_source, \
            self.subject, \
            self.text = row


def toBatches(seq, size):
    batch = []
    for item in seq:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def readQuestions(questionsFile, skipHeader):
    with open(questionsFile, 'rU') as fp:
        reader = csv.reader(fp)
        linenum = 0
        try:
            for linenum, row in enumerate(reader):
                if linenum == 0 and skipHeader:
                    continue
                yield Question(linenum, row)
        except csv.Error:
            print("Error reading csv file on row", linenum + 1)
            raise


def toEntities(questions, api, errors):
    for batch in toBatches(questions, 25):
        result = api(batch)
        errors.extend(result['ErrorList'])
        for row, item in zip(batch, result['ResultList']):
            yield row, item['Entities']


def toCsvOutput(entities):
    yield ['Question Id'] + ENTITY_TYPES
    for question, entities in entities:
        coll = collections.defaultdict(list)
        for entity in entities:
            coll[entity['Type']].append(entity['Text'])
        if len(coll) == 0:
            continue
        yield [question.id] + [' + '.join(coll.get(e, [])) for e in ENTITY_TYPES]


class BotoApi(object):
    def __init__(self):
        self.client = boto3.client('comprehend')
        self.count = 0

    def __call__(self, questions):
        self.count += 1
        result = self.client.batch_detect_entities(
            TextList=[text for _id, text in questions], LanguageCode='en')
        if self.count % 100 == 0:
            self.summary()
        return result

    def summary(self):
        print('Queried {:,} batches'.format(self.count))


class CachedApi(object):
    def __init__(self, dir_, loader):
        self.dir = dir_
        self.loader = loader
        self.count = 0

    def __call__(self, questions):
        text = ' '.join(question.text for question in questions)
        cacheFile = os.path.join(self.dir, hashlib.sha256(text).hexdigest())
        created = False
        if not os.path.exists(cacheFile):
            created = True
            print('Creating cache', cacheFile, 'for questions')
            result = self.loader(questions)
            with open(cacheFile, 'w') as fp:
                json.dump(result, fp)
        with open(cacheFile) as fp:
            if not created:
                print('Using cache', cacheFile, 'for questions')
                self.count += 1
                if self.count % 100 == 0:
                    self.printCount()
            return json.load(fp)

    def printCount(self):
        print('Used {:,} cache files'.format(self.count))

    def summary(self):
        self.loader.summary()
        self.printCount()


def setupArgs():
    args = argparse.ArgumentParser()
    args.add_argument(
        'questionsFile', metavar='QUESTIONS', help='File containing questions, one per line')
    args.add_argument(
        'outFile', metavar='ENTITIES', help='File to write output (will be overwritten)')
    args.add_argument('-cache', help='Use a cache', default=None)
    args.add_argument('-skipHeader', help='Skip the header row', default=False, action='store_true')
    return args


def main(args):
    api = BotoApi()
    if args.cache:
        api = CachedApi(args.cache, api)
    errors = []
    questions = readQuestions(args.questionsFile, args.skipHeader)
    with open(args.outFile, 'w') as fp:
        for row in toCsvOutput(toEntities(questions, api, errors)):
            print(','.join(['"%s"' % item.replace('"', '') for item in row]), file=fp)
    api.summary()


if __name__ == '__main__':
    main(setupArgs().parse_args())
