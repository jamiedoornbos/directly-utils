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

MAX_TEXT_LENGTH = 5000


class Question(object):
    def __init__(self, linenum, row):
        self.linenum = linenum
        self.id, \
            self.queue_name, \
            self.ticket_source, \
            self.subject, \
            self.text = row

    def truncate(self):
        text = self.text
        unicode_text = self.text.decode('utf8')
        if len(text) > MAX_TEXT_LENGTH:
            while len(text) > MAX_TEXT_LENGTH:
                unicode_text = unicode_text[:-1]
                text = unicode_text.encode('utf8')
            print(
                "Truncating question", self.id, "on line", self.linenum,
                "from binary length", len(self.text),
                "to binary length", len(text),
                "and unicode length", len(unicode_text))
        return unicode_text


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
                question = Question(linenum, row)
                if len(question.text) == 0:
                    print("Skipping empty question on line", question.linenum)
                    continue
                yield question
        except csv.Error:
            print("Error reading csv file on row", linenum + 1)
            raise


class BotoApi(object):
    def __init__(self):
        self.client = boto3.client('comprehend')
        self.count = 0

    def __call__(self, questions):
        self.count += 1
        result = self._invoke(questions)
        if self.count % 100 == 0:
            self.summary()
        return result

    def _invoke(self, questions):
        raise NotImplementedError()

    def summary(self):
        print('Queried {:,} batches'.format(self.count))


class DetectEnglishEntities(BotoApi):
    def _invoke(self, questions):
        return self.client.batch_detect_entities(
            TextList=[question.truncate() for question in questions], LanguageCode='en')


class DetectDominantLanguage(BotoApi):
    def _invoke(self, questions):
        return self.client.batch_detect_dominant_language(
            TextList=[question.truncate() for question in questions])


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


class Mode(object):
    def __init__(self, cache):
        self.errors = []
        api = self.createApi()
        self.api = CachedApi(cache, api) if cache else api

    def fetchResults(self, questions, resultField):
        for batch in toBatches(questions, 25):
            result = self.api(batch)
            self.errors.extend(result['ErrorList'])
            for row, item in zip(batch, result['ResultList']):
                yield row, item[resultField]

    def csvOutput(self, questions):
        raise NotImplementedError()


class DetectEntities(Mode):
    def createApi(self):
        return DetectEnglishEntities()

    def csvOutput(self, questions):
        yield ['Question Id'] + ENTITY_TYPES
        for question, entities in self.fetchResults(questions, 'Entities'):
            coll = collections.defaultdict(list)
            for entity in entities:
                coll[entity['Type']].append(entity['Text'])
            if len(coll) == 0:
                continue
            yield [question.id] + [' + '.join(coll.get(e, [])) for e in ENTITY_TYPES]


class DetectLanguages(Mode):
    def createApi(self):
        return DetectDominantLanguage()

    def csvOutput(self, questions):
        yield ['Question Id', 'Language', 'Score']
        for question, languages in self.fetchResults(questions, 'Languages'):
            if len(languages) > 1:
                print("Question", question.id, "on line", question.linenum,
                    "has multiple languages:", languages)
            best = max(languages, key=lambda l: l['Score'])
            yield [question.id, best['LanguageCode'], str(best['Score'])]


MODES = [
    ('detect-entities', DetectEntities),
    ('detect-languages', DetectLanguages)
]


def setupArgs():
    args = argparse.ArgumentParser()
    args.add_argument(
        'mode', metavar='MODE', help='Mode to run', choices=(mode[0] for mode in MODES))
    args.add_argument(
        'questionsFile', metavar='QUESTIONS', help='File containing questions, one per line')
    args.add_argument(
        'outFile', metavar='ENTITIES', help='File to write output (will be overwritten)')
    args.add_argument('-cache', help='Use a cache', default=None)
    args.add_argument('-skipHeader', help='Skip the header row', default=False, action='store_true')
    return args


def main(args):
    mode = dict(MODES)[args.mode](args.cache)
    questions = readQuestions(args.questionsFile, args.skipHeader)
    with open(args.outFile, 'w') as fp:
        for row in mode.csvOutput(questions):
            print(','.join(['"%s"' % item.replace('"', '').encode('utf8') for item in row]), file=fp)
    mode.api.summary()


if __name__ == '__main__':
    main(setupArgs().parse_args())
