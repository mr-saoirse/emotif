#!/usr/bin/env python
import prefect
from prefect.environments.storage import Docker
from prefect import Flow, task
import pandas as pd

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import hashlib
import uuid
import boto3
import os
from prefect.client.secrets import Secret
import json

#TODO general best practice for setting up env variables and parameters in prefect
# TODO determine best deve workflow for testing flows


def migrate_statements_from_rss_entries(
        rss_uri='http://feeds.bbci.co.uk/news/world/rss.xml',
        uri_field='link'):
    f = feedparser.parse(rss_uri)
    f = pd.DataFrame(f.entries)
    for uri in f[uri_field]:
        text = soup_fetch_text(uri)
        save_text_to_bucket(text, uri)


def hash_string(s):
    return str(uuid.UUID(hashlib.md5(s.encode()).hexdigest()))


def soup_fetch_text(url):
    article = requests.get(url)
    soup = bs(article.content, 'html.parser')
    text = [p.text for p in soup.find_all('p')]
    return "/n".join(text)


def save_text_to_bucket(text, uri):
    encoded_string = text.encode("utf-8")
    bucket_name = "emotif-test1"
    file_name = f"{hash_string(uri)}.txt"
    #TODO create sub folders e.g. by dates etc.
    file_name = "/bbc-news/" + file_name

    s3 = boto3.resource("s3")

    print(f'saving to {file_name}: {encoded_string}')

    s3.Bucket(bucket_name).put_object(Key=file_name, Body=encoded_string)


def score_check(grade, subject, student):
    """
    This is a normal "business logic" function which is not a Prefect task.
    If a student achieved a score > 90, multiply it by 2 for their effort! But only if the subject is not NULL.
    :param grade: number of points on an exam
    :param subject: school subject
    :param student: name of the student
    :return: final nr of points
    """
    if pd.notnull(subject) and grade > 90:
        new_grade = grade * 2
        print(
            f'Doubled score: {new_grade}, Subject: {subject}, Student name: {student}'
        )
        return new_grade
    else:
        return grade


@task
def extract():
    """ Return a dataframe with students and their grades"""
    data = {
        'Name': [
            'Hermione', 'Hermione', 'Hermione', 'Hermione', 'Hermione', 'Ron',
            'Ron', 'Ron', 'Ron', 'Ron', 'Harry', 'Harry', 'Harry', 'Harry',
            'Harry'
        ],
        'Age': [12] * 15,
        'Subject': [
            'History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
            'History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
            'History of Magic', 'Dark Arts', 'Potions', 'Flying', None
        ],
        'Score':
        [100, 100, 100, 68, 99, 45, 53, 39, 87, 99, 67, 86, 37, 100, 99]
    }

    df = pd.DataFrame(data)
    return df


@task(log_stdout=True)
def transform(x):
    x["New_Score"] = x.apply(lambda row: score_check(grade=row['Score'],
                                                     subject=row['Subject'],
                                                     student=row['Name']), axis=1)
    return x


@task(log_stdout=True)
def load(y):
    S = Secret("AWS_CREDENTIALS").get()

    #print(f'We have a secret key {S} of type {type(S)}')

    #TODO: didnt check if we set this properly in json if we actually have to load the env
    #question is how to do you add env vars to a prefect flow runtime (best practice)
    #SS = data = json.loads(S)
    os.environ["AWS_ACCESS_KEY_ID"] = S['ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"] = S['SECRET_ACCESS_KEY']

    migrate_statements_from_rss_entries()

    print(f"Doing the full load for testing")


with Flow(
        "sirshreg",
        #TODO read this registry id from somewhere as we prefer not to check this in
        storage=Docker(
            registry_url="065921881757.dkr.ecr.us-east-1.amazonaws.com",
            python_dependencies=[
                "pandas==1.1.0", "feedparser", "boto3", "beautifulsoup4"
            ],
            secrets=["AWS_CREDENTIALS"],
            image_tag='latest')) as flow:

    load(None)

if __name__ == '__main__':
    # flow.run()

    # https://docs.prefect.io/orchestration/recipes/third_party_auth.html
    # set secrets in the environment
    # https://docs.prefect.io/api/latest/tasks/aws.html
    # to make sure context has a secrets attribute
    # prefect.context.setdefault("secrets", {})
    # AWS_SECRET = {
    #     "ACCESS_KEY": os.environ["AWS_ACCESS_KEY_ID"],
    #     "SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    # }

    # prefect.context.secrets["AWS_CREDENTIALS"] = AWS_SECRET

    flow.register(project_name='emotif')