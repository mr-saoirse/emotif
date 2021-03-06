#!/usr/bin/env python
from prefect.environments.storage import Docker
from prefect import Flow, task
import pandas as pd


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
        print(f'Doubled score: {new_grade}, Subject: {subject}, Student name: {student}')
        return new_grade
    else:
        return grade


@task
def extract():
    """ Return a dataframe with students and their grades"""
    data = {'Name': ['Hermione', 'Hermione', 'Hermione', 'Hermione', 'Hermione',
                     'Ron', 'Ron', 'Ron', 'Ron', 'Ron',
                     'Harry', 'Harry', 'Harry', 'Harry', 'Harry'],
            'Age': [12] * 15,
            'Subject': ['History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
                        'History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
                        'History of Magic', 'Dark Arts', 'Potions', 'Flying', None],
            'Score': [100, 100, 100, 68, 99,
                      45, 53, 39, 87, 99,
                      67, 86, 37, 100, 99]}

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
    old = y["Score"].tolist()
    new = y["New_Score"].tolist()
    print(f"ETL finished. Old scores: {old}. New scores: {new}")


with Flow("sirshreg",
#TODO read this registry id from somewhere as we prefer not to check this in
          storage=Docker(registry_url="065921881757.dkr.ecr.us-east-1.amazonaws.com",
                         python_dependencies=["pandas==1.1.0, feedparser, boto3, beautifulsoup4"],
                         image_tag='latest')) as flow:
    extracted_df = extract()
    transformed_df = transform(extracted_df)
    load(transformed_df)


if __name__ == '__main__':
    # flow.run()
    flow.register(project_name='emotif')