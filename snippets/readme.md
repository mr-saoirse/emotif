# Intro

The 'snippets' are complete sub-problems which contain some code snippets that can be refactored later as we build a proper production instance. The snippets allow for infra and code scraps to be prototyped. We are basically going to build a text processing pipeline where news feeds come in and knowledge graphs comes out. We can demonstrate how to deploy this in the cloud, scale it and invoke some state of the art deep learning models as part of the workflow.

We start with a simple AWS Lambda based examples which is the most rugged way to do it. We configure some feeds and move the raw data into S3 buckets on a schedule. ome useful scraping functions are accumulated and will be added to an ETL library later.

From here we jump to scale. Prefect is used as a workflow tool running on EKS/K8s. We focus on scheduling and distributed processing of the same scraping functions. Later we will use terraform and Argo CD to spin up the infrastructure for this.

Next we start to build in the NLP features. We use various tools and pre-trained models to extract interesting concepts from text. Many interesting things are stored as relations or triples in a graph database. Some of the relations are extracted from sentence understanding and some are augmentations from online known graph queries. The storage knowledge can be queried when answering questions about text. Various graph based queries can be used to infer things about text. We can do some interactive AML over our text, graph and columnar data.


A cloud abstraction layer called `silver-lining` protects transport and flow abstractions, hiding complexity. Complexity is managed by contract, and concerns are separated.
