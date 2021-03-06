# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch from MS SQL Server
without using an Airflow DAG.

"""

import sys
import textwrap
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.mssql_metadata_extractor import MSSQLMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.task.neo4j_staleness_removal_task import Neo4jStalenessRemovalTask

es_host = None
neo_host = None
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

es = Elasticsearch([
    {'host': es_host if es_host else 'localhost'},
])

DB_FILE = '/tmp/test.db'
Base = declarative_base()

NEO4J_ENDPOINT = 'bolt://{}:7687'.format(neo_host if neo_host else 'localhost')

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


# todo: connection string needs to change
def connection_string(windows_auth=False):
    """Generages an MSSQL connection string.


    Keyword Arguments:
        windows_auth {bool} -- set to true if connecting to DB using windows
                                credentials. (default: {False})

    Returns:
        [str] -- [connection string]
    """

    if windows_auth:
        base_string = (
            "mssql+pyodbc://@{host}/{db}" +
            "?driver=ODBC+Driver+17+for+SQL+Server" +
            "?trusted_connection=yes" +
            "&autocommit=true"  # comment to disable autocommit.
        )
        params = {

            "host": "localhost",
            "db": "master"
        }

    else:
        base_string = (
            "mssql+pyodbc://{user}:{pword}@{host}/{db}" +
            "?driver=ODBC+Driver+17+for+SQL+Server" +
            "&autocommit=true"  # comment to disable autocommit.
        )
        params = {
            "user": "SA",
            "pword": "Root@123",
            "host": "localhost",
            "db": "PersonDB"
        }

    return base_string.format(**params)


def run_mssql_job():
    where_clause_suffix = textwrap.dedent("""
        ('dbo')
    """)

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(
        tmp_folder=tmp_folder)

    job_config = ConfigFactory.from_dict({
        # MSSQL Loader
        'extractor.mssql_metadata.{}'.format(
            MSSQLMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
        'extractor.mssql_metadata.{}'.format(
            MSSQLMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME): True,
        'extractor.mssql_metadata.extractor.sqlalchemy.{}'.format(
            SQLAlchemyExtractor.CONN_STRING): connection_string(),
        # NEO4J Loader
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })

    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=MSSQLMetadataExtractor(),
            loader=FsNeo4jCSVLoader()),
        publisher=Neo4jCsvPublisher())
    return job


def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   cypher_query=None,
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_doc_type_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if cypher_query:
        job_config.put('extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                       cypher_query)
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job

# Modified portion
def remove_stale_data():
    task = Neo4jStalenessRemovalTask()

    job_config_dict = {
        'job.identifier': 'remove_stale_data_job',
        'task.remove_stale_data.neo4j_endpoint': neo4j_endpoint,
        'task.remove_stale_data.neo4j_user': neo4j_user,
        'task.remove_stale_data.neo4j_password': neo4j_password,
        'task.remove_stale_data.staleness_max_pct': 110,
        'task.remove_stale_data.target_nodes': ['Table', 'Column'],
        'task.remove_stale_data.job_publish_tag': 'idk'
    }

    job_config = ConfigFactory.from_dict(job_config_dict)
    job = DefaultJob(conf=job_config, task=task)
    
    return job


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)
    
    #Modified portion
    removal_job = remove_stale_data()
    removal_job.launch()

    loading_job = run_mssql_job()
    loading_job.launch()

    job_es_table = create_es_publisher_sample_job(
        elasticsearch_index_alias='table_search_index',
        elasticsearch_doc_type_key='table',
        model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
    job_es_table.launch()
