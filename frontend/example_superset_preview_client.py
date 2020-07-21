import logging
import requests
import uuid
from bs4 import BeautifulSoup

from requests import Response
from typing import Any, Dict  # noqa: F401

from amundsen_application.base.base_superset_preview_client import BaseSupersetPreviewClient

# 'main' is an existing default Superset database which serves for demo purposes
DEFAULT_DATABASE_MAP = {
    'main': 1,
    'mssql':2
}

LOGIN_URL = 'http://10.0.2.15:8088/login/'
DEFAULT_URL = 'http://10.0.2.15:8088/superset/sql_json/'



class SupersetPreviewClient(BaseSupersetPreviewClient):
    def __init__(self,
                 *,
                 database_map: Dict[str, int] = DEFAULT_DATABASE_MAP,
                 url: str = DEFAULT_URL, login_url: str = LOGIN_URL) -> None:
        self.database_map = database_map
        self.headers = {}
        self.url = url
        self.login_url = login_url

    def post_to_sql_json(self, *, params: Dict, headers: Dict) -> Response:
        """
        Returns the post response from Superset's `sql_json` endpoint
        """
        # Create the appropriate request data
        try:

            request_data = {}

            # Superset's sql_json endpoint requires a unique client_id
            request_data['client_id'] = uuid.uuid4()

            # Superset's sql_json endpoint requires the id of the database that it will execute the query on
            database_name = params.get('database')
            request_data['database_id'] = self.database_map.get(database_name, '')
            
            # set up session for auth
            s = requests.Session()
    
            login_form = s.get(self.login_url) 
            
            # get Cross-Site Request Forgery protection token
            soup = BeautifulSoup(login_form.text, 'html.parser')
            csrf_token = soup.find('input',{'id':'csrf_token'})['value'] 
            
            # login the given session
            s.post(self.login_url,data=dict(username='admin', password='root',csrf_token=csrf_token))

            # Generate the sql query for the desired data preview content
            try:
                schema = params.get('schema')

                table_name = params.get('tableName')

                request_data['sql'] = 'SELECT * FROM {schema}.{table}'.format(schema=schema, table=table_name)

            except Exception as e:
                logging.error('Encountered error generating request sql: ' + str(e))
        except Exception as e:
            logging.error('Encountered error generating request data: ' + str(e))
        
        result = s.post(self.url, json={"client_id":f"{request_data['client_id']}",'database_id':request_data['database_id'],'sql':f"{request_data['sql']}"})
        
        logging.info(f"Result:{result.text}")

        return result
