import json
from loguru import logger
from .gcp import GoogleUtils
from .pg_utils import PostgresUtils

class DataStats:
    def __init__(
            self, 
            script_execution_datetime: str,
            job_to_scrap: str,
            config: str
        ) -> None:
        """
        Class to interact with DataStats resources. 
        
        Parameters
        ----------
        script_execution_datetime : str
            The datetime when the script was executed.
        job_to_scrap : str
            The job to scrap.
        bucket_name : str
            The bucket name to store the data.
        
        Returns
        -------
        None
        """
        
        # Set variables
        self.formatted_date_time = script_execution_datetime.strftime("%Y-%m-%d_%H-%M")
        self.year_month = script_execution_datetime.strftime("%Y-%m")
        self.monthly_jobs_list_json = f'{self.year_month}_jobs_list.json'
        self.daily_jobs_csv = f'{self.formatted_date_time}_{job_to_scrap}.csv'
        self.today = script_execution_datetime.strftime("%Y-%m-%d")
        self.config = config
        self.gcp = GoogleUtils()
        
    def _generate_pg_instance(self):
        """
        Crea te a PostgresUtils instance to be used in the class
        """
        try:
            self.pg = PostgresUtils()
            return self.pg
        except Exception as e:
            logger.error(f'Error while generating PostGresUtils instance: {e}')
        
    def _set_pg_connection(self):
        """
        Set up the connection to Postgres SQL instance.
        The config from class init will be used as args.  
        """
        try:
            self.conn = self.pg.connect_with_ssl(
                db_host=self.config.DB_HOST,
                db_user=self.config.DB_USER,
                db_password=self.config.DB_USER_PASSWORD,
                db_name=self.config.DB_NAME,
                db_port=self.config.DB_PORT,
                db_root_cert=self.config.DB_ROOT_CERT,
                db_cert=self.config.DB_CERT,
                db_key=self.config.DB_KEY
            ) 
            return self.conn
        except Exception as e:
            logger.error(f'Error while setting up Postgres connection: {e}')
    
    def add_scraped_jobs_to_monhtly_list(
        self, 
        bucket_name: str, 
        jobs_list: list
    ) -> None:
        """
        Add the scraped jobs to the monthly list.
        
        Parameters
        ----------
        bucket_name : str
            The bucket name to store the data.
        jobs_list : list
            The list of jobs to add to the monthly list.
            
        Returns
        -------
        None
        """
        
        try:
            logger.info('Adding scraped jobs to monthly list...')
            # Check if the file exists
            file_exists = self.gcp.file_exists(
                bucket_name=bucket_name, 
                blob_name=self.monthly_jobs_list_json
            )
            
            if file_exists:
                # Download the file
                self.gcp.download_blob(
                    bucket_name=bucket_name,
                    source_blob_name=self.monthly_jobs_list_json,
                    destination_file_name=self.monthly_jobs_list_json
                )

                # Open the file
                with open(self.monthly_jobs_list_json, 'r') as f:
                    current_data = json.load(f)
                    
                current_data["jobs_list"].extend(jobs_list)
                    
            else:
                # Create the file      
                current_data = {"jobs_list": []}
                current_data["jobs_list"].extend(jobs_list)
                
            # Save the file 
            with open(self.monthly_jobs_list_json, 'w') as f:
                json.dump(current_data, f, indent=2)       
                
            # Upload the file
            self.gcp.upload_file(
                bucket_name=bucket_name,
                source_file_path=self.monthly_jobs_list_json,
                destination_blob_name=self.monthly_jobs_list_json
            )   
        except Exception as e:
            logger.error(f"Error when adding jobs to monthly list: {e}")
            raise e  
             
    def _create_urls_statistics_table(self):
        pg = self._generate_pg_instance()
        conn = self._set_pg_connection()
        
        schema = {
            'ID': 'SERIAL PRIMARY KEY',
            'TEST': 'VARCHAR(40)'
        }
        
        pg.create_table_if_not_exists(
            connection=conn,
            table_name='URLS_SCRAPPER_STATS',
            table_schema=schema
        )
    
    def start_workflow(self):
        try:
            logger.debug('bla')
            self._create_urls_statistics_table()
        except Exception as e:
            logger.error(f'blabla: {e}')