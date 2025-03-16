import os
import json
import time
from loguru import logger
from datetime import datetime
from .gcp_utils import GoogleUtils
from .pg_utils import PostgresUtils
from .config_loader import Config

class DataStats:
    def __init__(
            self, 
            script_execution_start_time: str,
            scraped_jobs_list: list,
            matched_jobs_list: list,
            config: Config
        ) -> None:
        """
        Class to interact with Datastats project and resources. 
        
        Parameters
        ----------
        script_execution_start_time : str
            The datetime when the script was executed.
        scraped_jobs_list : list
            The list of scraped jobs.
        matched_jobs_list : list
            The list of matched jobs from scraped_jobs_list.
        config: Config
            The config instance containing variables
        
        Returns
        -------
        None
        """
        
        # Set variables
        self.script_execution_start_time = script_execution_start_time
        self.today = script_execution_start_time.strftime("%Y-%m-%d")
        self.year_month = script_execution_start_time.strftime("%Y-%m")
        file_name_date_time = script_execution_start_time.strftime("%Y-%m-%d_%H-%M")
        self.daily_jobs_file_name = f'{file_name_date_time}_{config.JOB_TO_SCRAP}.json'
        self.monthly_jobs_list_json = f'{self.year_month}_jobs_list.json'
        self.config = config
        
        self.scrapped_jobs_list = scraped_jobs_list
        self.matched_jobs_list = matched_jobs_list
        self.urls_scrapper_statistics_table_name = 'urls_scrapper_statistics'
        self.urls_scrapper_statistics_table_schema = {
            'ID': 'SERIAL PRIMARY KEY',
            'SCRAP_DATE': 'VARCHAR(40)',
            'JOB_TO_SCRAP': 'VARCHAR(60)',
            'JOBS_SCRAPED': 'INTEGER',
            'JOBS_SCRAPED_MATCHED': 'INTEGER',
            'SCRAP_DURATION': 'VARCHAR(60)'
        }
    
    def _set_script_execution_duration(self):
        """
        Set the script execution duration.
        """
        try:
            end_time = datetime.now()
            duration_seconds = int((end_time - self.script_execution_start_time).total_seconds())
            formatted_duration = time.strftime('%H:%M:%S', time.gmtime(duration_seconds))
            return formatted_duration
        except Exception as e:
            logger.error(f'Error while setting script execution duration: {e}')
    
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
        
        gcp = GoogleUtils()
        
        try:
            # Check if the file exists
            file_exists = gcp.file_exists(
                bucket_name=bucket_name, 
                blob_name=self.monthly_jobs_list_json
            )
            
            if file_exists:
                # Download the file
                gcp.download_blob(
                    bucket_name=bucket_name,
                    source_blob_name=self.monthly_jobs_list_json,
                    destination_file_name=self.monthly_jobs_list_json
                )

                # Open the file
                with open(self.monthly_jobs_list_json, 'r') as f:
                    current_data = json.load(f)
                    
                # Add the jobs to it
                current_data["jobs_list"].extend(jobs_list)
                    
            else:
                # Create the file and add jobs to it 
                current_data = {"jobs_list": []}
                current_data["jobs_list"].extend(jobs_list)
                
            # Save the file 
            with open(self.monthly_jobs_list_json, 'w') as f:
                json.dump(current_data, f, indent=2)       
                
            # Upload the file
            gcp.upload_file(
                bucket_name=bucket_name,
                source_file_path=self.monthly_jobs_list_json,
                destination_blob_name=self.monthly_jobs_list_json
            )
            
            # Remove the local file
            os.remove(self.monthly_jobs_list_json)
        except Exception as e:
            logger.error(f"Error when adding jobs to monthly list: {e}")
            raise e  

    def generate_json_to_upload(
        self,
        job_to_scrap: str,
        date: str,
        urls_list: list
    ):
        """
        Generate a JSON formatted dict to upload.
        """
        try:
            logger.info(f'Generating {job_to_scrap} JSON')
            job_data = {
                'date': date,
                'job': {
                    job_to_scrap: urls_list
                }
            }
            json_data = json.dumps(job_data, indent=2)
            return json_data
        except Exception as e:
            logger.error(f'Error while generating JSON: {e}')
            raise e

    def start_workflow(self):
        
        logger.info('Starting workflow to interact with Datastats resources...')
        
        try:
            logger.info('Setting connection to pgsql...')
            pg = PostgresUtils()  
            conn = pg.connect_with_ssl(
                db_host=self.config.DB_HOST,
                db_user=self.config.DB_USER,
                db_password=self.config.DB_USER_PASSWORD,
                db_name=self.config.DB_NAME,
                db_port=self.config.DB_PORT,
                db_root_cert=self.config.DB_ROOT_CERT,
                db_cert=self.config.DB_CERT,
                db_key=self.config.DB_KEY            
            )
        
            logger.info('Checking if urls statistics table exists or create it...')
            pg.create_table_if_not_exists(
                connection=conn,
                table_name=self.urls_scrapper_statistics_table_name,
                table_schema=self.urls_scrapper_statistics_table_schema
            )
        
            logger.info('Inserting statistics data...')
            script_duration = self._set_script_execution_duration()
            pg.insert_data(
                connection=conn,
                table_name=self.urls_scrapper_statistics_table_name,
                data={
                    'SCRAP_DATE': self.today,
                    'JOB_TO_SCRAP': self.config.JOB_TO_SCRAP,
                    'JOBS_SCRAPED': len(self.scrapped_jobs_list),
                    'JOBS_SCRAPED_MATCHED': len(self.matched_jobs_list),
                    'SCRAP_DURATION': script_duration
                }
            )
            
            logger.info('Adding scraped jobs to monthly list...')
            self.add_scraped_jobs_to_monhtly_list(
                bucket_name=self.config.DATASTATS_BUCKET_UTILS,
                jobs_list=self.scrapped_jobs_list
            )
            
            if len(self.matched_jobs_list) > 0:  
                logger.info('Generating JSON to upload...')
                json_data = self.generate_json_to_upload(
                    job_to_scrap=self.config.JOB_TO_SCRAP,
                    date=self.today,
                    urls_list=self.matched_jobs_list
                )

                logger.info(f'Uploading {self.daily_jobs_file_name} to GCP...')
                gcp = GoogleUtils()
                gcp.upload_non_physical_file(
                    bucket_name=self.config.DATASTATS_BUCKET_URLS,
                    data=json_data,
                    destination_blob_name=self.daily_jobs_file_name,
                    content_type='application/json'
                )
            else:
                logger.warning(f'No jobs have been scraped and matched with {self.config.JOB_TO_SCRAP}.')
                
        except Exception as e:
            logger.error(f'Error while executing Datastats workflow: {e}')
            raise e
        
        finally:
            if conn is not None:
                logger.info('Closing connection to pgsql...')
                pg.close_connection(conn)
