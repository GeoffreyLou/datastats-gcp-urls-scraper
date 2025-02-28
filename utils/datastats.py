import json
from loguru import logger
from .gcp import GoogleUtils

class DataStats:
    def __init__(
            self, 
            script_execution_datetime: str,
            job_to_scrap: str,
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
        
        self.gcp = GoogleUtils()
    
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
            logger.debug(f'Jobs list: {jobs_list}')
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
             
        #  add_row_to_pgsql()
        #