import os
import sys
import json
from loguru import logger
from datetime import datetime
from dotenv import load_dotenv
from utils.gcp import GoogleUtils
from utils.datastats import DataStats
from utils.webpage_generator import WebpageGenerator
from utils.url_scrapper import UrlScraper

if __name__ == '__main__':

    # ------------------------------------------------------------------------------------------------------------------
    # Set config & env vars
    # ------------------------------------------------------------------------------------------------------------------
    
    # ENV variables
    load_dotenv()  
    job_to_scrap = os.getenv('JOB_TO_SCRAP')
    bucket_name = os.getenv('DATASTATS_BUCKET')
    url_to_scrap = os.getenv('URL_TO_SCRAP')
    
    config = {
        'job_to_scrap': job_to_scrap,
        'bucket_name': bucket_name,
        'url_to_scrap': url_to_scrap
    }
    
    if any(value is None for value in config.values()):
        logger.error(f'Missing environment variables: {config}')
        sys.exit(1)
     
    # Formatted variables 
    url_to_scrap = url_to_scrap.replace(
        'JOB_TO_SCRAP', job_to_scrap.replace(' ', '%20')                       # Same url for any job to scrape
    ) 
    
    # Date utils variables
    script_execution_datetime = datetime.now()
    formatted_date_time = script_execution_datetime.strftime("%Y-%m-%d")
    file_name_date_time = script_execution_datetime.strftime("%Y-%m-%d_%H-%M")
    year_month = script_execution_datetime.strftime("%Y-%m")
    monthly_jobs_list_json = f'{year_month}_jobs_list.json'
    daily_jobs_file_name = f'{file_name_date_time}_{job_to_scrap}.json'

    logger.info(f'Scrapping {job_to_scrap} jobs')
    
    # ------------------------------------------------------------------------------------------------------------------
    # Selenium webpage generation
    # ------------------------------------------------------------------------------------------------------------------

    try:
        logger.info('Generating webpage...')
        webpage_generator = WebpageGenerator(headless=True)                    # Class instance to generate webpage  
        webpage = webpage_generator.start(url=url_to_scrap)                    # Generate the webpage
    except Exception as e:
        logger.error(f"Error while trying to generate webpage: {e}")
        raise e

    # ------------------------------------------------------------------------------------------------------------------
    # Beautifulsoup urls scraping from webpage
    # ------------------------------------------------------------------------------------------------------------------

    try:
        logger.info('Scraping urls...')
        url_scrapper = UrlScraper(webpage=webpage, job_to_scrap=job_to_scrap)  # Class instance to scrape urls
        urls_list = url_scrapper.generate_urls_list()                          # Generate list of urls      
        jobs_list = url_scrapper.get_jobs_list()                               # Get the jobs list scraped for statistic purpose
    except Exception as e:
        logger.error(f"Error while scraping with BeautifulSoup: {e}")
        raise e

    # ------------------------------------------------------------------------------------------------------------------
    # Interact with DataStats
    # ------------------------------------------------------------------------------------------------------------------

    datastats = DataStats(
        script_execution_datetime=script_execution_datetime, 
        job_to_scrap=job_to_scrap
    )
    
    datastats.add_scraped_jobs_to_monhtly_list(
        bucket_name=bucket_name, 
        jobs_list=jobs_list
    )

    # TODO : Ajouter la logique pour :
    # - Vérifier si le json est présent dans le bucket
    # - Si oui, le récupérer, si non, le créer en code et l'upload
    
    """     # For statistic purpose
        today = script_execution_datetime.strftime("%Y-%m-%d")
        script_duration = str(datetime.now() - script_execution_datetime).split('.')[0]
        jobs_scraped = len(jobs_list)
        jobs_scraped_matched = len(df)
        
        # Ajouter la logique pour insérer ces valeurs dans une table pgsql
        print(today)
        print(job_to_scrap)
        print(jobs_scraped)
        print(jobs_scraped_matched)
        print(script_duration)     """
        
    
    if len(urls_list) > 0:
        try:
            logger.info(f'Generating {job_to_scrap} JSON')
            job_data = {
                'date': formatted_date_time,
                'job': {
                    job_to_scrap: urls_list
                }
            }
            json_data = json.dumps(job_data, indent=2)
        except Exception as e:
            logger.error(f'Error while generating JSON: {e}')
            raise e
        
        try:
            logger.info(f'uploading {daily_jobs_file_name} to {bucket_name}...')
            gcp = GoogleUtils
            gcp.upload_non_physical_file(
                bucket_name=bucket_name,
                data=json_data,
                destination_blob_name=daily_jobs_file_name,
                content_type='application/json'
            )
        except Exception as e:
            logger.error(f'Error while uploading JSON: {e}')
            raise e
    else:
        logger.warning(f'No jobs have been scraped and matched with {job_to_scrap}.')