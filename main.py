
import sys
from loguru import logger
from datetime import datetime
from utils.webpage_generator import WebpageGenerator
from utils.urls_scrapper import UrlsScraper
from utils.config_loader import Config
from utils.datastats_utils import DataStats
from google.cloud import logging as gcloud_logging

# Initialize Google Cloud Logging
client = gcloud_logging.Client()
client.setup_logging()

if __name__ == '__main__':

    # ------------------------------------------------------------------------------------------------------------------
    # Set config & env vars
    # ------------------------------------------------------------------------------------------------------------------
    
    try:
        config = Config.load()
    except EnvironmentError as e:
        logger.error(f'Error while generating config: {e}')
        sys.exit(0)
        
    # Formatted variables 
    url_to_scrap = config.URL_TO_SCRAP.replace(
        'JOB_TO_SCRAP', config.JOB_TO_SCRAP.replace(' ', '%20')                
    ) 
    
    # Date utils variables
    script_execution_start_time = datetime.now()
    
    logger.info(f'Scrapping {config.JOB_TO_SCRAP} jobs')
    
    # ------------------------------------------------------------------------------------------------------------------
    # Selenium webpage generation
    # ------------------------------------------------------------------------------------------------------------------

    try:
        logger.info('Generating webpage...')
        webpage_generator = WebpageGenerator(headless=True)                   
        webpage = webpage_generator.start(url=url_to_scrap)                    
    except Exception as e:
        logger.error(f"Error while trying to generate webpage: {e}")
        sys.exit(1)
    # ------------------------------------------------------------------------------------------------------------------
    # Beautifulsoup urls scraping from webpage
    # ------------------------------------------------------------------------------------------------------------------

    try:
        logger.info('Scraping urls and generating jobs list...')
        url_scrapper = UrlsScraper(webpage=webpage, job_to_scrap=config.JOB_TO_SCRAP) 
        urls_list = url_scrapper.generate_urls_list()                              
        scraped_jobs_list = url_scrapper.get_jobs_list()                           
    except Exception as e:
        logger.error(f"Error while scraping with BeautifulSoup: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------------------------------------------------------
    # Interact with DataStats resources : generating files, statistics, uploading, etc.
    # ------------------------------------------------------------------------------------------------------------------

    try:
        logger.info('Starting Datastats resources workflow...')
        datastats = DataStats(
            script_execution_start_time=script_execution_start_time, 
            scraped_jobs_list=scraped_jobs_list,
            matched_jobs_list=urls_list,
            config=config
            )
        
        datastats.start_workflow()
    except Exception as e:
        logger.error(f"Error while interacting with Datastats resources: {e}")
        sys.exit(1)