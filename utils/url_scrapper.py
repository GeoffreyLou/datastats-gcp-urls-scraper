from loguru import logger
from bs4 import BeautifulSoup

class UrlScraper:
    def __init__(
        self, 
        webpage: str, 
        job_to_scrap: str
    ) -> None:
        """
        Generate a soup from a Selenium webpage then scrap job informations
        
        Parameters
        ----------
        webpage: str
            The webpage from Selenium webdriver
        job_to_scrap: str
            The job name that will be scraped

        Returns
        -------
        None
        """
        self.soup = BeautifulSoup(webpage, "html.parser")
        self.formatted_jobs_list = []
        self.job_to_scrap = job_to_scrap
        self.urls_list = []

    def _generate_jobs_list(self) -> list:
        """
        Generate a job list from the soup generated at class init

        Returns
        -------
        jobs_list: list
            A list containing each job on the webpage soup
        """
        try:
            jobs_list = self.soup.find(
                'ul', 
                {'class': 'jobs-search__results-list'}
            ).find_all('li')
            return jobs_list
        except Exception as e:
            logger.error(f'Error while generating jobs list : {e}')
            return []

        
    def _get_lower_job_name(self, job_html_element) -> str:
        """
        Get the specific html element with the required informations.

        Parameters
        ----------
        job_html_element: str
            The html element to get information from

        Returns
        -------
        element: str
            The lower job name from html element
        """
        try:
            element = job_html_element.find(
                'h3', 
                {'class': 'base-search-card__title'}
            ).text.lower().strip()
            return element
        except Exception as e: 
            logger.error(f'Error while getting lower job name : {e}')
            return 'Not found'
    
    def _get_link(self, job_html_element) -> str:
        """
        Get the link to access the job html web page.

        Parameters
        ----------
        job_html_element: str
            tTe html element to get information from

        Returns
        -------
        link: str
            The link from html element.
        """
        try:
            link = job_html_element.find('a', href=True)['href']
            return link
        except Exception as e:
            logger.error(f'Error while getting link : {e}')
            return 'Not found'

    def generate_urls_list(self) -> list:
        """
        Generate a list containing job urls to scrape 
        
        Returns
        -------
        urls_list: list
            A list containing each link related to jobs scrapped
        """
        try:
            self.jobs_list = self._generate_jobs_list()
            for job in self.jobs_list:
                lower_job_name = self._get_lower_job_name(job)
                self.formatted_jobs_list.append(lower_job_name)
                if self.job_to_scrap in lower_job_name:
                    try:
                        logger.success(f"{lower_job_name} will be scraped because it DOES match: {self.job_to_scrap}.")        
                        self.link = self._get_link(job)
                    except Exception as e:
                        logger.error(f'Error while getting job link: {e}')
                        raise e
                    
                    # Adding scraped data to the url list
                    # The job will be added only if it matches job search
                    try:
                        self.urls_list.append(self.link)
                    except Exception as e:
                        logger.error(f'Error when append in url dict: {e}')
                        raise e
                else:
                    logger.warning(f"{lower_job_name} won't be scraped because it DOES NOT match : {self.job_to_scrap}.")
            return self.urls_list
        except Exception as e:
            logger.error(f'Error while starting scraping : {e}')
            return self.urls_list   
    
    def get_jobs_list(self):
        """
        Get the jobs list generated from the soup

        Returns
        -------
        jobs_list: list
            A list containing each job on the webpage soup
        """
        return self.formatted_jobs_list 