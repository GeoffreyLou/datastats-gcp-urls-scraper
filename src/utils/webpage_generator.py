import time
import random
from loguru import logger
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException


PAGE_LOAD_TIMEOUT = 30  # seconds
SCRIPT_TIMEOUT = 30     # seconds


class WebpageGenerator():
    def __init__(self, headless=True):
        """
        Class to generate a Selenium webpage avoiding http errors and empty pages.
        """
        self.headless = headless
        self.driver = None

    def _initialize_driver(self) -> None:
        """
        Initialize the Selenium webdriver with options
        """

        # Generate a random user agent
        user_agent = UserAgent().random

        window_sizes = [
            "1920,1080",  # Full HD
            "2560,1440",  # QHD (2K)
            "3840,2160",  # 4K UHD
            "1366,768",   # HD
            "1440,900",   # WXGA+
            "1600,900",   # HD+
            "1280,720",   # HD
            "1680,1050",  # WSXGA+
            "1024,768",   # XGA
            "800,600",    # SVGA
            "2560,1600",  # WQXGA
            "3200,1800",  # QHD+
            "1360,768",   # WXGA
        ]

        # Selenium options
        options = webdriver.ChromeOptions()
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument(f"--window-size={random.choice(window_sizes)}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--incognito')
        options.add_argument('--disable-gpu')
        options.add_argument('--lang=fr-FR')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-features=MediaSessionService')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--headless' if self.headless else "")

        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            self.driver.set_script_timeout(SCRIPT_TIMEOUT)
        except Exception as e:
            logger.error(f'Error while initializing Selenium webdriver : {e}')
            raise e

    def _quit_driver(self) -> None:
        """
        Safely quit the webdriver.
        """
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            finally:
                self.driver = None

    def scroll_down(self, scrolls=3) -> None:
        """
        Scroll down the web page.

        Parameters
        ----------
        scrolls: int [optional]
            the number of times you want the page scrolled, default is 3

        Returns
        -------
            None
        """
        try:
            main_scroll = self.driver.find_element(By.XPATH, '/html')
            for _ in range(scrolls):
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", main_scroll
                )
                time.sleep(2)
        except Exception as e:
            logger.error(f'Error while scrolling down : {e}')
            raise e

    def generate_webpage(self, url, max_attempts=50) -> str:
        """
        Generate a Selenium webpage avoiding http errors and empty pages.

        Parameters
        ----------
        url: str
            the url that will be converted as web page
        max_attempts: int [optional]
            max attempts to generate webpage, default to 50

        Returns
        -------
        webpage: str
            the webpage code as string
        """
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            logger.info(f'Generating webpage, attempt {attempts}')

            try:
                self.driver.get(url)
            except TimeoutException:
                logger.warning(
                    f"Page load timed out on attempt {attempts}, reinitializing driver..."
                )
                self._quit_driver()
                self._initialize_driver()
                continue
            except WebDriverException as e:
                logger.warning(f"WebDriverException on attempt {attempts}: {e}. Retrying...")
                self._quit_driver()
                self._initialize_driver()
                continue

            time.sleep(1)

            body_elements = self.driver.find_elements(By.TAG_NAME, "body")

            if body_elements:
                logger.info('Webpage is not empty, checking for authwalls...')

                authwall_elements = self.driver.find_elements(By.XPATH, "//*[starts-with(@class, 'authwall')]")
                specific_text_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Bienvenue dans votre communauté professionnelle')]")
                error_codes = self.driver.find_elements(By.CLASS_NAME, "error-code")

                if authwall_elements or specific_text_elements or error_codes:
                    reasons = []
                    if authwall_elements:
                        reasons.append("authwall detected")
                    if specific_text_elements:
                        reasons.append("blocked message detected")
                    if error_codes:
                        reasons.append("error code detected")

                    logger.warning(
                        f"Unable to generate webpage on attempt {attempts} due to: {', '.join(reasons)}. Retrying..."
                    )
                    continue

                else:
                    webpage = self.driver.page_source
                    logger.success('Webpage successfully generated')
                    return webpage

        # (évite un retour None silencieux qui ferait crasher scroll_down ensuite)
        raise RuntimeError(
            f"Failed to generate webpage after {max_attempts} attempts: {url}"
        )

    def start(
        self,
        url: str,
        scrolls: int = 3,
        max_attempts: int = 50
    ):
        """
        Start the whole process, including initialization and webpage generation.

        Parameters
        ----------
        url: str
            URL to scrape
        scrolls: int [optional]
            scroll down the page n times
        max_attempts: int [optional]
            max attempts to generate webpage, default to 50

        Returns
        -------
        webpage: str
            html code of the webpage
        """
        try:
            if not self.driver:
                self._initialize_driver()

            webpage = self.generate_webpage(url, max_attempts)

            self.scroll_down(scrolls)

            return webpage

        except Exception as e:
            logger.error(f"Error during the scraping process: {e}")
            raise e