import logging
import multiprocessing
import os
import random
import time
from datetime import date

import pandas as pd
import requests
import selenium.common
import selenium.webdriver as webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

NUM_PROCESSES = 8
OUTPUT_FILE_PATH = f'./output/mobile.bg-offers-{date.today()}.csv'

SEARCH_PAGE_URL = 'https://www.mobile.bg/search/avtomobili-dzhipove'

MAKE_SELECT_XPATH = '//*[@id="mainholder"]/form/table/tbody/tr/td/table[2]/tbody/tr[3]/td[1]/select'
MODEL_SELECT_XPATH = '//*[@id="mainholder"]/form/table/tbody/tr/td/table[2]/tbody/tr[3]/td[3]/select'
SEARCH_BUTTON_XPATH = '//*[@id="mainholder"]/form/table/tbody/tr/td/table[2]/tbody/tr[1]/td[7]/input'
COOKIES_ACCEPT_BUTTON_XPATH = '/html/body/div[3]/div[2]/div[1]/div[2]/div[2]/button[1]'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'./logs/{date.today()}.log', mode='a'),
        logging.StreamHandler()
    ]
)

columns = [
    'Марка',
    'Модел',
    'Категория',
    'Дата на производство',
    'Тип двигател',
    'Скоростна кутия',
    'Кубатура [куб.см]',
    'Мощност',
    'Евростандарт',
    'Пробег [км]',
    'Пробег с едно зареждане (WLTP) [км]',
    'Капацитет на батерията [kWh]',
    'Цвят',
    'VIN номер',
    'Цена [лв./EUR]',
    'Безопасност',
    'Комфорт',
    'Други',
    'Екстериор',
    'Защита',
    'Интериор',
    'Специализирани',
    'Област [Извън страната]',
    'Населено място [Държава]',
    'Посещения',
    'Заглавие'
]

english_columns = [
    'Make',
    'Model',
    'Body type',
    'Production date',
    'Engine type',
    'Transmission',
    'Displacement [cc]',
    'Power',
    'Euro standard',
    'Mileage [km]',
    'WLTP mileage',
    'Battery capacity [kWh]',
    'Color',
    'VIN number',
    'Price [BGN/EUR]',
    'Safety features',
    'Comfort features',
    'Other features',
    'Exterior features',
    'Protection features',
    'Interior features',
    'Specialized features',
    'Region (Outside country)',
    'City (Country)',
    'Views',
    'Title'
]


def has_next_page(current_page_element) -> bool:
    """
    Check if there is a page after the current one
    :param current_page_element:
    :return:
    """
    return (current_page_element.find_next_sibling() is not None
            and current_page_element.find_next_sibling('a', class_='saveSlink ') is not None)


def get_page(url: str) -> BeautifulSoup:
    """
    Fetch and parse page HTML content
    :param url: URL of page to fetch
    :return: Soup(parsed HTML) of page
    """
    for i in range(3):
        try:
            logging.info(f'Getting page: {url}')
            page_html = requests.get('https:' + url)
            return BeautifulSoup(page_html.content, 'html.parser')
        except requests.exceptions.RequestException:
            logging.error(f'Request exception occurred. Attempt {i + 1}...')


def scrape_offer_data(make: str, model: str, offer_url: str, data: pd.DataFrame):
    """
    Scrape car details from offer page and add them to the dataset
    :param data:
    :param make:
    :param model:
    :param offer_url:
    :return:
    """
    logging.info(f'Scraping offer: {offer_url}')

    offer_soup = get_page(offer_url)

    parsed_data = {}

    try:
        parse_main_car_details(offer_soup, parsed_data)
    except ValueError:
        return

    parsed_data['Заглавие'] = offer_soup.find('h1').text.strip()

    # Assign make and model
    parsed_data['Марка'] = make.strip()
    parsed_data['Модел'] = model.strip()

    # Assign price
    parsed_data['Цена [лв./EUR]'] = offer_soup.find('span', id='details_price').text.strip()

    # String with region, city
    try:
        region_city_data = offer_soup.find('div', class_='adress').text.split(',')
    except AttributeError:
        logging.warning('No region and city data found')
        region_city_data = []

    parsed_data['Област [Извън страната]'] = region_city_data[0].strip() if len(region_city_data) > 0 else None
    parsed_data['Населено място [Държава]'] = region_city_data[1].strip() if len(region_city_data) > 1 else None

    # Assign views
    parsed_data['Посещения'] = offer_soup.find('span', class_='advact').text

    # Assign safety, comfort, other, exterior, protection, interior, specialized
    parse_additional_car_features(offer_soup, parsed_data)

    # Assign None to expected columns with missing data
    for missing_column in [col for col in columns if col not in parsed_data]:
        parsed_data[missing_column] = None

    logging.debug(parsed_data)

    # Add new row to dataset
    data.loc[len(data)] = parsed_data


def parse_additional_car_features(offer_soup: BeautifulSoup, target: dict):
    """
    Parse additional car features from offer page to dictionary
    :param offer_soup: The offer page soup
    :param target: The dictionary to add the parsed data to
    """
    additional_features_html_table_label_elements = offer_soup.find_all('label', class_='extra_cat')

    if additional_features_html_table_label_elements is not None:

        for label in additional_features_html_table_label_elements:
            next_sibling = label.find_next_sibling()

            # Sometimes the next sibling is a <br> with the <div> as a child
            feature_div = next_sibling.find('div') if next_sibling.name != 'div' else next_sibling

            if feature_div is None:
                feature_div = label.find_next_sibling('div')

            feature_list = []

            while feature_div is not None and feature_div.name != 'label':
                feature_list.append(feature_div.text[2:].strip())
                feature_div = feature_div.find_next_sibling()

            target[label.text] = ', '.join(feature_list)


def parse_main_car_details(offer_soup: BeautifulSoup, target: dict):
    """
    Parse main car details like type, kms, power, transmission etc. from offer page
    :param offer_soup: The offer page soup
    :param target: The dictionary to add the parsed data to
    """
    try:
        main_car_details_html_list = offer_soup.find('ul', class_='dilarData').find_all('li')
    except AttributeError:
        logging.warning('No main car details found')
        raise ValueError

    is_data = lambda index, el: index % 2 != 0
    main_data = list(enumerate(main_car_details_html_list))

    available_columns = [el.text for index, el in main_data if not is_data(index, el)]
    available_data = [el.text for index, el in main_data if is_data(index, el)]

    target.update(dict(zip(available_columns, available_data)))


def is_offer_link(tag) -> bool:
    """
    Check if tag is a link to an offer
    :param tag:
    :return:
    """
    return (tag.has_attr('href')
            and tag['href'].startswith('//www.mobile.bg/pcgi/mobile.cgi?act=4&')
            and tag.has_attr('class')
            and 'photoLink' in tag['class'])


def get_all_models(makes: list, chrome_driver: webdriver.Chrome) -> list:
    """
    Get all models from search page
    :param makes:
    :param chrome_driver:
    :return: List of tuples (make, model)
    """

    logging.info('Getting all models')

    tuples = []

    for make in makes:
        go_to_search_page_for_make(make, chrome_driver)
        model_select = chrome_driver.find_element(By.XPATH, MODEL_SELECT_XPATH)
        all_models = list(map(lambda x: x.text, model_select.find_elements(By.TAG_NAME, 'option')))[1:]

        new_tuples = [(make, model.strip()) for model in all_models]
        tuples.extend(new_tuples)

    logging.info(f'Found {len(tuples)} models')

    return tuples


def main():
    start_time = time.time()

    chrome_driver = instantiate_driver()

    chrome_driver.get(SEARCH_PAGE_URL)
    handle_cookies(chrome_driver)

    make_select = chrome_driver.find_element(By.XPATH, MAKE_SELECT_XPATH)

    all_makes = list(map(lambda x: x.text, make_select.find_elements(By.TAG_NAME, 'option')))[1:]

    all_models = get_all_models(all_makes, chrome_driver)
    random.shuffle(all_models)

    chrome_driver.quit()

    # More chunks than processes for better CPU utilization
    chunk_size = len(all_models) // (NUM_PROCESSES * 10)

    model_subsets = [all_models[i:i + chunk_size] for i in range(0, len(all_models), chunk_size)]

    with multiprocessing.Pool(NUM_PROCESSES, maxtasksperchild=5) as p:
        p.map(scrape_worker, model_subsets)

    # Merge partial results into single csv file
    for partial_results_file in os.listdir('./temp'):
        partial_results_df = pd.read_csv(f'./temp/{partial_results_file}')
        partial_results_df.to_csv(OUTPUT_FILE_PATH, index=False, mode='a',
                                  header=(not os.path.exists(OUTPUT_FILE_PATH)))

        os.remove(f'./temp/{partial_results_file}')

    # Swap columns to English
    final_df = pd.read_csv(OUTPUT_FILE_PATH)
    final_df.columns = english_columns

    duplication_identification_cols = final_df.columns.copy()
    duplication_identification_cols.remove(['Model', 'Views'])
    final_df = final_df.drop_duplicates(subset=duplication_identification_cols)

    final_df.to_csv(OUTPUT_FILE_PATH, index=False)

    print('Execution time: ', (time.time() - start_time) / 3600, ' hours')
    print(final_df.count())


def handle_cookies(chrome_driver):
    try:
        cookies_accept_button = chrome_driver.find_element(By.XPATH, COOKIES_ACCEPT_BUTTON_XPATH)
        cookies_accept_button.click()
    except selenium.common.NoSuchElementException:
        return


def instantiate_driver():
    browser_options = webdriver.ChromeOptions()
    # browser_options.add_argument("--headless=new")
    browser_options.add_argument("--disable-gpu")
    chrome_driver = webdriver.Chrome(browser_options)
    chrome_driver.set_page_load_timeout(60)
    return chrome_driver


def scrape_worker(models: list):
    logging.info(f'Scraping {models}...')

    # Initialize dataframe for task
    process_df = pd.DataFrame(columns=columns)

    chrome_driver = instantiate_driver()
    chrome_driver.get(SEARCH_PAGE_URL)
    handle_cookies(chrome_driver)

    for (make, model) in models:

        # Retry model on exception
        for i in range(3):
            try:
                scrape_model(chrome_driver, make, model, process_df)

                process_df.to_csv(f'./temp/partial-{os.getpid()}.csv', index=False, mode='a',
                                  header=(not os.path.exists(f'./output/partial-{os.getpid()}.csv')))

                process_df = pd.DataFrame(columns=columns)
            except selenium.common.WebDriverException as e:
                logging.error(f'Exception occurred in process {os.getpid()}: {e}')
                logging.warning(f'Retrying {make} {model}... Attempt {i + 1}')
            else:
                break

    chrome_driver.quit()

    print('Process ', os.getpid(), ' finished successfully')


def scrape_model(chrome_driver, make, model, process_df):
    go_to_model_offers(make, model, chrome_driver)

    logging.info('Getting offers for ' + make + ' ' + model)

    current_page_soup = get_page(chrome_driver.current_url[6:])  # Remove 'https:' from URL

    # Loop through all pages of offers for this model
    while True:
        offer_links = list(map(lambda tag: tag['href'], current_page_soup.find_all(is_offer_link)))

        for link in offer_links:
            scrape_offer_data(make, model, link, process_df)

        # Find current page indicator
        current_page_element = current_page_soup.find('a', class_='saveSlink selected')

        if not has_next_page(current_page_element):
            break

        # Load next page
        current_page_soup = get_page(current_page_element.find_next_sibling()['href'])


def select_model(model: str, chrome_driver: webdriver.Chrome):
    """
    Select model from search page model dropdown
    :param chrome_driver:
    :param model:
    :return:
    """
    model_select = chrome_driver.find_element(By.XPATH, MODEL_SELECT_XPATH)
    model_option = list(filter(lambda x: x.text.strip() == model, model_select.find_elements(By.TAG_NAME, 'option')))[0]
    model_option.click()


def go_to_search_page_for_make(make: str, chrome_driver: webdriver.Chrome):
    """
    Go to search page and select make from dropdown
    :param chrome_driver:
    :param make:
    :return:
    """

    if chrome_driver.current_url != SEARCH_PAGE_URL:
        chrome_driver.get(SEARCH_PAGE_URL)

    make_select = chrome_driver.find_element(By.XPATH, MAKE_SELECT_XPATH)
    make_option = list(filter(lambda x: x.text.strip() == make, make_select.find_elements(By.TAG_NAME, 'option')))[0]
    make_option.click()


# TODO: This doesn't work - stays on search page
def go_to_model_offers(make: str, model: str, chrome_driver: webdriver.Chrome):
    """
    Go to offers page for model
    :param model:
    :param chrome_driver:
    :param make:
    :return:
    """

    if chrome_driver.current_url != SEARCH_PAGE_URL:
        chrome_driver.get(SEARCH_PAGE_URL)

    handle_cookies(chrome_driver)

    # TODO: div class=fc-dialog-overlay obscures model select dropdown
    make_select = chrome_driver.find_element(By.XPATH, MAKE_SELECT_XPATH)
    make_option = list(filter(lambda x: x.text == make, make_select.find_elements(By.TAG_NAME, 'option')))[0]
    make_option.click()

    select_model(model, chrome_driver)
    search_page_search_button = chrome_driver.find_element(By.XPATH, SEARCH_BUTTON_XPATH)
    search_page_search_button.click()

    logging.info(f'After search button click for {make} {model} - {chrome_driver.current_url}')


if __name__ == '__main__':
    main()
