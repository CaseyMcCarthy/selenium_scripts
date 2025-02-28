import time
import json
import random
import urllib.request
import logging
import boto3

from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

from tentrr.log import loghandler
from tentrr.message import sns_publish

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import NoSuchElementException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

def get_appconfig(env):
    url = 'http://localhost:2772/applications/state_parks/environments/{}/configurations/{}'.format(env, env)
    config = urllib.request.urlopen(url).read()
    return json.loads(config)

def get_driver():
    devicefarm_client = boto3.client("devicefarm", region_name="us-west-2")
    desired_capabilities = DesiredCapabilities.FIREFOX
    desired_capabilities["platform"] = "windows"
    desired_capabilities["aws:maxDurationSecs"] = "2400"

    testgrid_url_response = devicefarm_client.create_test_grid_url(
        projectArn="arn:aws:devicefarm:us-west-2:192994485053:testgrid-project:36d0911c-5a54-4780-9629-a9da8f817c2f",
        expiresInSeconds=3000)
    driver = webdriver.Remote(
        command_executor=testgrid_url_response["url"],
        desired_capabilities=desired_capabilities
    )
    return driver

# def get_caps(park):
#     time = datetime.strftime(datetime.today(), "%Y %m %d - %H:%M")
#     desired_cap = {
#         'os' : 'Windows',
#         'os_version' : '10',
#         'browser' : 'Chrome',
#         'browser_version' : '80',
#         'name' : f'LA {park} - {time}'
#     }
#     return desired_cap

# def get_driver(park, config):
#     driver = webdriver.Remote(
#         command_executor=config["GRID_URL"],
#         desired_capabilities=get_caps(park)
#     )
#     return driver

def random_wait():
    random.seed(time.perf_counter())
    wait_time = random.randint(2, 3)  ### pick a random value between 2 and 3 sec for wait time
    print("selected random wait time of {} seconds".format(wait_time))
    return wait_time

#TODO config
def publish_event(reservation, config):
    ''' Publish SNS event

        Parmeters:
        reservation (dict)
        config (dict)
    '''
    payload = json.dumps(reservation)
    event_type = 'statepark.ingest'
    topic_arn = config.get('SNS_TOPIC_ARN')
    sns_publish(topic_arn, 
        event_type, 
        payload, 
        compress=False)

def format_reservations(driver, state, config):
    page = BeautifulSoup(driver.page_source, 'html.parser')
    html = page.decode('utf-8')

    dfs = pd.read_html(html, header=0)
    # print(dfs)
    logging.info(f"Number of reservations: {len(dfs[20])}")
    if len(dfs[20]) == 0:
        logger.info("No reservations to process, returning...")
        return 0

    res_pd = dfs[20]
    res_pd.columns = [
        'res_number',
        'invoice_number',
        'event_id',
        'res_status',
        'associated',
        'customer',
        'camper_phone',
        'camper_email',
        'org_name',
        'occupancy',
        'park_name',
        'area',
        'site_name',
        'type',
        'arrival',
        'departure',
        'timeblocks',
        'res_balance',
        'res_charges_balance',
        'test_1',
        'state',
        'use_fees'
    ]

    # print(res_pd.columns)
    # print(res_pd)
    reservation_list = json.loads(res_pd.to_json(orient='records'))
    time.sleep(random_wait())

    for reservation in reservation_list:
        logger.info(f"Processing reservation: {reservation['res_number']}")
        res_number = reservation["res_number"]

        driver.find_element(By.LINK_TEXT, res_number).click()
        
        try:
            driver.find_element(By.XPATH, "//*[@id='ReservationDetails']/tbody/tr[1]/td[2]/div[11]/span/div/a").click()
        except NoSuchElementException:
            logger.info(f"ALERT! For reservation {res_number}")
            driver.find_element(By.LINK_TEXT, "OK").click()
            time.sleep(random_wait())
            driver.find_element(By.XPATH, "//*[@id='ReservationDetails']/tbody/tr[1]/td[2]/div[11]/span/div/a").click()

        fees = BeautifulSoup(driver.page_source, 'html.parser')
        fees_html = fees.decode('utf-8')

        dfs2 = pd.read_html(fees_html)

        fees_pd = dfs2[4]
        fees_pd.columns = [
            'title',
            'amount',
            'change_to',
            'base_rate',
            'prev_adjusted',
            'schedule',
            'rate_unit',
            'applies_to',
            'additonal_info',
            'fee_dates'
        ]
        json_out2 = json.loads(fees_pd.to_json(orient='records'))
        fee_section = json_out2[0]
        reservation["use_fees"] = fee_section['amount']
        reservation["state"] = state

        publish_event(reservation, config)

        driver.find_element(By.LINK_TEXT, "Reservation Search/List").click()

    return len(reservation_list)

def handle_event(event, config):
    park = event["park"]
    state = event["state"]

    driver = get_driver()

    # START
    try:
        driver.get("https://orms.reserveamerica.com/Start.do")
        # print(driver.title)
        driver.set_window_size(1680, 1025)
        driver.find_element(By.ID, "userName").click()
        driver.find_element(By.ID, "userName").send_keys(config["LA_USER"])
        driver.find_element(By.ID, "password").click()
        driver.find_element(By.ID, "password").send_keys(config["LA_PASS"])
        driver.find_element(By.ID, "okBtnAnchor").click()
    
        driver.find_element(By.ID, "selected_loc").click()
        dropdown = driver.find_element(By.ID, "selected_loc")
        dropdown.find_element(By.XPATH, f"//option[. = '{park}']").click()
    
        driver.find_element(By.LINK_TEXT, "Field Manager").click()
    
        driver.switch_to.frame(0)
        time.sleep(random_wait())
        driver.find_element(By.XPATH, "//a[@id=\'Search\']").click()
    
        driver.switch_to.default_content()
        driver.switch_to.frame(1)
    
        driver.find_element(By.ID, "loop").clear()
        driver.find_element(By.ID, "loop").send_keys("*tentrr*")
        # time.sleep(random_wait())
    
        today = datetime.strftime(datetime.today(), "%a %b %d %Y")
        logger.info(f"Searching date: {today}")
        element = driver.find_element(By.ID, "arrival_ForDisplay")
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        driver.find_element(By.ID, "arrival_ForDisplay").click()
        driver.find_element(By.ID, "arrival_ForDisplay").send_keys(today)
    
        driver.find_element(By.ID, "IncludeLaterArrivals").click()
    
        # Search button
        driver.find_element(By.XPATH, "//div[2]/a").click()
    
        run_loop = True
        runs = 0
        while run_loop:
    
            time.sleep(1)
            logger.info(f"Page: {runs + 1}")
            reservation_count = format_reservations(driver, state, config)
            runs += 1
            
            if reservation_count == 50:
                logger.info("Moving to next page...")
                for x in range(0, runs):
                    time.sleep(1)
                    logger.info("Selecting next page button")
                    driver.find_element(By.CSS_SELECTOR, "td:nth-child(3) .link > a").click()
            else:
                run_loop = False
    
        driver.quit()
    
        logger.info("Process Complete")
        
    except Exception as e:
        logger.error("Retrieval lambda failed")
        logger.error(e)
        driver.quit()
    
    return default_response

@loghandler
def lambda_handler(event, context):
    config = get_appconfig("prod")
    response = handle_event(event, config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }
    return response