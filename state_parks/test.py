import json
import random
import urllib.request
import logging
import boto3
from datetime import datetime
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd

# from tentrr.log import loghandler

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import NoSuchElementException

# from datadog_lambda.wrapper import datadog_lambda_wrapper
# from aws_xray_sdk.core import xray_recorder
# from aws_xray_sdk.core import patch_all
# patch_all()

# xray_recorder.configure(context_missing='LOG_ERROR')
# logging.basicConfig(level='INFO')
# logging.getLogger('aws_xray_sdk').setLevel(logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_response = {
    "statusCode": 200,
    "body": json.dumps("Success")
}

format_error_response = {
    "statusCode": 500,
    "body": json.dumps("Unrecognized format")
}

def get_appconfig(env):
    url = 'http://localhost:2772/applications/state_parks/environments/{}/configurations/{}'.format(env, env)
    config = urllib.request.urlopen(url).read()
    return json.loads(config)

def get_driver(park, config):
    devicefarm_client = boto3.client("devicefarm", region_name="us-west-2")
    desired_capabilities = DesiredCapabilities.FIREFOX
    desired_capabilities["platform"] = "windows"
    testgrid_url_response = devicefarm_client.create_test_grid_url(
        projectArn="arn:aws:devicefarm:us-west-2:192994485053:testgrid-project:b5cec815-d8aa-4946-8654-cbcf9b7390d4",
        expiresInSeconds=300)
    driver = webdriver.Remote(
        command_executor=testgrid_url_response["url"],
        desired_capabilities=desired_capabilities
    )
    return driver

def handle_event(event, config):
    park = event["park"]
    state = event["state"]
    form = event["format"]

    driver = get_driver(park, config)

    # START
    driver.get("https://orms.reserveamerica.com/Start.do")
    # print(driver.title)
    driver.set_window_size(1680, 1025)
    driver.find_element(By.ID, "userName").click()
    driver.find_element(By.ID, "userName").send_keys(config["UT_USER"])
    driver.find_element(By.ID, "password").click()
    driver.find_element(By.ID, "password").send_keys(config["UT_PASS"])
    driver.find_element(By.ID, "okBtnAnchor").click()

    driver.find_element(By.ID, "selected_loc").click()
    dropdown = driver.find_element(By.ID, "selected_loc")
    dropdown.find_element(By.XPATH, f"//option[. = '{park}']").click()

    if form == "multiple_roles":
        driver.find_element(By.XPATH, "//tr[17]/td/table/tbody/tr/td[4]/a/span").click()
    elif form == "single_role":
        driver.find_element(By.LINK_TEXT, "Field Manager").click()
    elif form == "dropdown_roles":
        driver.find_element(By.ID, "6.e_StationDropDown").click()
        dropdown = driver.find_element(By.ID, "6.e_StationDropDown")
        dropdown.find_element(By.XPATH, "//option[. = 'WMSPCMO']").click()
    else:
        logger.error(f"Unsupported format: {form}")
        return format_error_responses

    driver.switch_to.frame(0)
    sleep(2)

    main_page = driver.current_window_handle

    driver.find_element(By.ID, "fieldmgr.site.leftmenu.id.7").click()
    driver.switch_to.default_content()
    driver.switch_to.frame(1)

    driver.find_element(By.ID, "search_loops").click()
    dropdown = driver.find_element(By.ID, "search_loops")
    dropdown.find_element(By.XPATH, "//option[. = 'Tentrr - Furnished Wall Tents']").click()
    driver.find_element(By.CSS_SELECTOR, "td:nth-child(6) .accesskey").click()

    sleep(5)
    # page = BeautifulSoup(driver.page_source, 'html.parser')
    # html = page.decode('utf-8')

    # dfs = pd.read_html(html, header=0)

    # print(dfs[5])
    # res_pd = dfs[5]
    # res_pd.columns = [
    #     'test_1',
    #     'test_2',
    #     'test_3',
    #     'test_4',
    #     'test_5',
    #     'test_6',
    #     'test_7',
    #     'test_8',
    #     'test_9',
    #     'test_10',
    #     'test_11',
    #     'test_12',
    #     'test_13',
    #     'test_14',
    #     'test_15',
    #     'test_16',
    #     'test_17',
    #     'test_18',
    #     'test_19',
    #     'test_20',
    #     'test_21',
    #     'test_22',
    #     'test_23',
    #     'test_24',
    #     'test_25',
    #     'test_26', 
    #     'test_27',
    #     'test_28',
    #     'test_29',
    #     'test_30',
    #     'test_31',
    #     'test_32',
    #     'test_33',
    #     'test_34',
    #     'test_35',
    #     'test_36',
    #     'test_37'
    # ]
    # site_list = json.loads(res_pd.to_json(orient='records'))

    # print(json.dumps(site_list))

    site = "Tentrr 001"

    driver.find_element(By.XPATH, f"//*[@aria-label='select site {site}']").click()

    sleep(5)

    driver.quit()

    return default_response

# @datadog_lambda_wrapper
# @loghandler
# def lambda_handler(event, context):
#     config = get_appconfig("prod")
    # response = handle_event(event, config)
    
    # if not response.get("statusCode"):
    #     response = {
    #         "statusCode": 500,
    #         "body": json.dumps("ERROR: Unable to Processing Event")
    #     }
    # return response


def main():
    event = {
        "version": 1.0,
        "state": "Utah",
        "park": "East Canyon State Park",
        "format": "multiple_roles"
    }
    response = handle_event(event, config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }
    return response

if __name__ == "__main__":
    main()