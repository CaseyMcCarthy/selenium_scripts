import json
import random
import urllib.request
import logging
import boto3
from datetime import datetime
from time import sleep

from tentrr.log import loghandler

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

format_error_response = {
    "statusCode": 500,
    "body": json.dumps("Unrecognized format")
}

bad_event_response = {
    "statusCode": 500,
    "body": json.dumps("Unsupported event type")
}

error_response = {
    "statusCode": 500,
    "body": json.dumps("Unexpected Error")
}

def get_appconfig(env):
    url = 'http://localhost:2772/applications/state_parks/environments/{}/configurations/{}'.format(env, env)
    config = urllib.request.urlopen(url).read()
    return json.loads(config)

def get_driver():
    devicefarm_client = boto3.client("devicefarm", region_name="us-west-2")
    desired_capabilities = DesiredCapabilities.CHROME
    desired_capabilities["platform"] = "windows"
    desired_capabilities["aws:maxDurationSecs"] = "2400"

    testgrid_url_response = devicefarm_client.create_test_grid_url(
        projectArn="arn:aws:devicefarm:us-west-2:192994485053:testgrid-project:b5cec815-d8aa-4946-8654-cbcf9b7390d4",
        expiresInSeconds=3000)
    driver = webdriver.Remote(
        command_executor=testgrid_url_response["url"],
        desired_capabilities=desired_capabilities
    )
    return driver

def block_dates(event, config):
    reservation_id = event["reservation_id"]
    site = event["ra_name"]
    park = event["park"]
    form = event["format"]
    start = event["start"]
    end = event["end"]

    driver = get_driver()

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
    elif form == "dropdown_stations":
        driver.find_element(By.ID, "6.e_StationDropDown").click()
        dropdown = driver.find_element(By.ID, "6.e_StationDropDown")
        dropdown.find_element(By.XPATH, "//option[. = 'Visitor Center 1']").click()
    elif form == "multi_dropdown_stations":
        driver.find_element(By.ID, "6.e_StationDropDown").click()
        dropdown = driver.find_element(By.ID, "6.e_StationDropDown")
        dropdown.find_element(By.XPATH, "//option[. = 'Entrance Station']").click()
    else:
        logger.error(f"Unsupported format: {form}")
        return format_error_response

    driver.switch_to.frame(0)
    sleep(2)

    driver.find_element(By.ID, "fieldmgr.site.leftmenu.id.7").click()
    driver.switch_to.default_content()
    driver.switch_to.frame(1)

    driver.find_element(By.ID, "search_loops").click()
    dropdown = driver.find_element(By.ID, "search_loops")
    
    if park == "East Canyon State Park":
        dropdown.find_element(By.XPATH, "//option[. = 'A Tentrr Furnished Wall Tent']").click()
    elif park == "Kodachrome Basin State Park":
        dropdown.find_element(By.XPATH, "//option[. = 'Tentrr - Furnished Wall Tent']").click()
    else:
        dropdown.find_element(By.XPATH, "//option[. = 'Tentrr - Furnished Wall Tents']").click()
        
    driver.find_element(By.CSS_SELECTOR, "td:nth-child(6) .accesskey").click()

    # site checkbox 
    driver.find_element(By.XPATH, f"//*[@aria-label='select site {site}']").click()
    # close selected sites button
    driver.find_element(By.CSS_SELECTOR, "td:nth-child(1) > span a").click()
    # confirm pop up
    sleep(5)
    try:
        driver.find_element(By.XPATH, "//*[@id='confirmmsgboxcontent_1614624554873okbtn']").click()
    except NoSuchElementException:
        sleep(3)
        driver.find_element(By.XPATH, "/html/body/div[4]/div[3]/div/button[1]").click()
        

    # closure type always set to 'all'
    driver.find_element(By.ID, "closurePatternType").click()
    dropdown = driver.find_element(By.ID, "closurePatternType")
    dropdown.find_element(By.XPATH, "//option[. = 'All']").click()

    # date range
    # Fri Feb 26 2021
    driver.find_element(By.ID, "startDate_ForDisplay").click()
    driver.find_element(By.ID, "startDate_ForDisplay").send_keys(start)
    driver.find_element(By.ID, "endDate_ForDisplay").click()
    driver.find_element(By.ID, "endDate_ForDisplay").send_keys(end)

    # comment
    driver.find_element(By.ID, "comments").send_keys(f"Blocked for Tentrr reservation: {reservation_id}")

    driver.find_element(By.ID, "OKAnchor").click()
    sleep(1)

    logger.info("Blocked dates completed")

    driver.quit()

    return default_response

def unblock_dates(event, config):
    reservation_id = event["reservation_id"]
    site = event["ra_name"]
    park = event["park"]
    form = event["format"]
    start = event["start"]
    date = datetime.strftime(datetime.strptime(start, "%a %b %d %Y"), "%m-%d-%Y")

    driver = get_driver()

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
    elif form == "dropdown_stations":
        driver.find_element(By.ID, "6.e_StationDropDown").click()
        dropdown = driver.find_element(By.ID, "6.e_StationDropDown")
        dropdown.find_element(By.XPATH, "//option[. = 'Visitor Center 1']").click()
    elif form == "multi_dropdown_stations":
        driver.find_element(By.ID, "6.e_StationDropDown").click()
        dropdown = driver.find_element(By.ID, "6.e_StationDropDown")
        dropdown.find_element(By.XPATH, "//option[. = 'Entrance Station']").click()
    else:
        logger.error(f"Unsupported format: {form}")
        return format_error_response

    driver.switch_to.frame(0)
    sleep(2)

    driver.find_element(By.ID, "fieldmgr.site.leftmenu.id.7").click()
    driver.switch_to.default_content()
    driver.switch_to.frame(1)

    driver.find_element(By.ID, "search_loops").click()
    dropdown = driver.find_element(By.ID, "search_loops")

    if park == "East Canyon State Park":
        dropdown.find_element(By.XPATH, "//option[. = 'A Tentrr Furnished Wall Tent']").click()
    elif park == "Kodachrome Basin State Park":
        dropdown.find_element(By.XPATH, "//option[. = 'Tentrr - Furnished Wall Tent']").click()
    else:
        dropdown.find_element(By.XPATH, "//option[. = 'Tentrr - Furnished Wall Tents']").click()

    # Mon Aug 16 2021
    # search_date_ForDisplay
    element = driver.find_element(By.ID, "search_date_ForDisplay")
    actions = ActionChains(driver)
    actions.move_to_element(element).perform()
    driver.find_element(By.ID, "search_date_ForDisplay").clear()
    driver.find_element(By.ID, "search_date_ForDisplay").send_keys(start)

    driver.find_element(By.CSS_SELECTOR, "td:nth-child(6) .accesskey").click()

    # Date format 08-16-2021
    driver.find_element(By.XPATH, f"//*[@aria-label='{site} {date}']").click()
    sleep(2)

    driver.find_element(By.ID, "additional_comments").send_keys(f"Removing block for reservation {reservation_id}")

    driver.find_element(By.XPATH, "//*[@aria-label='Remove Closure']").click()
    sleep(2)
    driver.find_element(By.XPATH, "//*[@aria-label='OK']").click()

    logger.info("Unblock dates completed")

    driver.quit()

    return default_response

def handle_event(record, config):
    logger.info(record["Sns"]["Message"])
    data = json.loads(record["Sns"]["Message"])

    update_type = data["event"]
    park = data["park"]
    site = data["ra_name"]

    try:
        if update_type == "confirmed":
            logger.info(f"Blocking availability for {park} {site}")
            response = block_dates(data, config)
        elif update_type == "canceled":
            logger.info(f"Unblocking availability for {park} {site}")
            response = unblock_dates(data, config)
        else:
            logger.error(f"Unsuppored event type: {update_type}")
            return bad_event_response
    except Exception as e:
        logger.error("Availability lambda failed")
        logger.error(e)
        response = error_response

    return response

@loghandler
def lambda_handler(event, context):
    config = get_appconfig("prod")

    logger.info("Processing {} records:".format(len(event["Records"])))
    for record in event.get("Records"):
        response = handle_event(record, config)
    
    if not response.get("statusCode"):
        response = {
            "statusCode": 500,
            "body": json.dumps("ERROR: Unable to Processing Event")
        }
    return response