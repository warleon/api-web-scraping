import json
import os
import uuid
import time
import boto3

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import ChromeOptions


def get_driver():
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--window-size=1280x1696")
    chrome_options.binary_location = "/opt/chromium"

    service = Service("/opt/chromedriver")

    return webdriver.Chrome(service=service, options=chrome_options)


def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    driver = get_driver()
    driver.get(url)

    # esperar render React
    time.sleep(5)

    # ------------- EXTRACCIÓN EXACTA COMO EL CÓDIGO ORIGINAL -------------
    try:
        table = driver.find_element(By.TAG_NAME, "table")
    except:
        page = driver.page_source
        driver.quit()
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "No se encontró la tabla", "page": page}),
        }

    # headers (<th>)
    headers_el = table.find_elements(By.TAG_NAME, "th")
    headers_th = [h.text.strip() for h in headers_el]

    # rows (<tr>)
    rows_el = table.find_elements(By.TAG_NAME, "tr")[1:]  # skip header

    rows_data = []
    for row in rows_el:
        cells = row.find_elements(By.TAG_NAME, "td")
        if not cells:
            continue

        if len(rows_data) >= 10:
            break

        item = {}
        for i, cell in enumerate(cells):
            key = headers_th[i] if i < len(headers_th) else f"col_{i}"
            item[key] = cell.text.strip()

        rows_data.append(item)

    driver.quit()
    # ----------------------------------------------------------------------

    # Insertar id y número correlativo
    for i, row in enumerate(rows_data, start=1):
        row["#"] = i
        row["id"] = str(uuid.uuid4())

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaWebScrapping")

    # borrar registros previos
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # insertar nuevos
    with table.batch_writer() as batch:
        for item in rows_data:
            batch.put_item(Item=item)

    return {"statusCode": 200, "body": json.dumps(rows_data, ensure_ascii=False)}
