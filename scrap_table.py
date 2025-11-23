import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import os
import json


def lambda_handler(event, context):
    # URL de la página web que contiene los últimos sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "Error al realizar la solicitud HTTP", "details": str(e)}
            ),
        }

    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": json.dumps(
                {
                    "error": "Error al acceder a la página web",
                    "status_code": response.status_code,
                }
            ),
        }

    soup = BeautifulSoup(response.content, "html.parser")

    # Aquí ajustar según cómo esté estructurada la página del IGP:
    # ejemplo: la tabla podría tener un id o clase específica, o los sismos estar en un div
    # Supongamos que hay una tabla con <table id="sismos-reportados">…
    table = soup.find("table", {"class": "table"})
    if not table:
        # alternativa: quizá los sismos están listados en <ul> o <div class="sismo-item">
        return {
            "statusCode": 404,
            "body": json.dumps(
                {
                    "error": "No se encontró la tabla de sismos en la página web",
                    "page": soup.prettify(),
                }
            ),
        }

    headers_th = [th.get_text(strip=True) for th in table.find_all("th")]
    rows_data = []
    for row in table.find_all("tr")[1:]:  # omitir encabezado
        cells = row.find_all("td")
        if not cells:
            continue
        # sólo tomar los primeros 10
        if len(rows_data) >= 10:
            break
        item = {}
        for i, cell in enumerate(cells):
            key = headers_th[i] if i < len(headers_th) else f"col_{i}"
            item[key] = cell.get_text(strip=True)
        rows_data.append(item)

    # Insertar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_name = os.environ.get("DYNAMODB_TABLE", "TablaWebScrapping")
    db_table = dynamodb.Table(table_name)

    # Eliminar todos los elementos anteriores (opcional — evaluar si conviene)
    scan = db_table.scan()
    with db_table.batch_writer() as batch:
        for each in scan.get("Items", []):
            batch.delete_item(Key={"id": each["id"]})

    # Escribir los nuevos 10 sismos
    for idx, row in enumerate(rows_data, start=1):
        row["#"] = idx
        row["id"] = str(uuid.uuid4())
        db_table.put_item(Item=row)

    return {"statusCode": 200, "body": json.dumps(rows_data, ensure_ascii=False)}
