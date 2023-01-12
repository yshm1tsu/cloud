import io
import json
import os
import random

from PIL import Image
import boto3
from sanic import Sanic
from sanic.response import empty
import ydb
import ydb.iam

PHOTO_TABLE_NAME = 'photo_table'

app = Sanic(__name__)

ydb_driver: ydb.Driver
config: dict

AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')


@app.after_server_start
async def after_server_start(app, loop):
    global config
    config = {
        'PHOTO_BUCKET': os.environ.get('PHOTO_BUCKET'),
        'FACE_BUCKET': os.environ.get('FACE_BUCKET'),
        'DB_ENDPOINT': os.environ.get("DB_ENDPOINT"),
        'DB_PATH': os.environ.get("DB_PATH")
    }
    global ydb_driver
    ydb_driver = get_driver()
    ydb_driver.wait(timeout=5)


@app.post("/")
async def start_function(request):
    messages = request.json['messages']
    for message in messages:
        try:
            process_message(message)
        except Exception as e:
            ValueError(e)
    return empty(status=200)


@app.after_server_stop
async def shutdown_function():
    ydb_driver.close()


def add_image_to_database(original_id, face_id):
    rand = random.Random()
    query = f"""
    PRAGMA TablePathPrefix("{config['DB_PATH']}");
    INSERT INTO {PHOTO_TABLE_NAME} (id, original_id, face_id)
    VALUES ({rand.getrandbits(64)}, '{original_id}', '{face_id}');
    """
    session = ydb_driver.table_client.session().create()
    session.transaction().execute(query, commit_tx=True)
    session.closing()


def get_image(bucket, name):
    session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    response = s3.get_object(
        Bucket=bucket,
        Key=name
    )
    return response['Body'].read()


def put_image(bucket, name, content):
    session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    s3.put_object(
        Body=content,
        Bucket=bucket,
        Key=name,
        ContentType='application/octet-stream'
    )


def get_driver():
    endpoint = config["DB_ENDPOINT"]
    path = config["DB_PATH"]
    creds = ydb.iam.MetadataUrlCredentials()
    driver_config = ydb.DriverConfig(
        endpoint, path, credentials=creds
    )
    return ydb.Driver(driver_config)


def process_message(message):
    body = json.loads(message['details']['message']['body'])
    image = Image.open(io.BytesIO(get_image(config['PHOTO_BUCKET'], body['object_key'])))
    face = body['face']
    x = set()
    y = set()
    for coordinate in face:
        x.add(int(coordinate['x']))
        y.add(int(coordinate['y']))
    sorted_x = sorted(x)
    sorted_y = sorted(y)
    left = sorted_x[0]
    right = sorted_x[1]
    top = sorted_y[0]
    bottom = sorted_y[1]
    face_id = f"face_{body['object_key'].removesuffix('.jpg')}_{random.randint(0, 100000)}.jpg"
    cut_face = image.crop((left, top, right, bottom))
    img_byte_arr = io.BytesIO()
    cut_face.save(img_byte_arr, format='JPEG')
    put_image(config['FACE_BUCKET'], face_id, img_byte_arr.getvalue())
    add_image_to_database(body['object_key'], face_id)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ['PORT']), motd=False, access_log=False)
