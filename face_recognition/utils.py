import base64
import json
import os

import boto3
import requests

FACE_RECOGNITION_URL = 'https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze'
MESSAGE_QUEUE_URL = 'https://message-queue.api.cloud.yandex.net/b1g71e95h51okii30p25/dj600000000b174o02mk/vvot11-tasks'
API_KEY = os.environ.get('API_SECRET_KEY')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')


def get_request(content):
    return {
        "analyze_specs": [{
            "content": content,
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }


def get_object(bucket, name):
    session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.client(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    response = s3.get_object(
        Bucket=bucket,
        Key=name
    )
    return response['Body'].read()


def find_faces(img):
    encoded = base64.b64encode(img).decode('UTF-8')
    auth_header = {'Authorization': f'Api-Key {API_KEY}'}
    body = get_request(encoded)
    response = requests.post(FACE_RECOGNITION_URL, json=body, headers=auth_header)
    coordinates = []
    try:
        faces = response.json()['results'][0]['results'][0]['faceDetection']['faces']
        for face in faces:
            coordinates.append(face['boundingBox']['vertices'])
    except KeyError:
        return []
    return coordinates


def convert_to_message(object_key, face):
    return {
        'object_key': object_key,
        'face': face
    }


def send_faces_to_queue(object_key, faces):
    session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    sqs = session.client(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    )
    messages = [convert_to_message(object_key, face) for face in faces]
    for message in messages:
        body = json.dumps(message)
        sqs.send_message(
            QueueUrl=MESSAGE_QUEUE_URL,
            MessageBody=body,
            MessageDeduplicationId=object_key
        )


def handler(event, context):
    bucket = event['messages'][0]['details']['bucket_id']
    name = event['messages'][0]['details']['object_id']
    img = get_object(bucket, name)
    send_faces_to_queue(name, find_faces(img))
