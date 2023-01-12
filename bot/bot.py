import telegram
import os
import ydb
import ydb.iam
import json

TOKEN = os.environ.get("BOT_TOKEN")
BOT = telegram.Bot(token=TOKEN)
driver: ydb.Driver
PHOTO_LINK_TEMPLATE = os.environ.get("PHOTO_LINK_TEMPLATE")
OBJECT_LINK_TEMPLATE = os.environ.get("OBJECT_LINK_TEMPLATE")


def get_driver():
    endpoint = os.environ.get("DB_ENDPOINT")
    path = os.environ.get("DB_PATH")
    creds = ydb.iam.MetadataUrlCredentials()
    driver_config = ydb.DriverConfig(
        endpoint, path, credentials=creds
    )
    return ydb.Driver(driver_config)


def get_face(chat_id):
    query = f"""
    PRAGMA TablePathPrefix("{os.environ.get("DB_PATH")}");
    SELECT * FROM photo_table WHERE name is NULL LIMIT 1;
    """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    session.closing()
    for row in result_sets[0].rows:
        face_id = row.face_id
        photo_url = PHOTO_LINK_TEMPLATE.format(face_id)
        print(photo_url)
        BOT.send_photo(chat_id=chat_id, photo=photo_url)


def add_name_to_last_photo(name):
    query = f"""
        PRAGMA TablePathPrefix("{os.environ.get("DB_PATH")}");
        SELECT * FROM photo_table WHERE name is NULL LIMIT 1;
        """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    face_id = ''
    for row in result_sets[0].rows:
        face_id = row.face_id
    if face_id == '':
        return
    query = f"""
    PRAGMA TablePathPrefix("{os.environ.get("DB_PATH")}");
    UPDATE photo_table SET name = '{name}' WHERE face_id = '{face_id}';
    """
    session.transaction().execute(query, commit_tx=True)
    session.closing()


def find(chat_id, name):
    query = f"""
    PRAGMA TablePathPrefix("{os.environ.get("DB_PATH")}");
    SELECT DISTINCT original_id, name FROM photo_table WHERE name = '{name}';
    """
    session = driver.table_client.session().create()
    result_sets = session.transaction().execute(query, commit_tx=True)
    session.closing()
    if len(result_sets[0].rows) == 0:
        BOT.sendMessage(chat_id, text=f'No photos with {name}')
    for row in result_sets[0].rows:
        object_id = row.original_id
        photo_url = OBJECT_LINK_TEMPLATE.format(object_id)
        BOT.send_photo(chat_id=chat_id, photo=photo_url)


def set_up():
    global driver
    driver = get_driver()
    driver.wait(timeout=5)


def handler(event, context):
    set_up()
    request = event['body']
    update = telegram.Update.de_json(json.loads(request), BOT)

    chat_id = update.message.chat.id
    command = update.message.text.encode('utf-8').decode()

    if command == '/start':
        BOT.sendMessage(chat_id=chat_id, text='Hello)')
        return
    if command == '/getface':
        get_face(chat_id)
        return
    if command.startswith('/find'):
        args = command.split(' ')
        find(chat_id, args[1])
        return
    add_name_to_last_photo(command)
    BOT.sendMessage(chat_id=chat_id, text=f'Added new name {command}')
