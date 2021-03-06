import mysql.connector
from lesscli import Application, add_option
from lesscli.util import eafp
import toml
from pathlib import Path
import time
import sys
import logging
from canal.client import Client
from canal.protocol import EntryProtocol_pb2


def load_config(config_path: Path):
    config_text = config_path.open().read()
    return toml.loads(config_text)


def get_primary_pair(columns):
    for column in columns:
        if column.isKey:
            return column.name, column.value
    return '?', '?'


def str_or_null(column):
    return None if column.isNull else column.value


def handle_canal_entry(entry, verbose, write, dest_mycursor):
    row_change = EntryProtocol_pb2.RowChange()
    row_change.MergeFromString(entry.storeValue)
    header = entry.header
    database = header.schemaName
    table = header.tableName
    event_type = header.eventType
    if not (event_type == EntryProtocol_pb2.EventType.DELETE or
            event_type == EntryProtocol_pb2.EventType.INSERT or
            event_type == EntryProtocol_pb2.EventType.UPDATE):
        return
    full_tablename = f"`{database}`.`{table}`"
    for row in row_change.rowDatas:
        if event_type == EntryProtocol_pb2.EventType.DELETE:
            primary_key, primary_value = get_primary_pair(row.beforeColumns)
            sql = f'DELETE FROM {full_tablename} WHERE `{primary_key}`=%s'
            sql_params = [primary_value]
        elif event_type == EntryProtocol_pb2.EventType.INSERT:
            row_data = {column.name: str_or_null(column) for column in row.afterColumns}
            keys_text = ','.join(f'`{x}`' for x in row_data.keys())
            vals_text = ','.join('%s' for x in row_data.keys())
            sql = f'INSERT INTO {full_tablename}({keys_text}) VALUES ({vals_text})'
            sql_params = list(row_data.values())
        elif event_type == EntryProtocol_pb2.EventType.UPDATE:
            primary_key, primary_value = get_primary_pair(row.beforeColumns)
            row_data = {column.name: str_or_null(column) for column in row.afterColumns}
            keys_text = ','.join(f'`{x}`=%s' for x in row_data.keys())
            sql = f'UPDATE {full_tablename} SET {keys_text} WHERE `{primary_key}`=%s'
            sql_params = list(row_data.values()) + [primary_value]
        else:
            continue
        if verbose:
            print(f'sql=[{sql}] params={sql_params}')
        if write:
            try:
                dest_mycursor.execute(sql, sql_params)
            except Exception as e:
                logging.error(f'sql execute failed! error={type(e)} message=[{e}] sql=[{sql}] params={sql_params}')


def sync_with_canal(config, *, write, verbose):
    databases = eafp(lambda: config['databases'].split(','), [])
    assert databases, '?????????????????????databases????????????'
    if config['error_log_path']:
        logging.basicConfig(filename=config['error_log_path'])
    else:
        logging.basicConfig(stream=sys.stderr)
    canal_config = config['canal']
    dest_mysql_config = config['dest_mysql']
    canal_client = Client()
    filter_db_part = '|'.join(databases)
    canal_client.connect(host=canal_config['host'], port=canal_config['port'])
    canal_client.check_valid(username=canal_config['username'].encode(), password=canal_config['password'].encode())
    canal_client.subscribe(
        client_id=canal_config['client_id'].encode(),
        destination=canal_config['destination'].encode(),
        filter=f'({filter_db_part})\\..*'.encode(),
    )
    if verbose:
        print('filter:', f'({filter_db_part})\\..*')
    dest_mydb = None
    if write:
        dest_mydb = mysql.connector.connect(
            host=dest_mysql_config['host'],
            port=dest_mysql_config['port'],
            user=dest_mysql_config['username'],
            passwd=dest_mysql_config['password'],
        )
    try:
        dest_mycursor = None
        while True:
            if write:
                dest_mycursor = dest_mydb.cursor()
            message = canal_client.get(100)
            entries = message['entries']
            for entry in entries:
                if entry.entryType in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                    continue
                handle_canal_entry(entry, verbose, write, dest_mycursor)
            if write:
                dest_mydb.commit()
            time.sleep(0.5)
    finally:
        canal_client.disconnect()
        if dest_mydb is not None:
            dest_mydb.close()


@add_option('config_file', short='-c', long='--config', help='????????????????????????')
@add_option('write', short='-w', type='bool', help='??????????????????dest_mysql??????????????????????????????')
@add_option('verbose', short='-v', type='bool', help='??????????????????')
def start_sync(*args, **kwargs):
    """
    ??????canal???python????????????????????????
    python start.py [options]
    """
    assert not args
    assert kwargs['config_file'], '?????????--config??????????????????--help????????????'
    config_path = Path(kwargs['config_file'])
    assert config_path.exists(), '?????????????????????'
    config = eafp(lambda: load_config(config_path))
    assert config, '?????????????????????????????????'
    sync_with_canal(config, write=kwargs['write'], verbose=kwargs['verbose'])


if __name__ == '__main__':
    Application().run_dealer(start_sync)