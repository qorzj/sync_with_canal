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


def get_primary_pair(columns, full_tablename, primary_key_map):
    if primary_key_map:
        primary_key = primary_key_map[full_tablename]
        primary_value = {column.name: column.value for column in columns}[primary_key]
        return primary_key, primary_value
    else:
        return '?', '?'


def handle_canal_entry(entry, primary_key_map, verbose, write, dest_mycursor):
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
            primary_key, primary_value = get_primary_pair(row.beforeColumns, full_tablename, primary_key_map)
            sql = f'DELETE FROM {full_tablename} WHERE {primary_key}=%s'
            sql_params = [primary_value]
        elif event_type == EntryProtocol_pb2.EventType.INSERT:
            row_data = {column.name: column.value for column in row.afterColumns}
            keys_text = ','.join(row_data.keys())
            vals_text = ','.join('%s' for x in row_data.keys())
            sql = f'INSERT INTO {full_tablename}({keys_text}) VALUES ({vals_text})'
            sql_params = list(row_data.values())
        elif event_type == EntryProtocol_pb2.EventType.UPDATE:
            primary_key, primary_value = get_primary_pair(row.beforeColumns, full_tablename, primary_key_map)
            row_data = {column.name: column.value for column in row.afterColumns}
            keys_text = ','.join(x + '=%s' for x in row_data.keys())
            sql = f'UPDATE {full_tablename} SET {keys_text} WHERE {primary_key}=%s'
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


def query_primary_key_map(dest_mydb, databases):
    primary_key_map = {}
    keys_text = ' or '.join('TABLE_SCHEMA=%s' for x in databases)
    sql = f'select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME from information_schema.COLUMNS where ({keys_text}) and COLUMN_KEY="PRI"'
    dest_mycursor = dest_mydb.cursor()
    dest_mycursor.execute(sql, databases)
    for (db_name, table_name, col_name) in dest_mycursor.fetchall():
        full_tablename = f"`{db_name}`.`{table_name}`"
        primary_key_map[full_tablename] = col_name
    return primary_key_map


def sync_with_canal(config, *, write, verbose):
    databases = eafp(lambda: config['databases'].split(','), [])
    assert databases, '配置文件错误：databases不能为空'
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
        filter=f'{filter_db_part}\\..*'.encode(),
    )
    dest_mydb = None
    primary_key_map = {}
    if write:
        dest_mydb = mysql.connector.connect(
            host=dest_mysql_config['host'],
            port=dest_mysql_config['port'],
            user=dest_mysql_config['username'],
            passwd=dest_mysql_config['password'],
        )
        primary_key_map = query_primary_key_map(dest_mydb, databases)
        if verbose:
            print(f'primary_key_map={primary_key_map}')
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
                handle_canal_entry(entry, primary_key_map, verbose, write, dest_mycursor)
            if write:
                dest_mydb.commit()
            time.sleep(0.5)
    finally:
        canal_client.disconnect()
        if dest_mydb is not None:
            dest_mydb.close()


@add_option('config_file', short='-c', long='--config', help='指定配置文件路径')
@add_option('write', short='-w', type='bool', help='指定是否写入dest_mysql，不指定则只消费队列')
@add_option('verbose', short='-v', type='bool', help='输出调试信息')
def start_sync(*args, **kwargs):
    """
    基于canal和python的数据库同步工具
    python start.py [options]
    """
    assert not args
    assert kwargs['config_file'], '请输入--config参数，或使用--help查看帮助'
    config_path = Path(kwargs['config_file'])
    assert config_path.exists(), '配置文件不存在'
    config = eafp(lambda: load_config(config_path))
    assert config, '配置文件读取和解析出错'
    sync_with_canal(config, write=kwargs['write'], verbose=kwargs['verbose'])


if __name__ == '__main__':
    Application().run_dealer(start_sync)