import gzip
import logging
import os
import json

import click
import elasticsearch
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers
import csv
import urllib3.exceptions

from ssl import create_default_context
from ssl import CERT_NONE
import urllib3
from multiprocessing import Pool
from functools import partial

logging.basicConfig()
logger = logging.getLogger()


@click.group()
def cli():
    pass


### dictionary and JSON helper functions
def read_json_from_gzip_file(f):
    for line in f:
        line = line.decode(encoding='UTF-8')
        yield json.loads(line)


def nested_replace(structure, transform_map):
    if type(structure) == list:
        return [nested_replace(item, transform_map) for item in structure]

    if type(structure) == dict:
        return {key: nested_replace(value, transform_map)
                for key, value in structure.items()}

    if structure in transform_map.keys():
        return transform_map[str(structure)]
    else:
        return structure


def apply_transformations(doc, trans_list):
    transform_map = {}
    # This transformation isn't currently helpful, but  is left as an example
    # if 'boolean_lowercase' in trans_list:
    #     transform_map.update({'True':'true','False':'false'})
    if transform_map:
        return nested_replace(doc, transform_map)
    else:
        return doc


### Elasticsearch helper functions
def get_es_hosts(hosts):
    es_hosts = hosts or os.getenv('ES_HOSTS') or 'localhost:9200'
    return es_hosts.split(',')


def get_es(hosts, username, pwd, crtfile, verify_cert, read_timeout=10):
    if crtfile is not None:
        context = create_default_context(cafile=crtfile)
        if not verify_cert:
            context.check_hostname = False
            context.verify_mode = CERT_NONE
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return Elasticsearch(hosts=get_es_hosts(hosts), ssl_context=context, timeout=read_timeout)
    if username and pwd:
        return Elasticsearch(hosts=get_es_hosts(hosts),
                             http_auth=(username, pwd),
                             timeout=read_timeout)
    return Elasticsearch(hosts=get_es_hosts(hosts), timeout=read_timeout)


def get_target_type(es):
    version = (int)(es.info()['version']['number'][0])
    if version >= 7:
        type = None
    else:
        type = "_doc"
    return type


### interface functions and main implementation
def delete_func(index, es_source, timeout, error_on_timeout):
    try:
        es_source.indices.delete(index, request_timeout=timeout)
    except elasticsearch.exceptions.NotFoundError:
        # TODO wildcards never get here even if no index exists. think whether implementing a check
        click.echo(f'Error deleting index {index}: Not Found', err=True)
        return False
    except (elasticsearch.exceptions.ConnectionTimeout, urllib3.exceptions.ReadTimeoutError):
        click.echo(f'Error deleting index {index}: Timeout', err=True)
        return error_on_timeout
    return True


@cli.command()
@click.argument('index')
@click.option('--hosts')
@click.option('-u', '--username')
@click.option('-p', '--pwd')
@click.option('--timeout', default=u'1m')
@click.option('--crtfile')
@click.option('--verify-cert/--no-verify-cert', default=False)
@click.option('--size', default=1000)
@click.option('--sliced/--no-sliced', default=False)
# TODO organize timeouts across functions
@click.option('--read-timeout', default=10)
@click.option('--query', default=None)
def dump(index, hosts, username, pwd, timeout, crtfile, verify_cert, size, sliced, read_timeout, query):
    if sliced:
        # TODO move get_es out of inner function in order to run it within copy_cluster
        dump_func_slice(index, hosts, username, pwd, crtfile, verify_cert, timeout, size, read_timeout, query)
    else:
        es_source = get_es(hosts, username, pwd, crtfile, verify_cert, read_timeout)
        dump_func(index, es_source, timeout, size, query)


def get_shards_info(client, index):
    shards_info = client.search_shards(index)
    number_of_shards = len(shards_info['shards'])
    nodes = dict([(k, v['transport_address'].split(':')[0]) for k, v in shards_info['nodes'].items()])

    shards_info = {}
    i = 0

    while len(shards_info.keys()) < number_of_shards:
        result = client.search_shards(index, routing=i)
        shard = _get_primary_shard(result)

        shard_number = shard['shard']
        node = shard['node']
        address = nodes[node]

        if shard_number not in shards_info:
            shards_info[shard_number] = address
        i += 1

    return shards_info


def _get_primary_shard(search_shards_result):
    shards = search_shards_result['shards'][0]
    return [shard for shard in shards if shard['primary'] is True][0]


def dump_slice(hosts, username, pwd, crtfile, verify_cert, index, size, timeout, read_timeout, slices, q, shard_info):
    slice = shard_info[0]
    es_source = get_es(hosts, username, pwd, crtfile, verify_cert, read_timeout)
    query = {"slice": {"id": slice, "max": slices}}
    if q:
        query['query'] = {"simple_query_string": {"query": q}}

    with gzip.open(index + '_' + str(slice) + '_dump.jsonl.gz', mode='wb') as out:
        try:
            for d in tqdm(
                    helpers.scan(es_source, index=index, query=query, size=size, scroll=timeout, raise_on_error=True,
                                 preserve_order=False, request_timeout=read_timeout)):
                out.write(("%s\n" % json.dumps({
                    '_source': d['_source'],
                    '_index': d['_index'],
                    '_type': d['_type'],
                    '_id': d['_id'],
                }, ensure_ascii=False)).encode(encoding='UTF-8'))
        except elasticsearch.exceptions.NotFoundError:
            click.echo(f'Error dumping index {index}: Not Found', err=True)
            return False
    return True


def dump_func_slice(index, hosts, username, pwd, crtfile, verify_cert, timeout, size, read_timeout, query):
    es_source = get_es(hosts, username, pwd, crtfile, verify_cert, read_timeout)
    info = get_shards_info(es_source, index)
    if len(info.keys()) < 2:
        dump_func(index, es_source, timeout, size, query)
    else:
        pool = Pool(len(info))
        prod_x = partial(dump_slice, hosts, username, pwd, crtfile, verify_cert, index, size, timeout, read_timeout, query, len(info))
        info_items = [(k, v) for k, v in info.items()]
        pool.map(prod_x, info_items)


def dump_func(index, es_source, timeout, size, query):
    file_name = index.replace('.', '_') + '_dump.jsonl.gz'
    logger.info(f'Dumping {index} to {file_name}')

    q = None
    if query:
        q = {"query": {"simple_query_string": {"query": query}}}

    with gzip.open(file_name, mode='wb') as out:
        counter = 0
        try:
            for d in tqdm(helpers.scan(es_source, index=index, query=q,
                                       scroll=timeout, raise_on_error=True, preserve_order=False, size=size)):
                out.write(("%s\n" % json.dumps({
                    '_source': d['_source'],
                    '_index': d['_index'],
                    '_type': d['_type'],
                    '_id': d['_id'],
                }, ensure_ascii=False)).encode(encoding='UTF-8'))
                counter += 1
                if counter % 1000 == 0:
                    out.flush()
        except elasticsearch.exceptions.NotFoundError:
            click.echo(f'Error dumping index {index}: Not Found', err=True)
            return False
    return True


@cli.command()
@click.argument('in_filename')
@click.argument('out_filename')
@click.option('--target')
@click.option('--source')
@click.option('--delete_timeout', default=60)
@click.option('--error_on_timeout', default=False)
@click.option('--preserve-index/--no-preserve-index', default=True)
@click.option('--preserve-ids/--no-preserve-ids', default=False)
@click.option('--abort-on-failure', default=True)
@click.option('--dump_timeout', default=u'1m')
@click.option('--crtfile-target')
@click.option('--verify-cert-target/--no-verify-cert-target', default=False)
@click.option('--crtfile-source')
@click.option('--verify-cert-source/--no-verify-cert-source', default=False)
@click.option('--transformations')
@click.option('--ingest-timeout', default=10)
@click.option('--size', default=1000)
def copy_cluster(in_filename, out_filename, target, source, delete_timeout, error_on_timeout, preserve_index,
                 preserve_ids,
                 abort_on_failure, dump_timeout, crtfile_target, verify_cert_target, crtfile_source, verify_cert_source,
                 transformations, ingest_timeout, size):
    if target is None and source is None:
        click.echo(f'No relevant Elasticsearch instances', err=True)
        return
    es_source = get_es(source, None, None, crtfile_source, verify_cert_source)
    es_target = get_es(target, None, None, crtfile_target, verify_cert_target)
    trans_list = []
    if transformations is not None:
        trans_list = transformations.split(",")
    # TODO override ES's behavior to use localhost as a default
    with open(out_filename, 'w') as out_file, open(in_filename, newline='') as in_file:
        reader = csv.reader(in_file, delimiter=',', quotechar='|')
        writer = csv.writer(out_file)
        for row in reader:
            cur_op = row[0]
            to_del = ""
            ok = False
            if cur_op == "copy" or cur_op == "dump":
                cur_index = row[1]
                if len(row) == 3:
                    to_del = row[2]
                if cur_op == "copy":
                    ok = copy_func(cur_index, es_target, es_source, trans_list)
                elif cur_op == "dump":
                    ok = dump_func(cur_index, es_source, dump_timeout, size, None)
            elif cur_op == "delete":
                cur_index = row[1]
                ok = delete_func(cur_index, es_source, delete_timeout, error_on_timeout)
            elif cur_op == "ingest":
                cur_file = row[1]
                cur_index = None
                if len(row) == 3:
                    cur_index = row[2]
                ok = ingest_func(cur_file, cur_index, es_target, preserve_index, preserve_ids, trans_list,
                                 ingest_timeout)
            result_row = row
            if ok is False:
                if abort_on_failure is True:
                    return
                result_row.append("failed")
            if to_del == "X" and cur_op != "delete" and ok is True:
                ok = delete_func(cur_index, es_source, delete_timeout, error_on_timeout)
                if not ok:
                    result_row.append("delete failed")
            writer.writerow(result_row)


@cli.command()
@click.argument('index')
@click.option('--target')
@click.option('--source')
@click.option('--crtfile-target')
@click.option('--verify-cert-target/--no-verify-cert', default=False)
@click.option('--crtfile-source')
@click.option('--verify-cert-source/--no-verify-cert-source', default=False)
@click.option('--transformations')
def copy(index, target, source, crtfile_target, verify_cert_target, crtfile_source, verify_cert_source,
         transformations):
    es_source = get_es(source, None, None, crtfile_source, verify_cert_source)
    es_target = get_es(target, None, None, crtfile_target, verify_cert_target)
    trans_list = []
    if transformations is not None:
        trans_list = transformations.split(",")
    copy_func(index, es_target, es_source, trans_list)


def copy_func(index, es_target, es_source, trans_list):
    docs = helpers.scan(es_source, index=index,
                        query={"sort": ["_doc"]},
                        scroll=u'1m', raise_on_error=True, preserve_order=False)
    type = get_target_type(es_target)
    indexer = helpers.streaming_bulk(es_target, (dict(
        _index=doc['_index'],
        _type=type,
        _op_type="index",
        **(apply_transformations(doc['_source'], trans_list))) for doc in docs))
    try:
        for _ in tqdm(indexer):
            pass
    except elasticsearch.exceptions.NotFoundError:
        click.echo(f'Error copying index {index}: Not Found', err=True)
        return False
    return True


@cli.command()
@click.argument('path')
@click.argument('index')
@click.option('--hosts')
@click.option('-u', '--username')
@click.option('-p', '--pwd')
@click.option('--preserve-index/--no-preserve-index', default=True)
@click.option('--preserve-ids/--no-preserve-ids', default=False)
@click.option('--crtfile')
@click.option('--verify-cert/--no-verify-cert', default=False)
@click.option('--transformations')
@click.option('--ingest-timeout', default=10)
def ingest(path, index, hosts, username, pwd, preserve_index, preserve_ids, crtfile, verify_cert, transformations,
           ingest_timeout):
    es_target = get_es(hosts, username, pwd, crtfile, verify_cert)
    trans_list = []
    if transformations is not None:
        trans_list = transformations.split(",")
    ingest_func(path, index, es_target, preserve_index, preserve_ids, trans_list, ingest_timeout)


def ingest_func(path, index, es_target, preserve_index, preserve_ids, trans_list, ingest_timeout=10):
    with gzip.open(path, mode='rb') as f:
        type = get_target_type(es_target)
        # objs = [json.loads(line.decode(encoding='UTF-8')) for line in f]
        it = helpers.streaming_bulk(es_target, (dict(
            _index=index if not preserve_index else o['_index'],
            _type=type,
            _id=None if not preserve_ids else o['_id'],
            _op_type="index",
            **(apply_transformations(o['_source'], trans_list))) for o in read_json_from_gzip_file(f)),
                                    max_chunk_bytes=10 * 1024 * 1024, request_timeout=ingest_timeout)
        for ok, response in it:
            if not ok:
                click.echo(f'Error indexing to {index}: response is {response}', err=True)


if __name__ == '__main__':
    cli()
