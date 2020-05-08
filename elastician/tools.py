import gzip
import logging
import os
import json

import click
import elasticsearch
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers
import csv

logging.basicConfig()
logger = logging.getLogger()


@click.group()
def cli():
    pass


def get_es_hosts(hosts):
    es_hosts = hosts or os.getenv('ES_HOSTS') or 'localhost:9200'
    return es_hosts.split(',')

def delete_func(index,es_source):
    try:
        es_source.indices.delete(index)
    except elasticsearch.exceptions.NotFoundError:
        click.echo(f'Error deleting index {index}: Not Found', err=True)


@cli.command()
@click.argument('index')
@click.option('--hosts')
def dump(index, hosts):
    es_source = Elasticsearch(hosts=get_es_hosts(hosts))
    dump_func(index,es_source)

def dump_func(index, es_source):
    with gzip.open(index + '_dump.jsonl.gz', mode='wb') as out:
        try:
            for d in tqdm(helpers.scan(es_source, index=index,
                                       scroll=u'1m', raise_on_error=True, preserve_order=False)):
                source = d['_source']
                out.write(("%s\n" % json.dumps(source, ensure_ascii=False)).encode(encoding='UTF-8'))
        except elasticsearch.exceptions.NotFoundError:
            click.echo(f'Error dumping index {index}: Not Found', err=True)
            return False
    return True


@cli.command()
@click.argument('in_filename')
@click.argument('out_filename')
@click.argument('target')
@click.option('--hosts')

def copy_cluster(in_filename,out_filename,target,hosts):
    es_source = Elasticsearch(hosts=get_es_hosts(hosts))
    es_target = Elasticsearch(hosts=get_es_hosts(target))
    with open(out_filename, 'w') as out_file, open(in_filename, newline='') as in_file:
        reader = csv.reader(in_file, delimiter=',', quotechar='|')
        writer = csv.writer(out_file)
        for row in reader:
            if len(row) == 2:
                cur_index, cur_op = row
                to_del = ""
            elif len(row) == 3:
                cur_index, cur_op, to_del = row
            ok = False
            if cur_op == "copy":
                ok = copy_func(cur_index,es_target,es_source)
            elif cur_op == "dump":
                ok = dump_func(cur_index,es_source)
            if ok is False:
                return
            if to_del == "X":
                delete_func(cur_index,es_source)
            writer.writerow(row)

@cli.command()
@click.argument('index')
@click.argument('target')
@click.option('--hosts')
def copy(index, target, hosts):
    es_source = Elasticsearch(hosts=get_es_hosts(hosts))
    es_target = Elasticsearch(hosts=get_es_hosts(target))
    copy_func(index, es_target, es_source)

def copy_func(index, es_target, es_source):
    docs = helpers.scan(es_source, index=index,
                        scroll=u'1m', raise_on_error=True, preserve_order=False)

    indexer = helpers.streaming_bulk(es_target, (dict(
        _index=doc['_index'],
        _type='_doc',
        _op_type="index",
        **doc['_source']) for doc in docs))
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
def ingest(path, index, hosts):
    es = Elasticsearch(hosts=get_es_hosts(hosts))
    with gzip.open(path, mode='rb') as f:
        it = helpers.streaming_bulk(es, (dict(
            _index=index,
            _type='_doc',
            _op_type="index",
            **(json.loads(line.decode(encoding='UTF-8')))) for line in f))
        for ok, response in it:
            if not ok:
                click.echo(f'Error indexing to {index}: response is {response}', err=True)


if __name__ == '__main__':
    cli()
