import gzip
import logging
import os
import json

import click
import elasticsearch
from tqdm import tqdm
from elasticsearch import Elasticsearch, helpers

logging.basicConfig()
logger = logging.getLogger()


@click.group()
def cli():
    pass


def get_es_hosts(hosts):
    es_hosts = hosts or os.getenv('ES_HOSTS') or 'localhost:9200'
    return es_hosts.split(',')


@cli.command()
@click.argument('index')
@click.option('--hosts')
def dump(index, hosts):
    es = Elasticsearch(hosts=get_es_hosts(hosts))

    with gzip.open(index + '_dump.jsonl.gz', mode='wb') as out:
        try:
            for d in tqdm(helpers.scan(es, index=index,
                                       scroll=u'1m', raise_on_error=True, preserve_order=False)):
                source = d['_source']
                out.write(("%s\n" % json.dumps(source, ensure_ascii=False)).encode(encoding='UTF-8'))
        except elasticsearch.exceptions.NotFoundError:
            click.echo(f'Error dumping index {index}: Not Found')


@cli.command()
@click.argument('index')
@click.argument('target')
@click.option('--hosts')
def copy(index, target, hosts):
    es = Elasticsearch(hosts=get_es_hosts(hosts))
    es_target = Elasticsearch(hosts=get_es_hosts(target))

    docs = helpers.scan(es, index=index,
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
        click.echo(f'Error dumping index {index}: Not Found')


@cli.command()
@click.argument('path')
@click.argument('index')
@click.option('--hosts')
def ingest(path, index, hosts):
    es = Elasticsearch(hosts=get_es_hosts(hosts))

    with gzip.open(path, mode='rb') as f:
        helpers.streaming_bulk(es, (dict(
            _index=index,
            _type='_doc',
            _op_type="index",
            **(json.loads(line.decode(encoding='UTF-8')))) for line in f))


if __name__ == '__main__':
    cli()
