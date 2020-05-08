# Elastician

A set of tools to easily develop and manage Elasticsearch and the Elastic Stack

## Build or get from Docker Hub

Get from Docker Hub:

```bash
docker pull bigdataboutique/elastician
```

Build locally:

```bash
docker build .
```

## Commands

### Dump an index to a file

```bash
docker run  --net=host -v `pwd`:/data --rm bigdataboutique/elastician dump myindex --hosts http://10.63.246.27:9200
```

### Upload file and ingest it into an index

```bash
docker run  --net=host -v `pwd`:/data --rm bigdataboutique/elastician ingest myindex_dump.jsonl.gz myindex --hosts http://10.63.246.27:9200
```

### Copy index between clusters

```bash
docker run  --net=host --rm bigdataboutique/elastician copy source_index_name http://target-cluster:9200 --hosts http://10.63.246.27:9200
```
### Copy more than one index between clusters
```bash
docker run  --net=host -v `pwd`:/instructions --rm bigdataboutique/elastician copy-cluster /instructions/source.csv /instructions/target.csv  http://target-cluster:9200 --hosts http://10.63.246.27:9200
```
First file name is csv with rows in the following structure:
```
index_name,instruction,to_del
```
where instruction is either `copy` or `dump`, to_del is `X` if the index in the source is to be deleted following dump or copy
