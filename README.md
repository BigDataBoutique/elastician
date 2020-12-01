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

Both dump and ingest now have additional parameters to allow accessing ES using a certificate.
Make sure the certificate file has the appropriate file permissions.

`--crtfile` - name of the certificate file. Notice that unless you specify a folder this should sit in the working directory of the container, which is /data .

`--verify-cert/--no-verify-cert` - Whether you're using a signed cert and want to verify it. By default false. 


### Copy index between clusters

```bash
docker run  --net=host --rm bigdataboutique/elastician copy source_index_name --target http://target-cluster:9200 --source http://10.63.246.27:9200
```
### Perform a set of instructions on clusters (e.g. to copy indices between clusters)
```bash
docker run  --net=host -v `pwd`:/instructions --rm bigdataboutique/elastician copy-cluster /instructions/source.csv /instructions/target.csv  --target http://target-cluster:9200 --source http://10.63.246.27:9200
```
First file name is csv with rows in the following structure:
```
instruction,index_name,to_del
```
where instruction is either `copy`, `dump`, `delete`, to_del is `X` if the index in the source is to be deleted following the action

For `ingest` the format is:
```
ingest,file_name,index_name
```
Other parameters for copy-cluster:

`--abort-on-failure`: stops the default is true

`--dump-timeout` - default is u'1m'

`--delete-timeout` - default is 60

`--error-on-timeout`: whether the delete function should return an error on timeout

`--preserve-index/--no-preserve-index` - whether the index saved in the dumps should be used during ingest

Copy and copy-cluster also have certificate parameters, that work similarly to those above:

`--crtfile-target`

`--verify-cert-target/--no-verify-cert`

`--crtfile-source`

`--verify-cert-source/--no-verify-cert-source`

Ingest, Copy and copy cluster have a transformations parameter, supporting a comma-separated list
of transformations. Currently available transformations:
`boolean_lowercase` - turn True and False into 'true' and 'false'
