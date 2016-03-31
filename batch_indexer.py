#!/usr/bin/env python
from __future__ import print_function
import os, argparse, requests, time, multiprocessing

def index_batch(batch, solr_endpoint, batch_num):
    print('Sending batch {}...'.format(batch_num))
    start = time.time()
    batch_of_solr_docs = ['<add>']
    for s in batch:
        with open(s) as fd:
            batch_of_solr_docs.append(fd.read())

    batch_of_solr_docs.append('</add>')
    binary_data = ''.join(batch_of_solr_docs)

    res = requests.post(url = solr_endpoint,
                        data = binary_data,
                        headers = {'Content-Type':'text/xml'})
    end = time.time()
    print('Documents sent. Took {}s'.format(end - start))
    print(res.url)
    print(res.status_code)
    print(res.text)
    pass

def index(solr_core, data_dir='.', solr_url='http://localhost:8983/solr',
        batch_size=100):
    solr_endpoint = '{}/{}/update'.format(solr_url, solr_core)
    print('Indexing {} to {}'.format(args.data_dir, solr_endpoint))
    log_path = os.path.join(os.getcwd(), 'log')
    os.chdir(data_dir)
    solr_docs = []
    for f in os.listdir(os.getcwd()):
        if f.endswith('.xml'):
            solr_docs.append(f)
    print('Found {} documents.'.format(len(solr_docs)))

    batch_counter = 1
    pool = multiprocessing.Pool(4)
    for i in xrange(0, len(solr_docs), batch_size):
        batch = solr_docs[i:i+batch_size]
        pool.apply_async(index_batch, args = (batch, solr_endpoint, batch_counter))
        batch_counter += 1
    pool.close()
    pool.join()
    # commit all docs
    print('Batch indexing finished.')
    print('Commiting index')
    start = time.time()
    commit = requests.get(url = '{}?commit=true'.format(solr_url))
    end = time.time()
    print(commit.status_code)
    print(commit.text)
    print('Commit complete. Took {}s'.format(end - start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('-s', '--host', dest='solr_host', help='the solr host url')
    parser.add_argument('-c', '--core', dest='solr_core', help='the solr core',
            required=True)
    parser.add_argument('-b', '--batch-size', type=int, default=100,
            help='number of docs in a batch (default 100)') 
    args = parser.parse_args()
    index(args.solr_core, args.data_dir, args.solr_host, args.batch_size)
