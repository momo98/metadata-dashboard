from datetime import datetime
from elasticsearch import Elasticsearch
from iron_mq import IronMQ
from json import loads


#connection to es cluster
es = Elasticsearch(['http://58d3ea40c46e8b15000.qbox.io:80'])

#initiate ironmq connection
ironmq = IronMQ(host="mq-aws-us-east-1.iron.io",
            project_id="557330ae33f0350006000040",
            token="JZsM3ArjIhEfiKlG52Bt99b7Hh4",
            protocol="https", port=443,
            api_version=1,
            config_file=None)

#specify the queue where seqment is writing 
event_queue = ironmq.queue("event-stream")


#iterate over things waiting in the queue
for i in range(event_queue.info()['size']):
    #get next event off queue
    data = event_queue.get()
    try:
        #get rid of cruft
        event = data['messages'][0]
        #id to delete on sucessful write
        queue_id = event['id']
        #interesting things
        body = loads(event['body'])

        #take arbitrary properties 
        doc = body['Properties']

        #convert date into datetime
        doc['timestamp'] = datetime.strptime(body['Date'], '%Y-%m-%d')

        #change index to body['index'] when testing is over
        res = es.index(index="test-index", doc_type=body['Type'], id=queue_id, body=doc)
        if res['created'] == True:
            print 'success'
            event_queue.delete(queue_id)
    except Exception,e: 
        #ugh errors
        print str(e)
        print data

