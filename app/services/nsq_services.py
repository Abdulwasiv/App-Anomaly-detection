import nsq
import tornado.ioloop
import json
from app.views.kpi_views import account_id

class NSQService:
    def __init__(self, nsqd_host='127.0.0.1', nsqd_port=4150, topic='metrics_data', channel='default'):
        self.nsqd_host = nsqd_host
        self.nsqd_port = nsqd_port
        self.topic = topic
        self.channel = channel
        #kPI need accound id
        self.my_account_id = account_id()  
    
    def message_handler(self, message):
        try:
            
            data = json.loads(message.body.decode('utf-8'))
            #print(data)
            #NSQ all data's accouunt id
            account_id = data.get("account_id")
            if account_id == self.my_account_id:
                datas=json.dumps(data)
                print(f"Received message with account ID {self.my_account_id}: {datas}")
                message.finish()  # Acknowledge message handling
            else:
                #print(f"Message ignored, account ID {account_id} does not match target {self.my_account_id}")
                message.finish()  # Acknowledge the message without processing
        except Exception as e:
            print(f"Failed to process message: {e}")
            message.requeue()

    def consume_from_nsq(self):
        try:
            # Setup the NSQ reader
            nsq.Reader(
                message_handler=self.message_handler,
                nsqd_tcp_addresses=[f'{self.nsqd_host}:{self.nsqd_port}'],
                topic=self.topic,
                channel=self.channel,
                lookupd_poll_interval=15
            )
            tornado.ioloop.IOLoop.instance().start()
        except Exception as e:
            print(f"Failed to start NSQ consumer: {e}")
