# coding: utf-8

# src.ActiveMQ
# by Rafael Fogel
# created on 01/03/2013

import pyactivemq
from pyactivemq import AcknowledgeMode
from threading import Event

class DurableSubscriber:
    class TextListener(pyactivemq.MessageListener):
        def __init__(self):
            pyactivemq.MessageListener.__init__(self)
            self.monitor = Event()

        def onMessage(self, message):
            if isinstance(message, pyactivemq.TextMessage):
                print 'SUBSCRIBER: Reading Message: ' + message.text
            else:
                self.monitor.set()

    def __init__(self, connectionFactory, topicName):
        try:
            connection = connectionFactory.createConnection()
        except Exception as ex:
            print ex
        # XXX should throw without this
        #connection.clientID = 'client1234'
        session = connection.createSession(AcknowledgeMode.AUTO_ACKNOWLEDGE)
        topic = session.createTopic(topicName)
        self.connection = connection 
        self.session = session
        self.topic = topic

    def startSubscriber(self):
        print 'Starting subscriber'
        self.connection.stop()
        subscriber = self.session.createDurableConsumer(self.topic, "MakeItLast", '', False)
        listener = self.TextListener()
        subscriber.messageListener = listener
        self.subscriber = subscriber
        self.connection.start()

    def closeSubscriber(self):
        subscriber = self.subscriber
        listener = subscriber.messageListener
        listener.monitor.wait()
        print 'Closing subscriber'
        subscriber.close()

    def finish(self):
        self.session.unsubscribe("MakeItLast")
        self.connection.close()

class MultiplePublisher:
    def __init__(self, connectionFactory, topicName):
        connection = connectionFactory.createConnection()
        session = connection.createSession(AcknowledgeMode.AUTO_ACKNOWLEDGE)
        topic = session.createTopic(topicName)
        producer = session.createProducer(topic)
        self.connection = connection
        self.session = session
        self.producer = producer
        self.startindex = 0

    def publishMessages(self):
        NUMMSGS = 3
        MSG_TEXT = 'Here is a message'
        message = self.session.createTextMessage()
        for i in xrange(self.startindex, self.startindex + NUMMSGS):
            message.text = MSG_TEXT + ' %d' % (i+1)
            print 'PUBLISHER: Publishing message: ' + message.text
            self.producer.send(message)
        self.startindex = self.startindex + NUMMSGS
        self.producer.send(self.session.createMessage())

    def finish(self):
        self.connection.close()

def main():
    url = 'tcp://localhost:61616'
    topicName = 'topic-1234'
    f = pyactivemq.ActiveMQConnectionFactory(url)
    multiplePublisher = MultiplePublisher(f, topicName)
    durableSubscriber = DurableSubscriber(f, topicName) 
    durableSubscriber.startSubscriber()
    multiplePublisher.publishMessages()
    durableSubscriber.closeSubscriber()
    multiplePublisher.publishMessages()
    durableSubscriber.startSubscriber()
    durableSubscriber.closeSubscriber()
    multiplePublisher.finish()
    durableSubscriber.finish()

if __name__ == '__main__':
    main()
    
    
    