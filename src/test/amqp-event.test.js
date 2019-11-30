const { describe, it } = require('mocha');
const assert = require('assert');
const AMQPEvent = require('../amqp-event');

describe('AMQP Event', () => {
  it('emit and on event', (done) => {
    const producerServiceName = 'producer-service';
    const consumerServiceName = 'consumer-service';
    const messagePublishedEventName = 'message_published';
    const message = 'Message published!';
    const url = 'amqp://root:root@localhost';

    let producer = new AMQPEvent({
      url,
      exchange: producerServiceName
    });

    let consumer = new AMQPEvent({
      url,
      exchange: producerServiceName,
      consumer: consumerServiceName
    });

    consumer.register(messagePublishedEventName, { autoDelete: true })
      .then(empty => {
        return consumer.on(messagePublishedEventName, (msg) => {
          assert.equal(msg, message);
          done();
        });
      })
      .then(empty => {
        return producer.emit(messagePublishedEventName, message);
      })
      .catch(done);
  });
});