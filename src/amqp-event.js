const amqp = require('amqplib');
const defaultOptions = {
  prefix: 'amqpe',
  exchange: 'amqpe',
  durable: true
};

class AMQPEvent {
  /**
   *
   * @param {object} options
   * @param {string} [options.url]
   * @param {string} [options.protocol] The to be used protocol.  Default value: 'amqp'.
   * @param {string} [options.hostname] Hostname used for connecting to the server. Default value: 'localhost'.
   * @param {number} [options.port] Port used for connecting to the server. Default value: 5672.number.
   * @param {string} [options.username] Username used for authenticating against the server. Default value: 'guest'.
   * @param {string} [options.password] Password used for authenticating against the server. Default value: 'guest'.
   * @param {string} [options.prefix] Default value: 'amqpe'.
   * @param {string} [options.exchange] Is the name of the exchange - It like a domain that the events belong to.
   * @param {boolean} [options.durable]
   * @param {string} [options.consumer]
   */
  constructor(options) {
    this.options = { ...defaultOptions, ...options };
    this.connection;
    this.channel;
    this.exchange;
    this.exchangeName = '';
  }

  /**
   *
   * @param {string} event
   * @param {Function} listenner
   * @param {object} options
   * @param {boolean} [options.noAck]
   *
   */
  async on(event, listenner, options) {
    const defaultOptions = {
      noAck: true,
    };

    await this.channel.consume(
      this.makeQueueName(event), (msg) => {
        listenner(msg.content.toString());
        msg.content.toString();
      },
      { ...defaultOptions, ...options }
    );
  }


  /**
   *
   * @param {string} event
   * @param {object} [options]
   * @param {boolean} [options.autoDelete]
   * @param {boolean} [options.durable]
   * @param {number} [options.messageTtl]
   */
  async register(event, options) {
    options = options || {};

    await this.initialize();

    const defaultOptions = {
      autoDelete: false,
      durable: true,
      messageTtl: 1000 * 60 * 60 * 6 //6h
    };

    const { queue } = await this.channel.assertQueue(
      this.makeQueueName(event),
      { ...defaultOptions, ...options }
    );

    const routingKey = this.makeEventRoutingKey(event, this.options.exchange);

    return await this.channel.bindQueue(queue, this.exchangeName, routingKey);
  }

  /**
   *
   * @param {string} event
   */
  makeQueueName(event) {
    return `${this.options.prefix}.${this.options.exchange}.${event}.${this.options.consumer}`;
  }

  /**
   * @param {string} event
   * @param {string|Buffer} msg
   */
  async emit(event, msg) {
    await this.initialize();

    const routingKey = this.makeEventRoutingKey(event);

    const isPublished = await this.channel.publish(
      this.exchangeName,
      routingKey,
      Buffer.from(msg),
      {
        persistent: true
      }
    );

    return isPublished;
  }

  /**
   * @param {string} event
   */
  makeEventRoutingKey(event) {
    return `${this.options.prefix}.${this.options.exchange}.emit.${event}`;
  }

  async initialize() {
    if (!this.connection) {
      this.connection = await amqp.connect(this.options.url || this.options);
    }

    if (!this.channel) {
      this.channel = await this.connection.createChannel();
    }

    if (!this.exchange) {
      this.exchangeName = `${this.options.prefix}.${this.options.exchange}`;

      this.exchange = await this.channel.assertExchange(
        this.exchangeName,
        'topic',
        {
          durable: this.options.durable
        }
      );
    }
  }
}

module.exports = AMQPEvent;