from confluent_kafka import Producer, Consumer, KafkaError


class KafkaClient:
    def __init__(self, brokers):
        self.brokers = brokers

    def produce(self, topic, key, value):
        producer = Producer({'bootstrap.servers': self.brokers})

        def delivery_report(err, msg):
            if err is not None:
                print('Message delivery failed: {}'.format(err))
            else:
                print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.flush()  # Ensure all messages are sent out

    def consume(self, topic, group_id, process_message_func, auto_offset_reset='earliest'):
        consumer = Consumer({
            'bootstrap.servers': self.brokers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset
        })

        consumer.subscribe([topic])

        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                process_message_func(msg)

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


# Utility function to process consumed messages
def process_message(msg):
    print(f'Received message: {msg.value().decode("utf-8")}')


# Usage
if __name__ == '__main__':
    kafka_client = KafkaClient('localhost:9092')

    # To produce messages
    kafka_client.produce('your_topic', 'your_key', 'your_value')

    # To consume messages
    kafka_client.consume('your_topic', 'your_group_id', process_message)