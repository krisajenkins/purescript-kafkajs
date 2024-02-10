
export const makeProducerImpl = (kafka, producerConfig) => kafka.producer(producerConfig)

export const connectImpl = (producer) => () => producer.connect()

export const sendImpl = (producer, data) => () => producer.send(data)

export const disconnectImpl = (producer) => () => producer.disconnect()
