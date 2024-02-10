

export const transactionImpl = (producer) => () => producer.transaction()

export const sendImpl = (transaction, data) => () => transaction.send(data)

export const sendOffsetsImpl = (transaction, sendOffsets) => () => transaction.sendOffsets(sendOffsets)

export const commitImpl = (transaction) => () => transaction.commit()

export const abortImpl = (transaction) => () => transaction.abort()
