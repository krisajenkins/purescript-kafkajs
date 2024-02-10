

export const makeConsumerImpl = (kafka, consumerConfig) => kafka.consumer(consumerConfig)

export const connectImpl = (consumer) => () => consumer.connect()

export const subscribeImpl = (consumer, subscriptionConfig) => () => consumer.subscribe(subscriptionConfig)

export const eachBatchImpl = (consumer, eachBatchAutoResolve, autoCommit, handler) => () => {
    return consumer.run({
        autoCommit,
        eachBatchAutoResolve,
        eachBatch: ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            uncommittedOffsets,
            isRunning,
            isStale }) => {
              const resolveOffsetEff = offset => () => resolveOffset(offset);
              const commitOffsetsIfNecessaryEff = () => commitOffsetsIfNecessary();
              const uncommittedOffsetsEff = () => uncommittedOffsets();
              const isRunningEff = () =>  isRunning();
              const isStaleEff = () => isStale();

              return handler(batch, resolveOffsetEff, heartbeat, commitOffsetsIfNecessaryEff, uncommittedOffsetsEff, isRunningEff, isStaleEff)();
        }

    });
};

export const disconnectImpl = (consumer) => () => consumer.disconnect();
