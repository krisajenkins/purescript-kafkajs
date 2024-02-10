import { Kafka, logLevel } from 'kafkajs';

export const makeClientImpl = params => new Kafka(params)

export const internalLogNothing = logLevel.NOTHING
export const internalLogDebug = logLevel.DEBUG
export const internalLogInfo = logLevel.INFO
export const internalLogWarn = logLevel.WARN
export const internalLogError = logLevel.ERROR
