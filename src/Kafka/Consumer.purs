module Kafka.Consumer
  ( Consumer
  , GroupId(..)
  , EachBatchAutoResolve(..)
  , AutoCommit(..)
  , ConsumerConfig
  , SubscriptionConfig
  , makeConsumer
  , connect
  , subscribe
  , ConsumerMessage
  , ConsumerBatch
  , Heartbeat
  , ResolveOffset
  , CommitOffsetsIfNecessary
  , OffsetInfo
  , toOffsetInfo
  , UncommittedOffsets
  , IsStale
  , IsRunning
  , EachBatch
  , eachBatch
  , disconnect
  ) where

import Kafka.Types
import Control.Applicative (pure)
import Control.Monad.Error.Class (throwError)
import Control.Promise (Promise, toAffE, fromAff)
import Data.Either (Either, either, note)
import Data.Function.Uncurried (Fn2, Fn4, Fn7, mkFn7, runFn2, runFn4)
import Data.Int (fromString)
import Data.Maybe (Maybe, maybe)
import Data.Newtype (class Newtype, un, unwrap)
import Data.Nullable (Nullable, toMaybe)
import Data.Show (class Show)
import Data.Traversable (traverse)
import Effect (Effect)
import Effect.Aff (Aff)
import Effect.Exception (error)
import Foreign.Object (Object)
import Kafka.Kafka (Kafka)
import Node.Buffer (Buffer)
import Prelude (Unit, bind, map, (#), ($), (>>>))

foreign import data Consumer :: Type

newtype GroupId
  = GroupId String

instance ntGroupId :: Newtype GroupId String

derive newtype instance showGroupId :: Show GroupId

newtype EachBatchAutoResolve
  = EachBatchAutoResolve Boolean

instance ntEachBatchAutoResolve :: Newtype EachBatchAutoResolve Boolean

newtype AutoCommit
  = AutoCommit Boolean

instance ntAutoCommit :: Newtype AutoCommit Boolean

type ConsumerConfig
  = { groupId :: GroupId
    , readUncommitted :: Boolean
    , autoCommit :: Boolean
    }

type InternalSubscriptionConfig
  = { topics :: Array String
    , fromBeginning :: Boolean
    }

type SubscriptionConfig
  = { topics :: Array Topic
    , fromBeginning :: Boolean
    }

fromSubscriptionConfig :: SubscriptionConfig -> InternalSubscriptionConfig
fromSubscriptionConfig { topics, fromBeginning } =
  { topics: map unwrap topics
  , fromBeginning
  }

foreign import makeConsumerImpl :: Fn2 Kafka ConsumerConfig Consumer

makeConsumer :: Kafka -> ConsumerConfig -> Consumer
makeConsumer = runFn2 makeConsumerImpl

foreign import connectImpl :: Consumer -> Effect (Promise Unit)

connect :: Consumer -> Aff Unit
connect = connectImpl >>> toAffE

foreign import subscribeImpl :: Fn2 Consumer InternalSubscriptionConfig (Effect (Promise Unit))

subscribe :: Consumer -> SubscriptionConfig -> Aff Unit
subscribe consumer subscriptionConfig = runFn2 subscribeImpl consumer (fromSubscriptionConfig subscriptionConfig) # toAffE

type InternalConsumerMessage
  = { key :: Nullable Buffer
    , value :: Buffer
    , offset :: String
    , headers :: Object (Nullable Buffer)
    }

type InternalConsumerBatch
  = { fetchedOffset :: String
    , highWatermark :: String
    , messages :: Array InternalConsumerMessage
    , partition :: Int
    , topic :: String
    }

type ConsumerMessage
  = { key :: Maybe Buffer
    , value :: Buffer
    , offset :: Offset
    , headers :: Object (Maybe Buffer)
    }

type ConsumerBatch
  = { fetchedOffset :: Offset
    , highWatermark :: Offset
    , messages :: Array ConsumerMessage
    , partition :: Partition
    , topic :: Topic
    }

type InternalHeartbeat
  = Effect (Promise Unit)

type Heartbeat
  = Aff Unit

type InternalResolveOffset
  = Int -> Effect Unit

type ResolveOffset
  = Offset -> Effect Unit

type InternalCommitOffsetsIfNecessary
  = Effect (Promise Unit)

type CommitOffsetsIfNecessary
  = Aff Unit

type InternalOffsetInfo
  = { topics ::
        Array
          { partitions ::
              Array
                { offset :: String
                , partition :: String
                }
          , topic :: String
          }
    }

type InternalUncommittedOffsets
  = Effect InternalOffsetInfo

type OffsetInfo
  = { topics ::
        Array
          { partitions ::
              Array
                { offset :: Offset
                , partition :: Partition
                }
          , topic :: Topic
          }
    }

raiseMaybe :: forall a. Maybe a -> Effect a
raiseMaybe = maybe (throwError $ error "Maybe is Nothing") pure

raiseEither :: forall a. Either String a -> Effect a
raiseEither = either (error >>> throwError) pure

toOffsetInfo :: InternalOffsetInfo -> Effect OffsetInfo
toOffsetInfo { topics } = do
  topicInfo <- traverse parseTopicInfo topics
  pure { topics: topicInfo }
  where
  parsePartitionInfo ::
    { offset :: String
    , partition :: String
    } ->
    Effect
      { offset :: Offset
      , partition :: Partition
      }
  parsePartitionInfo { offset, partition } = do
    o <- raiseMaybe $ fromString offset
    p <- raiseMaybe $ fromString partition
    pure { offset: Offset o, partition: Partition p }

  parseTopicInfo ::
    { partitions ::
        Array
          { offset :: String
          , partition :: String
          }
    , topic :: String
    } ->
    Effect
      { partitions ::
          Array
            { offset :: Offset
            , partition :: Partition
            }
      , topic :: Topic
      }
  parseTopicInfo { partitions, topic } = do
    partitionInfo <- traverse parsePartitionInfo partitions
    pure { partitions: partitionInfo, topic: Topic topic }

type UncommittedOffsets
  = Effect OffsetInfo

type InternalIsStale
  = Effect Boolean

type IsStale
  = Effect Boolean

type InternalIsRunning
  = Effect Boolean

type IsRunning
  = Effect Boolean

type EachBatchImpl
  = Fn7
      InternalConsumerBatch
      InternalResolveOffset
      InternalHeartbeat
      InternalCommitOffsetsIfNecessary
      InternalUncommittedOffsets
      InternalIsRunning
      InternalIsStale
      (Effect (Promise Unit))

type EachBatch
  = ConsumerBatch ->
    ResolveOffset ->
    Heartbeat ->
    CommitOffsetsIfNecessary ->
    UncommittedOffsets ->
    IsRunning ->
    IsStale ->
    (Aff Unit)

toConsumerMessage :: InternalConsumerMessage -> Either String ConsumerMessage
toConsumerMessage { key, value, offset, headers } = do
  o <- note "offset is not a number" $ fromString offset
  pure
    { key: toMaybe key
    , value: value
    , offset: Offset o
    , headers: map toMaybe headers
    }

toConsumerBatch :: InternalConsumerBatch -> Either String ConsumerBatch
toConsumerBatch { fetchedOffset, highWatermark, messages, partition, topic } = do
  fo <- note "fetchedOffset is not a number" $ fromString fetchedOffset
  hwm <- note "highWatermark is not a number" $ fromString highWatermark
  msgs <- traverse toConsumerMessage messages
  pure
    { fetchedOffset: Offset fo
    , highWatermark: Offset hwm
    , messages: msgs
    , partition: Partition partition
    , topic: Topic topic
    }

foreign import eachBatchImpl :: Fn4 Consumer Boolean Boolean EachBatchImpl (Effect (Promise Unit))

eachBatch :: Consumer -> EachBatchAutoResolve -> AutoCommit -> EachBatch -> Aff Unit
eachBatch consumer eachBatchAutoResolve autoCommit handler =
  let
    iResolveOffset :: (Int -> Effect Unit) -> (Offset -> Effect Unit)
    iResolveOffset f = un Offset >>> f

    iUncommitedOffsets :: Effect InternalOffsetInfo -> Effect OffsetInfo
    iUncommitedOffsets ioiEff = do
      ioi <- ioiEff
      toOffsetInfo ioi

    internalHandlerCurried ::
      InternalConsumerBatch ->
      InternalResolveOffset ->
      InternalHeartbeat ->
      InternalCommitOffsetsIfNecessary ->
      InternalUncommittedOffsets ->
      InternalIsRunning ->
      InternalIsStale ->
      (Effect (Promise Unit))
    internalHandlerCurried icb iro ih icoin iuo iir iis = do
      batch <- raiseEither (toConsumerBatch icb)
      handler
        batch
        (iResolveOffset iro)
        (toAffE ih)
        (toAffE icoin)
        (iUncommitedOffsets iuo)
        (iir)
        (iis)
        # fromAff

    internalHandler :: EachBatchImpl
    internalHandler = mkFn7 internalHandlerCurried
  in
    runFn4 eachBatchImpl consumer
      (unwrap eachBatchAutoResolve)
      (unwrap autoCommit)
      internalHandler
      # toAffE

foreign import disconnectImpl :: Consumer -> Effect (Promise Unit)

disconnect :: Consumer -> Aff Unit
disconnect = disconnectImpl >>> toAffE
