module Kafka.Types
  ( Offset(..)
  , Partition(..)
  , Topic(..)
  ) where

import Data.Eq (class Eq)
import Data.Newtype (class Newtype)
import Data.Ord (class Ord)
import Data.Show (class Show)

newtype Offset
  = Offset Int

instance ntOffset :: Newtype Offset Int

derive newtype instance showOffset :: Show Offset

derive newtype instance eqOffset :: Eq Offset

derive newtype instance ordOffset :: Ord Offset

newtype Partition
  = Partition Int

instance ntPartiton :: Newtype Partition Int

derive newtype instance showPartition :: Show Partition

derive newtype instance eqPartition :: Eq Partition

derive newtype instance ordPartition :: Ord Partition

newtype Topic
  = Topic String

instance ntTopic :: Newtype Topic String

derive newtype instance showTopic :: Show Topic

derive newtype instance eqTopic :: Eq Topic

derive newtype instance ordTopic :: Ord Topic
