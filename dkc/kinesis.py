import boto.kinesis
import boto.exception

from functools import total_ordering
from dkc.logger import get_logger
from dkc.config import get_global_option, get_logging_option
import time


@total_ordering
class Shard(object):
    """
        Amazon Shard represantation
    """
    def __init__(self, shard):
        self.shard_id = str(shard.get('ShardId'))
        self.starting_hash = long(shard.get('HashKeyRange').get('StartingHashKey'))
        self.ending_hash = long(shard.get('HashKeyRange').get('EndingHashKey'))
        self.hash_range = self.ending_hash - self.starting_hash
        self.parent_shard_id = shard.get('ParentShardId')
        self.children = set()

    def __repr__(self):
        return repr('<Shard>: %s' % self.shard_id)

    def __lt__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.hash_range < other.hash_range

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.hash_range == other.hash_range

    def add_child(self, child_shard):
        self.children.add(child_shard)

    def is_parent(self):
        return bool(self.children)

    def split(self):
        """
         Split the shard to two. Do not actually splits the shards but calculates the ranges of new shards.
        """
        new_shard_hash_range = self.hash_range / 2
        start = self.starting_hash
        return str(start + new_shard_hash_range + 1)


class Stream(object):
    """
        Amazon Stream wrapper API
    """
    __conn = None

    def __init__(self, stream):
        self.name = stream
        self.logger = get_logger(self, get_logging_option('level'))

        if not self.is_connected():
            self.connect()

        self._stream = self.__conn.describe_stream(self.name)
        self._shards = list(Shard(s) for s in self._stream.get('StreamDescription').get('Shards'))

    def __len__(self):
        return len(self._shards)

    def __iter__(self):
        return (shard for shard in self._shards)

    def __repr__(self):
        return repr('<Stream>: %s' % self.name)

    def get_status(self):
        stream = self.__conn.describe_stream(self.name)
        return stream.get('StreamDescription').get('StreamStatus')

    def is_connected(self):
        return bool(self.__conn)

    def connect(self):
        self.logger.debug("Connecting to kinesis")
        self.__conn = boto.kinesis.connect_to_region(get_global_option('region'))
        if not self.is_connected():
            self.logger.error('Could not connect to Kinesis')
            raise ConnectionException
        self.logger.debug("Succesfully conected")

    def split_shard(self, shard):
        """
            Accepts list of shards. Each shard must be
            http://boto.readthedocs.org/en/latest/ref/kinesis.html?highlight=kinesis#module-boto.kinesis

            Based on that, we wait until the stream is ready.
        """

        if shard.is_parent():
            self.logger.error('Trying to split a parent shard')
            return

        if self.get_status() == 'ACTIVE':
            try:
                self.__conn.split_shard(self.name, shard.shard_id, shard.split())
                self.logger.debug('Splitted shard')
            except Exception as e:
                self.logger.debug('Could not split shard: {}'.format(e))
        else:
            self.logger.debug('Sleeping for 60 seconds. %s' % self.get_status())
            time.sleep(60)

    def merge_shards(self, shards):
        if self.get_status() == 'ACTIVE':
            try:
                self.__conn.merge_shards(self.name, *shards)
                self.logger.debug('Merged shards')
            except Exception as e:
                self.logger.debug('Could not merge shards: %s' % (e))

        else:
            self.logger.debug('Sleeping for 60 seconds. %s' % self.get_status())
            time.sleep(60)

    def get_shard(self, shard_id):
        for s in self:
            if s.shard_id == shard_id:
                return s

    def update_shards(self):
        for s in self:
            if s.parent_shard_id:
                parent_shard = self.get_shard(s.parent_shard_id)
                parent_shard.add_child(s)


class Kinesis(object):
    """
        Kinesis as a service API
    """
    __conn = None

    def __init__(self, stream_name):
        self.stream = Stream(stream_name)
        self.logger = get_logger(self, get_logging_option('level'))

    def get_biggest_shard(self):
        return max(self.get_shards())

    def get_smallest_shard(self):
        return min(self.get_shards())

    def get_adjacent_shard(self, shard):
        for s in self.stream:
            if s.starting_hash - 1 == shard.ending_hash:
                return shard.shard_id, s.shard_id
            if s.ending_hash + 1 == shard.starting_hash:
                return s.shard_id, shard.shard_id
        return

    def get_shards(self):
        for s in self.stream:
            if not s.is_parent():
                yield s

    def get_shards_count(self):
        return len(self.stream)


class ConnectionException(Exception):
    "Connection to Kinesis failed"

if __name__ == '__main__':
    stream = 'Test'
    kinesis = Kinesis(stream)
    kinesis.stream.update_shards()

    smallest_shard = kinesis.get_smallest_shard()
    shards = kinesis.get_adjacent_shard(smallest_shard)

    print kinesis.stream.merge_shards(shards)

    # print kinesis.stream.split_shard(kinesis.get_biggest_shard())