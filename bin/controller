from dkc.cloudwatch import Cloudwatch
from dkc.kinesis import Kinesis
from dkc.logger import get_logger
from dkc.config import get_logging_option, get_kinesis_option, get_global_option
import time

class Controller(object):
    def __init__(self, kinesis, cloudwatch):
        self.logger = get_logger(self, get_logging_option('level'))
        self.kinesis = kinesis
        self.cloudwatch = cloudwatch
        self.input_hwm = get_kinesis_option('input_hwm')
        self.output_hwm = get_kinesis_option('output_hwm')
        self.check_interval = get_global_option('check_interval')
        self.input_per_shard = get_kinesis_option('input_per_shard')
        self.output_per_shard = get_kinesis_option('output_per_shard')
        self.shards = self.kinesis.get_shards()

    @property
    def total_input_capacity(self):
        return self.kinesis.get_shards_count() * self.input_per_shard

    @property
    def total_output_capacity(self):
        return self.kinesis.get_shards_count() * self.output_per_shard

    @property
    def is_input_hwm(self):
        if self.get_input_capacity_percentage() >= self.input_hwm:
            self.logger.debug('Input capacity is at %s\% starting to split shards')
            return True
        return False

    @property
    def is_input_lwm(self):
        if self.get_output_capacity_percentage() <= self.output_lwm:
            self.logger.debug('Input capacity is at %s\% starting to merge shards')
            return True
        return False

    @property
    def is_output_hwm(self):
        if self.get_output_capacity_percentage() >= self.output_hwm:
            self.logger.debug('Input capacity is at %s\% starting to split shards')
            return True
        return False

    @property
    def is_output_lwm(self):
        if self.get_input_capacity_percentage() <= self.input_lwm:
            self.logger.debug('Input capacity is at %s\% starting to merge shards')
            return True
        return False

    def get_input_capacity_percentage(self):
        input_bytes = self.cloudwatch.get_input_bytes(180)
        return (input_bytes/self.total_input_capacity) * 100

    def get_output_capacity_percentage(self):
        output_bytes = self.cloudwatch.get_output_bytes(180)
        return (output_bytes/self.total_output_capacity) * 100

    def should_split(self):
        if self.is_input_hwm or self.is_output_hwm:
            return True
        return False

    def should_merge(self):
        if self.is_input_lwm or self.is_output_lwm:
            return True
        return False

    def split_biggest_shard(self):
        biggest_shard = self.kinesis.get_biggest_shard()
        self.kinesis.stream.split_shard(biggest_shard)

    def merge_smallest_shard(self):
        shard = self.kinesis.get_smallest_shard()
        shards = self.kinesis.get_adjacent_shard(shard)
        self.kinesis.stream.merge_shards(shards)

    def start(self):
        while True:
            if self.should_split():
                self.logger.debug('Spliting shard')
                self.split_biggest_shard()
            elif self.should_merge():
                self.logger.debug('Merging shards')
                self.merge_smallest_shard()

            self.logger.debug('Sleeping for one minute')
            time.sleep(self.check_interval)


def run(stream):
    cloudwatch = Cloudwatch(stream)
    kinesis = Kinesis(stream)
    controller = Controller(kinesis, cloudwatch)

    print kinesis.get_biggest_shard()
    print kinesis.get_shards()
    print kinesis.get_shards_count()
    controller.split_biggest_shard()


def main():
    import sys

    stream = 'Test'
    run(stream)

    # sys.exit(run(stream))


if __name__ == '__main__':
    main()