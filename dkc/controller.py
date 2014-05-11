from dkc.cloudwatch import Cloudwatch
from dkc.kinesis import Kinesis
from dkc.logger import get_logger
import time

class Controller(object):
    def __init__(self, kinesis, cloudwatch,
                 input_per_shard=None,
                 output_per_shard=None,
                 input_hwm='75',
                 output_hwm='75',
                 check_interval=60,
                 log_level='DEBUG'):
        self.logger = get_logger(self, log_level)
        self.kinesis = kinesis
        self.cloudwatch = cloudwatch
        self.input_hwm = input_hwm
        self.output_hwm = output_hwm
        self.check_interval = check_interval
        self.shards = self.kinesis.get_shards()

        if not input_per_shard:
            self.input_per_shard = 1024 * 1024
        else:
            self.input_per_shard = input_per_shard

        if not output_per_shard:
            self.output_per_shard = 1024 * 1024 * 2
        else:
            self.output_per_shard = output_per_shard

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

    def is_output_hwm(self):
        if self.get_output_capacity_percentage() >= self.output_hwm:
            self.logger.debug('Input capacity is at %s\% starting to split shards')
            return True
        return False

    def get_input_capacity_percentage(self):
        input_bytes = self.cloudwatch.get_input_bytes(180)
        return (input_bytes/self.total_input_capacity) * 100

    def get_output_capacity_percentage(self):
        output_bytes = self.cloudwatch.get_output_bytes(180)
        return (output_bytes/self.total_output_capacity) * 100

    def critical_state(self):
        if self.is_input_hwm or self.is_output_hwm:
            return True
        return False

    def split_biggest_shard(self):
        biggest_shard = self.kinesis.get_biggest_shard()
        self.kinesis.stream.split_shard(biggest_shard)

    def start(self):
        while True:
            if self.critical_state():
                self.split_biggest_shard()

            time.sleep(self.check_interval)


def run(stream):
    cloudwatch = Cloudwatch(stream)
    kinesis = Kinesis(stream)
    controller = Controller(kinesis, cloudwatch)
    controller.start()


def main():
    import sys
    import argparse
    stream = 'Events'

    sys.exit(run(stream))


if __name__ == '__main__':
    main()