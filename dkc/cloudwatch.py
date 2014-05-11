import boto
import boto.exception
import boto.ec2.cloudwatch
import datetime

from config import get_global_option, get_logging_option
from dkc.logger import get_logger
import pytz


class Cloudwatch(object):
    __conn = None

    def __init__(self, stream, namespace='AWS/Kinesis'):
        self.dimensions = {'StreamName': stream}
        self.namespace = namespace
        self.logger = get_logger(self, get_logging_option('level'))
        self.connect()

    def connect(self):
        self.__conn = boto.ec2.cloudwatch.connect_to_region(get_global_option('region'))
        if not self.__conn:
            raise ConnectionException

    def valid_period(self, period):
        if period <= 0:
            self.logger.debug('Period is less or equal to zero. Setting it to one minute')
            period = 60

        if period % 60 != 0:
            wrong_period = period
            self.logger.debug('Period must be multiply of 60. Fixing')
            period = wrong_period - (wrong_period % 60)
            self.logger.debug('Fixed period to [%s] from [%s]' % (period, wrong_period))

        return period

    def calculate_time(self, period):
        # Period must be a multiply of 60
        end_time = datetime.datetime.now(pytz.utc)
        start_time = end_time - datetime.timedelta(seconds=period)
        return start_time, end_time

    def get_stream_metrics(self):
        try:
            return self.__conn.list_metrics(dimensions=self.dimensions, namespace=self.namespace)
        except boto.exception.BotoServerError, e:
            self.logger.error('%s Could not get metrics %s' % (e.reason, e.message))
            return

    def get_metric(self, period, start_time, end_time, metric_name, statistics):
        try:
            return self.__conn.get_metric_statistics(period,
                                                     start_time,
                                                     end_time,
                                                     metric_name,
                                                     namespace=self.namespace,
                                                     statistics=statistics,
                                                     dimensions=self.dimensions,
                                                     unit=None)
        except boto.exception.BotoServerError, e:
            self.logger.error('%s Could not get metric %s' % (e.reason, e.message))
            return

    def get_output_latency(self, period, statistics='Average'):
        metric_name = 'PutRecord.Latency'

        period = self.valid_period(period)
        start_time, end_time = self.calculate_time(period)
        return self.get_metric(period, start_time, end_time, metric_name, statistics)

    def get_input_latency(self, period, statistics='Average'):
        metric_name = 'GetRecords.Latency'

        period = self.valid_period(period)
        start_time, end_time = self.calculate_time(period)
        return self.get_metric(period, start_time, end_time, metric_name, statistics)

    def get_input_bytes(self, period, statistics='Average'):
        metric_name = 'PutRecord.Bytes'

        period = self.valid_period(period)
        start_time, end_time = self.calculate_time(period)

        metric = self.get_metric(period, start_time, end_time, metric_name, statistics)

        if not metric:
            return -1

        return metric[0].get(statistics)

    def get_output_bytes(self, period, statistics='Average'):
        metric_name = 'GetRecords.Bytes'

        period = self.valid_period(period)
        start_time, end_time = self.calculate_time(period)

        metric = self.get_metric(period, start_time, end_time, metric_name, statistics)

        if not metric:
            return -1

        return metric[0].get(statistics)


class ConnectionException(Exception):
    "Connection to Cloudwatch failed"
