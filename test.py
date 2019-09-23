import unittest
import os
import re
import time
from queue import Queue

from main import ConsumerThread, LOG_FILENAME, get_logger


class ExampleTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger()

    def setUp(self):
        self.queue = Queue(2)

    def tearDown(self):
        pass

    def testValidUrl(self):
        test_input = 'http://google.com'
        self.queue.put(test_input)
        consumer = ConsumerThread(self.queue, self.logger)
        consumer.start()
        while not self.queue.empty():
            time.sleep(0.5)
        # Wait for file creations
        time.sleep(2)

        # Parse log
        log = open(LOG_FILENAME, 'r')
        last_line = log.readlines()[-1]
        log.close()
        self.assertRegex(last_line, 'Consumed http.*?\.result file')

    def testResultFile(self):
        test_input = 'http://google.com'
        self.queue.put(test_input)
        consumer = ConsumerThread(self.queue, self.logger)
        consumer.start()
        while not self.queue.empty():
            time.sleep(0.5)
        # Wait for file creations
        time.sleep(2)

        # Parse log
        log = open(LOG_FILENAME, 'r')
        last_line = log.readlines()[-1]
        match = re.findall('Consumed http.*? into (.*?\.result) file', last_line)
        log.close()
        self.assertTrue(len(match), "The expected log message couldn't be found!")
        self.assertTrue(os.path.exists(match[0]), "The result file not exist!")

    def testWrongScheme(self):
        test_input = ''
        self.queue.put(test_input)
        consumer = ConsumerThread(self.queue, self.logger)
        consumer.start()
        while not self.queue.empty():
            time.sleep(0.5)
        # Wait for file creations
        time.sleep(2)

        # Parse log
        log = open(LOG_FILENAME, 'r')
        last_line = log.readlines()[-1]
        match = re.findall('MissingSchema', last_line)
        log.close()
        self.assertTrue(len(match), "The expected log message couldn't be found!")

    def testWrongScheme2(self):
        test_input = 'xyz'
        self.queue.put(test_input)
        consumer = ConsumerThread(self.queue, self.logger)
        consumer.start()
        while not self.queue.empty():
            time.sleep(0.5)
        # Wait for file creations
        time.sleep(2)

        # Parse log
        log = open(LOG_FILENAME, 'r')
        last_line = log.readlines()[-1]
        match = re.findall('MissingSchema', last_line)
        log.close()
        self.assertTrue(len(match), "The expected log message couldn't be found!")

    def testConnectionError(self):
        test_input = 'http://google.wrong.url.couldnotconnect.to.com'
        self.queue.put(test_input)
        consumer = ConsumerThread(self.queue, self.logger)
        consumer.start()
        while not self.queue.empty():
            time.sleep(0.5)
        # Wait for file creations
        time.sleep(2)

        # Parse log
        log = open(LOG_FILENAME, 'r')
        last_line = log.readlines()[-1]
        match = re.findall('ConnectionError', last_line)
        log.close()
        self.assertTrue(len(match), "The expected log message couldn't be found!")


