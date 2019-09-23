import os
import time
import logging
import threading
import uuid
from queue import Queue
from urllib.parse import urlparse

# Third party
import requests
from requests.exceptions import MissingSchema, ConnectionError
from urllib3.exceptions import NewConnectionError

import lxml.html
from slugify import slugify


# -------------
# -- CONFIGS --
# -------------

LOG_FILENAME = 'parser.log'
WORKER_LIMIT = 5
QUEUE_LIMIT = 100


def get_logger():
    # Clean-up log
    if os.path.exists(LOG_FILENAME):
        os.remove(LOG_FILENAME)

    # create a file handler
    handler = logging.FileHandler(LOG_FILENAME)
    handler.setLevel(logging.DEBUG)
    # global logger
    logger = logging.getLogger('main')
    logger.setLevel(logging.DEBUG)
    format = "%(asctime)s: %(message)s"
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    return logger


class ProducerThread(threading.Thread):
    def __init__(self, queue, logger):
        super(ProducerThread, self).__init__()
        self.queue = queue
        self.logger = logger

    def run(self):
        url_file = open("input.txt", "r")
        for line in url_file:
            parsed = urlparse(line.strip())
            self.queue.put(parsed.geturl())
            self.logger.info("Produced {}".format(parsed))
        return


class ConsumerThread(threading.Thread):
    def __init__(self, queue, logger, stand_by=False):
        super(ConsumerThread, self).__init__()
        self.queue = queue
        self.logger = logger
        self.stand_by = stand_by

    def run(self):
        if self.queue.empty():
            # Wait some time to get first queue element if needed
            time.sleep(0.5)
        while not self.queue.empty() or self.stand_by:
            url = self.queue.get()
            # Get html with requests module
            try:
                response = requests.get(url)
                filename = self.save_urls(response.content, url)
                self.logger.info("Consumed {} into {} file".format(url, filename))
                self.queue.task_done()

            except MissingSchema:
                # Do some log if needed
                # Incorrect link scheme
                self.logger.error('MissingSchema')
                self.queue.task_done()
            except ConnectionError:
                # Do some log if needed
                # Couldn't connect
                self.logger.error('ConnectionError')
                self.queue.task_done()
            except Exception as e:
                # Do some log if needed
                self.logger.error('Unknown Exception')
                self.logger.error(e)
                self.queue.task_done()

    def extract(self, content):
        links = []
        dom = lxml.html.fromstring(content)
        for link in dom.xpath('//a/@href'):
            links.append(link)
        return links

    def save_urls(self, html, url):
        filename = '{}_{}.result'.format(uuid.uuid4(), slugify(url))
        fw = open(filename, "w")
        links = self.extract(html)

        for link in links:
            # Save only links wich start with http (https will be saved this way)
            # Since there were no specific requirements what kind of links should we save, I will save only exact links
            if link.startswith('http'):
                fw.write(link)
                fw.write("\n")
        fw.close()
        return filename


if __name__ == '__main__':

    queue = Queue(QUEUE_LIMIT)  # Add limit to avoid much memory consumption
    logger = get_logger()

    producer = ProducerThread(queue, logger)

    threads = [
        producer.start(),
    ]
    for i in range(WORKER_LIMIT):
        consumer = ConsumerThread(queue, logger)
        threads.append(consumer.start())



