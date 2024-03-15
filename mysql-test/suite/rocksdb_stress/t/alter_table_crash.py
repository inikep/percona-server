import MySQLdb
from MySQLdb.constants import CR
from MySQLdb.constants import ER
import os
import random
import signal
import sys
import threading
import time
import string
import traceback
import logging
import argparse
import array as arr

# global variable checked by threads to determine if the test is stopping
TEST_STOP = False

LOCKED_TABLE_IDS = set()
LOCK = threading.Lock()

def execute(cur, stmt):
  ROW_COUNT_ERROR = 18446744073709551615
  logging.debug("Executing %s" % stmt)
  cur.execute(stmt)
  if cur.rowcount < 0 or cur.rowcount == ROW_COUNT_ERROR:
    raise MySQLdb.OperationalError(MySQLdb.constants.CR.CONNECTION_ERROR,
                                   "Possible connection error, rowcount is %d"
                                   % cur.rowcount)

def wait_for_workers(workers):
  logging.info("Waiting for %d workers", len(workers))

  # polling here allows this thread to be responsive to keyboard interrupt
  # exceptions, otherwise a user hitting ctrl-c would see the load_generator as
  # hanging and unresponsive
  try:
    while threading.active_count() > 1:
      time.sleep(1)
  except KeyboardInterrupt as e:
    os._exit(1)

  num_failures = 0
  for w in workers:
    w.join()
    if w.exception:
      logging.error(w.exception)
      num_failures += 1

  return num_failures

class WorkerThread(threading.Thread):
    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self.con = None
        self.cur = None
        self.num_requests = OPTIONS.num_requests
        self.loop_num = 0
        self.exception = None
        self.table_id = thread_id

        self.start_time = time.time()
        self.total_time = 0

        self.start()

    def runme(self):
        global TEST_STOP

        self.con = MySQLdb.connect(user=OPTIONS.user, host=OPTIONS.host,
                           port=OPTIONS.port, db=OPTIONS.db)

        if not self.con:
            raise Exception("Unable to connect to mysqld server")

        self.con.autocommit(True)
        self.cur = self.con.cursor()

        table_id = self.table_id
        table_name = 'tbl%02d' % table_id

        while self.loop_num < self.num_requests and not TEST_STOP:
            stmt = ("CREATE TABLE %s ("
                    "id1 int unsigned NOT NULL,"
                    "id2 int unsigned NOT NULL,"
                    "PRIMARY KEY (id1)"
                    ") ENGINE=ROCKSDB" % (table_name))
            execute(self.cur, stmt)

            stmt = ("INSERT INTO %s VALUES (1, 1)" % table_name)
            #for value in range(2, 7):
            #    stmt += ", (%d, %d)" % (value, value)
            execute(self.cur, stmt)

            stmt = ("ALTER TABLE %s "
                    "ADD INDEX secondary_key (id2) " % (table_name))
            execute(self.cur, stmt)

            stmt = ("DROP TABLE %s" % table_name)
            execute(self.cur, stmt)

    def run(self):
        global TEST_STOP

        try:
            logging.info("Started")
            self.runme()
            logging.info("Completed successfully")
        except Exception as e:
            self.exception = traceback.format_exc()
            logging.error(self.exception)
            TEST_STOP = True
        finally:
            self.total_time = time.time() - self.start_time
            logging.info("Total run time: %.2f s" % self.total_time)
            self.con.close()

if  __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Drop cf stress')

    parser.add_argument('-d, --db', dest='db', default='test',
                        help="mysqld server database to test with")

    parser.add_argument('-H, --host', dest='host', default='127.0.0.1',
                        help="mysqld server host ip address")

    parser.add_argument('-L, --log-file', dest='log_file', default=None,
                        help="log file for output")

    parser.add_argument('-w, --num-workers', dest='num_workers', type=int,
                        default=16,
                        help="number of worker threads to test with")

    parser.add_argument('-P, --port', dest='port', default=3307, type=int,
                        help='mysqld server host port')

    parser.add_argument('-r, --num-requests', dest='num_requests', type=int,
                        default=100000000,
                        help="number of requests issued per worker thread")

    parser.add_argument('-u, --user', dest='user', default='root',
                        help="user to log into the mysql server")

    parser.add_argument('-v, --verbose', dest='verbose', action='store_true',
                        help="enable debug logging")

    OPTIONS = parser.parse_args()

    if OPTIONS.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level,
                    format='%(asctime)s %(threadName)s [%(levelname)s] '
                           '%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=OPTIONS.log_file)

    con = MySQLdb.connect(user=OPTIONS.user, host=OPTIONS.host,
                           port=OPTIONS.port, db=OPTIONS.db)

    if not con:
        raise Exception("Unable to connect to mysqld server")

    workers = []
    for i in range(OPTIONS.num_workers):
        workers.append(WorkerThread(i))

    workers_failed = 0
    workers_failed += wait_for_workers(workers)
    if workers_failed > 0:
        logging.error("Test detected %d failures, aborting" % workers_failed)
        sys.exit(1)

    con.close()

    sys.exit(0)

