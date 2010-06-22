#
# $LicenseInfo:firstyear=2010&license=mit$
# 
# Copyright (c) 2010, Linden Research, Inc.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# $/LicenseInfo$
#

import math
import random
import sys

import sqllog
import timestamp


class SQLModel(object):
    
    def __init__(self):
        self._sum_of_weights = 0.0
        self._entries = []
        self._weights = []
        self._accounts = []
        self._add_all_sql_options()
    
    def initial_sql(self):
        sql = """
DROP TABLE IF EXISTS `test`.`bankbalance`;
DROP TABLE IF EXISTS `test`.`banktransaction`;
CREATE TABLE `test`.`bankbalance` (
  `acct` VARCHAR(16) NOT NULL,
  `balance` INT NOT NULL DEFAULT 0,
  PRIMARY KEY (`acct`)
);
CREATE TABLE `test`.`banktransaction` (
  `sequence` INT NOT NULL AUTO_INCREMENT,
  `acct` VARCHAR(16) NOT NULL,
  `amount` INT NOT NULL DEFAULT 0,
  `cleared` ENUM('N', 'Y') DEFAULT 'N',
  PRIMARY KEY (`sequence`),
  INDEX acct(`acct`)
);
INSERT INTO bankbalance SET acct="zzz-99999", balance=0;
"""
        return sql.split(';')

    def _add_sql_option(self, weight, sql):
        self._sum_of_weights += weight
        self._entries.append(sql)
        self._weights.append(weight)

    def _add_all_sql_options(self):
        options = """
2:INSERT INTO bankbalance SET acct="%(newacct)s", balance=100
25:INSERT INTO banktransaction SET acct="%(acct)s", amount=%(delta)d
5:SELECT acct FROM bankbalance WHERE balance < 0
30:SELECT balance FROM bankbalance WHERE acct="%(acct)s"
5:UPDATE bankbalance SET balance=balance+%(delta)d WHERE acct="%(acct)s"
5:SELECT sequence, amount FROM banktransaction WHERE acct="%(acct)s" AND cleared="N"
5:UPDATE banktransaction SET cleared="Y" WHERE acct="%(acct)s" AND cleared="N"
"""
        for line in options.split('\n'):
            if ':' in line:
                (weight,sql) = line.split(':',1)
                weight = float(weight)
                self._add_sql_option(weight,sql)

    def _random_acct(self):
        if self._accounts:
            return random.choice(self._accounts)
        return 'zzz-99999'
    
    def _random_new_acct(self):
        acct = 'aaa-%05d' % random.randint(0,99999)
        self._accounts.append(acct)
        return acct
    
    def _random_delta(self):
        return random.randint(-20,20)
    
    def random_sql(self):
        v = random.uniform(0.0, self._sum_of_weights)
        for i in xrange(0, len(self._entries)):
            v -= self._weights[i]
            if v < 0.0:
                sql = self._entries[i]
                break
        values = {}
        if '%(newacct)' in sql:
            values['newacct'] = self._random_new_acct()
        if '%(acct)' in sql:
            values['acct'] = self._random_acct()
        if '%(delta)' in sql:
            values['delta'] = self._random_delta()
        return sql % values



class Generator(object):

    def __init__(self, model, target_event_count,
                        target_concurrency, target_sequence_length):
        self._model = model
        self._target_event_count = target_event_count
        self._target_concurrency = target_concurrency
        self._target_sequence_length = target_sequence_length

    def run(self):
        self._sequences = []
        self._sequence_count = 0
        self._sequence_events_to_go = { }
        self._event_count = 0
        self._time = timestamp.TimeStamp(1000000)
        self._timeincr = timestamp.TimeStamp(0,1000) # 10ms
        
        self.initial();
        while self.step():
            pass
    
    def initial(self):
        name = "7:999999"
        start = self._time
        for sql in self._model.initial_sql():
            sql = sql.strip()
            if sql:
                self.output_event(name, "Init", sql)
        self.gen_end(name)
        self._time = start + timestamp.TimeStamp(1)
        
    def step(self):
        winding_down = self._event_count > self._target_event_count
        how_full = float(len(self._sequences)) / self._target_concurrency
        should_add = math.exp(-1.0 * math.pow(how_full, 2.0))
        if not winding_down and random.random() < should_add:
            self.step_add()
            return True
        if self._sequences:
            self.step_event(random.choice(self._sequences))
            return True
        return False # nothing to do!
    
    def step_add(self):
        self._sequence_count += 1
        name = "8:%06d" % self._sequence_count
        count = max(1, int(random.normalvariate(
                self._target_sequence_length,
                self._target_sequence_length/2.0)))
        self._sequences.append(name)
        self._sequence_events_to_go[name] = count
        self.step_event(name)
    
    def step_event(self, name):
        if self._sequence_events_to_go[name] > 0:
            self.gen_event(name)
            self._sequence_events_to_go[name] -= 1
            self._event_count += 1
        else:
            self.gen_end(name)
            del self._sequence_events_to_go[name]
            self._sequences = self._sequence_events_to_go.keys()
    
    def gen_event(self, name):
        self.output_event(name, sqllog.Event.Query, self._model.random_sql())
    
    def gen_end(self, name):
        self.output_event(name, sqllog.Event.End, "Quit")
    
    def output_event(self, seq, state, body):
        t = self._time
        self._time = self._time + self._timeincr
        e = sqllog.Event(t, seq, "test", state, body)
        sys.stdout.write(str(e))
    

if __name__ == '__main__':
    m = SQLModel()
    g = Generator(m, 1000, 5, 6.0)
    g.run()
    
