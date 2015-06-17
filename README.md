SUMMARY
=======

Apiary is a multi-protocol load testing and benchmarking tool.  Unlike most load-testing tools that generate artificial load programmatically, Apiary replays captured production load at a test host.  This is critically important in allowing you to gather useful information that lets you make confident predictions of how a system will perform in production.  Apiary goes to a lot of trouble to replay traffic with the same timing and parallelization as seen in the captured traffic.  It can also simulate higher or lower load by adjusting the time scale by an arbitrary floating-point multiple.

To use apiary, you capture production traffic using your favorite tool that generates a pcap file (`tcpdump`, `gulp`, etc).  Next you post-process the pcap file into a `.jobs` file, which describes the sequences of requests that each simulated client will make, along with their exact timings.  Optionally, you can then analyze this jobs file to determine the number of worker threads you'll need.  Finally, you run apiary, which fires off queries and tabulates statistics.

The Apiary project is a toolset containing the following components:
  * `bin/apiary`, the load simulation tool
  * various scripts to process pcap files into `.jobs` files (`bin/genjobs-*`)
  * other scripts such as `bin/count-concurrent-jobs`
  * protocol plugins


PERFORMANCE
===========

Apiary has been used to produce 30,000+ MySQL queries per second when run on a powerful test system.  It has also produced 3000 HTTP requests per second on a moderately powerful test system, and 100,000 CountDB (simple TCP) requests per second.  It can run 6000 simultaneous worker threads without issue and maintain proper timing.

The overall structure of Apiary is multiple processes with multiple threads per process, similar to Apache's `mpm_worker`.  The 6000 thread scenario mentioned above involved 100 worker processes, each with 60 threads.  Common wisdom is that Python is terrible at threads due to the Global Interpreter Lock(GIL), and that 6000 processes would be too heavy-weight and would consume too much memory to be feasible.  This would indicate an asynchronous approach such as twisted, eventlet, etc.  However, because most of Apiary's worker threads are blocked on IO the vast majority of the time, the GIL doesn't slow things down too badly.  Somewhat surprisingly, a reasonably powerful multiprocessor Linux machine is perfectly capable of running 6000 threads doing lots of blocking IO without spending too much time context-switching.

In the most recent incarnation of Apiary (see HISTORY), no attempt has yet been made to automatically run workers on multiple hosts.  This can still be accomplished through the use of --skip and --offset to split load generation out to multiple hosts, so long as apiary is started at about the same time on all hosts.

REQUIREMENTS
============

  * percona-toolkit (for MySQL protocol only)
    * http://www.percona.com/software/percona-toolkit
  * python-mysqldb
    * FIXME: currently required even if you don't use the MySQL protocol
  * tcpflow (for TCP)
    * https://github.com/simsong/tcpflow
  * lsprof (if you want to use --profile)
    * http://codespeak.net/svn/user/arigo/hack/misc/lsprof/


MySQL TUTORIAL
========

WARNING: This tutorial will guide you through capturing MySQL traffic from a production server.  This potentially dangerous activity has the possibility of saturating your server's network link, disk, and/or CPU, impacting production query processing.  Please exercise caution.  The authors of Apiary will not be responsible for any such impact or damage resulting from it.

What we'll do:
  1. Make a snapshot of the MySQL database.
  2. Capture query traffic starting at the moment the database snapshot is taken.
  3. Process the snapshot into a `.jobs` file.
  4. Replay the `.jobs` file against a test host.

It's important to capture the database snapshot and query traffic as close to simultaneously as possible.  Failure to do this may result in inaccurate load simulation and duplicate key errors.

First, you'll need a replicating set of at least two MySQL hosts.  One common structure for MySQL replicating sets is to have a "backup" host from which regular snapshots are taken.  This host is an ideal place to take your snapshot.  Wait until this host is caught up in its replication, and prepare to take a snapshot using your normal backup snapshot method or any other method you like.

Next, prepare to take a packet capture from your database host.  This can be from the master, a read slave, or even multiple hosts in the same replicating tree.  TEST THIS FIRST to ensure that you can safely capture traffic without impacting production query processing.  Consider that heavily-loaded database servers may have high network traffic, and writing this to the same disk that your database lives on will cause contention and filesystem cache invalidation.  Be sure to capture only traffic you need by filtering to port 3306 and disabling promiscuous mode (`tcpdump -p`).  Be careful!

My preferred packet capture method is to run this from a non-production host with a big disk:

    ssh <db host> sudo timeout 5m tcpdump -n -p -w - -s 0 port 3306 > db.pcap

This will stream the packet capture over your SSH tunnel to your non-production host, avoiding disk IO contention on the database host.  Be sure to filter your capture so that you don't try to capture port 22; otherwise you'll probably have a pretty bad day.

Once you've got your packet capture commands ready, start your database snapshot and packet capture at the same time and wait until they finish.  Capture as much traffic as you'd like to be able to simulate, keeping in mind that you might want to play it back at double speed or more to test a load spike scenario or growth.

Next, you'll need to process your traffic capture into a jobs file.  The process looks like this:

  1. reprocess the pcap into the format expected by `pt-query-digest`
  2. run `pt-query-digest` to produce a query log tagged with timing information and client identifiers
  3. run `bin/genjobs` to process the query log into a jobs file

We can combine steps 1 and 2 (and probably 3, if you tried hard enough):

    tcpdump -r <pcap file> -x -n -q -tttt | \
      pt-query-digest --type=tcpdump --no-report --output slowlog query.log

This can take a very long time, so sit back.  It can also consume a large amount of RAM.

Next:

    bin/genjobs query.log > foo.jobs

This also takes quite awhile.  The reason these take so long is that they're sorting massively interleaved streams into individual client/server conversations.  Additionally, the jobs file must be sorted by the timestamp of each job's first query.  This requires `genjobs` to hold in-flight jobs in memory and flush them to disk in order as their ends are found.

Now, we have our jobs file and we can simulate load.  Spin up a new MySQL host to test on and load the snapshot you made earlier.  Keep in mind that every time you run your load scenario, you're going to need to use a fresh copy of the snapshot.  LVM can be particularly useful for rolling back to the pristine snapshot and seems to impose a relatively minimal performance penalty.

Run apiary:
    bin/apiary --workers <#> --threads <#> --protocol mysql --mysql-host <test host> --mysql-user <user> --mysql-passwd <password> --mysql-db <db> foo.jobs

Apiary will start up workers and begin to report statistics periodically, by default every 15 seconds.  Apiary reports various statistics (depending on the protocol in use) and also reports how these change from interval to interval.

You can shut down Apiary using Ctrl-C.  This causes the Queen Bee to stop enqueueing jobs for the Worker Bees.  Once they finish running all jobs in the queue, Apiary will shut down.  A second Ctrl-C will immediately terminate Apiary.

INTERPRETING STATISTICS
=======================

Apiary reports a set of statistics periodically during load generation.  The particular statistics gathered can vary between protocols, although some statistics are common to all protocols.  Statistics come in three different types:
  * **Tally** - A simple counter that can only be incremented.  Reports the number of increments in the current period and the total increments ever, along with the increment rate this period.
  * **Level** - A counter that can be incremented and decremented.  Reports the current value, along with min, max, median, mean, and standard deviation of level values seen throughout this period.
  * **Series** - A floating-point time series.  Reports the most recent value (as "current"), along with min, max, median, mean, and standard deviation of values seen in this period.

Statistics are printed in the order the types are listed above.  For each value printed, if it has changed since the last report, the amount of change is printed beside it.

Here's an example report:

    2015-06-17 15:09:39
    ===================
    ERR: <SQL syntax error>:  current:      109 (-8)      total:      226 (+109)     rate:       7 (+0)
       ERR: <access denied>:  current:        0 (-1)      total:        1            rate:       0 (+0)
             Jobs Completed:  current:    12937 (+295)    total:    25579 (+12937)   rate:     862 (+16)
         Requests Completed:  current:    59890 (+1728)   total:   118052 (+59890)   rate:    3992 (+103)
               Jobs Running:  current:      109 (+3)        min:       77 (+75)       max:     155 (+9)    median:      112 (+7)        mean:      112 (+7)       stdev:       12 (-3)
           Requests Running:  current:        2 (+2)        min:        0             max:      16 (-6)    median:        2             mean:        2 (+0)       stdev:        1 (+0)
      Request Duration (ms):  current: 0.428915 (+0.0889)   min: 0.295162 (-0.00381)  max: 77.0881 (+18.7) median: 0.430822 (-0.000238) mean: 0.459850 (+0.00504) stdev: 0.720739 (+0.407)

From this report, we can learn:
  * The report happened at `2015-06-17 15:09:39`
  * 109 SQL syntax errors were reported by the `mysql` protocol plugin.  MySQL errors can be verbose and are simplified to avoid making the table excessively wide.
  * About 13000 jobs were completed in the last reporting interval, which is 295 more than in the previous interval, and amounts to about 862 jobs completed per second.
  * Similarly, about 60000 requests were completed in the last reporting interval, implying that each job consists of about 5 requests on average.
  * 109 jobs are in process right now, but only 2 requests are actively running.  This implies that requests are quite short (as seen in Request Duration), and that most of the time taken to run a job is spent sleeping while waiting for the proper time to run each query.

HTTP
====

Apiary can replay captured HTTP traffic, including keep-alive patterns (but not pipelining).  Requests are sent as seen on the wire without processing.  Responses are parsed in order to properly read a full response for each request, and HTTP response codes are tallied.

Capture traffic using your favorite packet capture tool.  Only the requests are used by Apiary, and it is allowable to capture only incoming traffic with your capture tool.  Use `tcpflow` to parse the pcap file into streams, one per file, with timing information:

    tcpflow -r foo.pcap -Fm -I -Z -o flows

Note that tcpflow can be very picky about the pcap.  If your pcap is terminated mid-packet, tcpflow may crash at the end without flushing out any streams buffered in memory.  To avoid this, pre-process your pcap with `tcpdump` to remove the partial packet:

    tcpdump -r foo.pcap -w foo_fixed.pcap

Annoying, but it works.

This uses the undocumented -I switch to save timing information.  -Z prevents decompressing gzip-encoded HTTP requests -- we want apiary to send them just as they were.

Next, use `bin/genjobs-http` or `bin/genjobs-http-individual` to generate a jobs file.  These two are the same except that the latter ignores the observed keep-alive in the packet capture and sends all requests with a new connection.

CountDB
=======

This one is a protocol used by an internal tool at DeviantArt.  It may be useful to see how new protocols can be added, or perhaps it might be useful for other services that use simple protocols.  It was written before the author knew about `tcpflow`.  `pkt2flow` (https://github.com/caesar0301/pkt2flow) might also have been useful here.

TUNING
======

These networking settings are useful with large query volumes:

    sudo bash -c 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse'
    sudo bash -c 'echo 1024 65535 > /proc/sys/net/ipv4/ip_local_port_range'

Otherwise, you may start to see errors like "Could not connect to MySQL host", or "resource not available", because the kernel will quickly run out of local ports it's willing to use.

Also, be sure to run apiary with a high file descriptor limit when replaying large numbers of concurrent requests.

Tuning worker threads is important.  Too few and your workers will fall behind and be unable to simulate full production load.  Too many, and in theory you'll run out of memory or consume too much time in context switching, though in practice, I've never seen this.  You can figure out the minimum number of workers required to run your jobs file like this:

    bin/count-concurrent-jobs foo.jobs

I'd probably add a margin of 10-20% just to be sure.  For high concurrency, you may need to experiment with a balance of processes and threads.  I'd recommend running no more than 80 threads per worker process.

For more than a few hundred total threads, use --startup-wait to cause Apiary to pause and allow workers to initalize.  30-60 seconds is usually enough.  Failure to do this may cause an initial load spike as workers start the first few jobs late and rush to catch up.

HISTORY
=======

Apiary was originally designed and developed at Linden Lab by Charity Majors and (Dante Linden <FIXME: real name unknown>).  It was used to test MySQL to gain confidence in upgrading to version 5.0.  Apiary's initial design used RabbitMQ as its messaging bus.  Sets of workers were spawned manually on multiple machines by the user in order to achieve the desired parallelization.  Apiary also had rudimentary support for the HTTP protocol but this was unfinished, and may never have worked at all.  The query load scenario was stored in a custom text format.  At this point, Apiary was open-sourced as-is, somewhat short on documentation but functional.  It was the only tool of its kind available.

Years later, Apiary was used by Lex Neva to test a new database machine build prior to replacing Linden's aging database fleet.  By this point, the query volume had increased by a significating factor and the original Apiary codebase was no longer able to generate the required query volume.  In addition, the PCAP parsing code no longer worked properly and the original developers had left the company.  Lex modified Apiary to allow the use of `pt-query-digest`, avoiding the need to use the old custom in-house written PCAP parser.

Lex identified multiple bottlenecks and ended up rewriting large portions of Apiary's main engine for speed and simplicity of use.  The load scenario was now stored on disk as a series of cPickle-formatted jobs that could be read in and shoved onto the wire as-is.  Multiple-protocol support was removed in favor of raw speed.  Multiple features such as time scaling were added.  Apiary now all ran on one machine and spawned and managed its own child processes.  At this point, Apiary could generate 30,000+ queries per second.

In a later role at DeviantArt Inc, Lex used Apiary to test an in-house tool called CountDB.  CountDB uses a simple raw TCP protocol, with the client and server communicating in json-serialized messages delimited by null bytes.  CountDB serves around 6000 concurrent clients in production at around 100k requests per second.  Multi-protocol support was re-added, along with threading, since the previous model used individual processes for workers and likely would not scale to 6000 processes.  This necessitated the use of a different python AMQP library (rabbitpy) that was thread-safe, leading to the somewhat unwieldy requirement of two (!) AMQP client libraries.

Lex later added HTTP support in order to test DeviantArt's new fleet of web servers processing thousands of requests per second.  --offset, --skip, and related options were added in order to test fractions of the full fleet.  At this point, RabbitMQ became a significant bottleneck.  Even without durability, RabbitMQ was unable to handle enough messages per second to allow workers to report statistics on individual requests.  ZeroMQ was tested against simple Python multiprocessing Queues, and the latter was faster by at least a factor of two.  Now Apiary has no RabbitMQ dependency and has much more advanced statistics gathering capabilities.
