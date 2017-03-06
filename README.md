# Real Time Trace Analyzer (RTTAnalyzer)
Do you have a pesky user that always knows when their Order Entry is running
slower than it should? Would you like to already know when the phone rings
what the complaint is all about? (No, not the whole database, just that
particular Order Entry thingy)

## Simple Real Time Tracking and Alerting of a statement's response time
Oracle does have an exceptionally nice SQL trace facility and there are
number of profilers available on the market (including some free ones)
that can analyze the SQL trace files. However, profilers are designed to
work on historical data. What if you need an alert in real time if/when
your Order Entry statement runs slower than 2 seconds?
You could tail a trace file manually and hope to catch that line that you
are after, but it may get a bit tedious. Humans need breaks, some like
coffee and even food, so they need to get away from a terminal from time
to time. Also, that Order Entry statement may run concurrently by
a 100 users, which (in a dedicated/MTS connection mode) would generate
a 100 trace files. At this point you could try to clone yourself 100
times and instruct your clones to tail 100 traces ... or you could give
`RTTAnalyzer` a shot.

`RTTAnalyzer` makes it possible to receive a notification in real time when
that Order Entry statement crosses the 2 seconds mark (and you may even
decide be forward it to a pager of your colleague too if you like).
You may also appreciate having all those notifications recorded in a
in a succinct format in one central place, in a database table as opposed
to being scattered among potentially hundreds of trace files cluttered
with the stuff you don't care about.
This central database table that `RTTAnalyzer` automatically records
notifcations too may later become a treasure trove for future analysis,
trending, forecasting, etc.

## Overall Database Performance and user centric Service Level Objectives (SLO)
Once you get comfortable with the above scenario you may start pondering about
the overall database performance. In a typical busy enough (and so perhaps worth
keeping an eye on) database, there are thousands things going on, but what you
really care about (from a performance point of view) is just that Order Entry thing.
And may be a couple more like it. But what about the other 
	(thousands - 3 things that you care about)?


What about that batch job that runs for 2 hours and consumes
a tons of resource? It may cause the smoke to start coming out of
your storage array, but frankly it could run for 4 hours too and
nobody would notice (or care) as long as the data that this job
aggregates is ready for reporting in the following morning.
OK, but if so, why does that job, that nobody particularly cares about,
is part of your system wide synthetic performance metric?
Wouldn't it be nice to exclude it and since we are at it, perhaps
exclude the other (thousands - 3 things that you care about) too?

If that scenario is familiar to you, you may start wondering if judging
overall database performance by monitoring a system-wide synthetic
database metric (e.g. Average Active Session or a DB Time) may not
necessarily be indicative of the user perceived response time of that
Order Entry that they care about. In other words that the overall database
performance metric that you go by, may not reflect the user experience.

So where does that leave us? Well, the alternative is to focus on just that.
Focus on specific business transactions that matter to the user most,
map them to the corresponding SQL statements and track performance of
these statements in real time.

This approach aligns business priorities with technology, promotes common terminology 
and leads to a sharper focus across different Product Areas (PAs).
Once the key business transactions are defined, Service Level Indicators (SLIs)
can be established and the objectives for each SLI can be defined. 
Conceptually, these Service Level Objectives (SLOs) constitute the input
to `RTTAnalyzer`, and in this scenario, `RTTAnalyzer` is a tool that raises
alerts once an SLO for your key business transaction is breached.


## The Mechanics of how RTTAnalyzer may work for you
The process of taking the SLOs and converting them to the actual input for
`RTTAnalyzer` may resemble the following:

- Take each business transaction with the corresponding SLO value
- Map it to the corresponding set of SQL statements
- Enable basic SQL trace on these statements (10046 level 1)
	
- `RTTAnalyzer` takes the above input, monitors SQL traces in real time and raises
violations if SLOs are breached for any of the business transactions of interest.

Let's briefly review all of the above steps:

### Identify the key business transactions that matter to the users most
That's where you take your user to lunch, buy them a nice sandwich and expect
them to spill their guts about their pain points with your system. It doesn't
have to be quite literally like that, but as long as you get a short list of
things that your user really, really cares about in your system, the money on that
sandwich should be considered well spent. Please also remember to get a threshold
for each "Order Entry" thing after which your user starts pulling his hair out.
This is not only because his hair is important to you, but also because you'd
probably want to get notified when things go south in your system.

### Map the key business transactions to a set of SQL statements
If you are lucky enough to deal with the software that is intelligent
enough to instrument its code by attaching a module, action, client id
to the statements it runs, it's a walk in the park (and BTW, if you
are the one who wrote that software you surely did it too, right?)

If not, SQL tracing may be the way to go. Most apps don't generate
statements dynamically, so the SQL IDs are not hard to find. Please
also remember that if your Order Entry bus. tx consists of 300 statements
you don't necessarily need to track them all. Once you understand
what Order Entry does to your system, you'll probably be able to
narrow it down to perhaps 20 that may misbehave and you'd need
to keep an eye on.

Please also note that if you don't want to spend time on tracing
and understanding what Order Entry is in terms of SQL, you can feed
`RTTAnalyzer` an AWR report and it will attempt to help you out
(see below for an example of that).

### RTTAnalyzer: sample runs
Can we finally see how `RTTAnalyzer` works? I'm glad you asked...

```
$ ./rtta -help

        Real Time Trace Analyzer (RTTAnalyzer)

Tool for monitoring and alerting SLO violations of business transactions of interest.
See github.com/borisdali/rttanalyzer for details.

Available options:
  - debug: As the name implies, adding debugging clutter.
  - service: Run the trace file tracker as a service/daemon.
  - dequeue: (For running in PubSub mode on GCP) dequeue events, persist in BQ.
  - setup: Presently supports only one option: -awr, as in:
        rtta -setup -awr <AWR report file path name>
```

You need two input files: a configuration file (rtta.conf) and a SQL input file
(rtta.sqlinput). You'll see samples of both of them presented below.

`RTTAnalyzer` can run either interactively or in the background as a daemon.
Sample interactive run where the output is requested to be in the ASCII file format 
(referred to as VarZ) is presented below:

```
$ cat rtta.conf 
# RTTAnalyzer input parameters. The format is key-value pairs.
# The mandatory parameters are: dbname, dirname, mode, sqlinput, outputyype
#
dbname = CLOUD2
dirname = /u01/app/oracle/diag/rdbms/cloud2/CLOUD2/trace
sqlinput=rtta.sqlinput
outputtype=varz


$ sudo ./rtta 
Real Time Trace Analyzer (RTTAnalyzer): github.com/borisdali/rttanalyzer
[2017-01-30 16:43:01] info> the output media requested for RTTAnalyzer is an ASCII file (referred to as VarZ).

[2017-01-30 16:43:08] info> interesting SQL found: acc988uzvjmmt (BusinessTxName=EBS/Month End Reconciliation Job, ELA Threshold=1)
[2017-01-30 16:43:08] info> parsingInCursor: New cursor# 12. Open for SQLID=acc988uzvjmmt, BusinessTxName=EBS/Month End Reconciliation Job: PARSING IN CURSOR #12 len=612 dep=1 uid=0 oct=47 lid=0 tim=1409063809187197 hv=3670034437 ad='7cbeae9d8' sqlid='acc988uzvjmmt'
[2017-01-30 16:43:20] warning> EBS/Month End Reconciliation Job [SQL_ID=acc988uzvjmmt] ran for 100.015 [ms] (cpu=1.000 [ms]) during EXEC phase (threshold of 1.000 [ms]): 
[2017-01-30 16:43:20] info> lastela:100.015 worstela:100.015 violations:1

```

In the ASCII output mode the `RTTAnalyzer` generated file may look like the following:

```
$ cat /opt/mg-agent-xp/data.d/rttanalyzer.CLOUD2.ebs_month_end_reconciliation_job.varz 
rttanalyzer{id=CLOUD2,businesstxname="EBS/Month End Reconciliation Job",runtimethreshold=1.0,sqlid=acc988uzvjmmt} map:stats lastela:100.015 worstela:100.015 violations:1

```

The other --and more useful-- output format available is [Google Pub/Sub](https://cloud.google.com/pubsub).
If deployed in a cloud environment, `RTTAnalyzer` can asynchronously stream the 
SLO "violations" for the business transactions of interest to the 
message-oriented middleware, which can be later independently retrieved by
`RTTAnalyzer` running in the dequeueing mode (see the "rtta -dequeue" option below).
Once retrieved, the violation messages get automatically persisted to
[Google Big Query](https://cloud.google.com/bigquery) table, which opens an easy
possibility of a later offline analysis, trending, graphing, etc.

```
$ cat rtta.conf 
# RTTAnalyzer input parameters. The format is key-value pairs.
# The mandatory parameters are: dbname, dirname, mode, sqlinput, outputyype
#
dbname = CLOUD2
dirname = /u01/app/oracle/diag/rdbms/cloud2/CLOUD2/trace
sqlinput=rtta.sqlinput
outputtype=pubsub 
appcredentials = "/home/bdali/Testing Oracle on Cloud-a123456bcd.json"
projectname = GCPprojectName

$ sudo ./rtta
Real Time Trace Analyzer (RTTAnalyzer): github.com/borisdali/rttanalyzer
[2017-01-30 17:03:49] info> the output media requested for RTTAnalyzer is Pub/Sub.

[2017-01-30 17:03:54] info> interesting SQL found: acc988uzvjmmt (BusinessTxName=EBS/Month End Reconciliation Job, ELA Threshold=1)
[2017-01-30 17:03:54] info> parsingInCursor: New cursor# 12. Open for SQLID=acc988uzvjmmt, BusinessTxName=EBS/Month End Reconciliation Job: PARSING IN CURSOR #12 len=612 dep=1 uid=0 oct=47 lid=0 tim=1409063809187197 hv=3670034437 ad='7cbeae9d8' sqlid='acc988uzvjmmt'
[2017-01-30 17:03:58] warning> EBS/Month End Reconciliation Job [SQL_ID=acc988uzvjmmt] ran for 100.015 [ms] (cpu=1.000 [ms]) during EXEC phase (threshold of 1.000 [ms]): 
[2017-01-30 17:03:58] info> lastela:100.015 worstela:100.015 violations:1
[2017-01-30 17:03:58] info> Published a message: id=53553948383322 (on topic=projects/GCPprojectName/topics/rttanalyzertopic)

```

Retrieving SLO violation messages from the Pub/Sub queue can be done by the same rtta program in dequeue mode:

```
$ ./rtta -dequeue
Real Time Trace Analyzer (RTTAnalyzer): github.com/borisdali/rttanalyzer
[2017-01-30 17:05:53] count=1: Processing message id=53553948383322 from topic=projects/GCPprojectName/topics/rttanalyzertopic: DB=CLOUD2, payload={CLOUD2 true EBS/Month End Reconciliation Job 1 acc988uzvjmmt 100.015 100.015 1 2017-01-30 17:03:58.268789362 +0000 UTC}, IsViolation=true, BusinessTxName=EBS/Month End Reconciliation Job, Threshold=1.000000, SQLID=acc988uzvjmmt, WorstELA=100.015000, LastELA=100.015000, NumViolations=1
```

To verify that the whole pipeline (from the SLO violations recorded in the trace files
through the `RTTAnalyzer` to Pub/Sub and BigQuery), check the latest BQ trace record:

```
$ bq query "select * from (select * from [GCPprojectName.rttanalyzer.traces] order by dequeued_at desc) limit 1"
Waiting on bqjob_r25c4fb5838209d2a_00000159f05c01f9_1 ... (0s) Current status: DONE   
+----+----------+----------------+-----------+---------------+---------+----------+------------+---------------------+---------------------+
| id | database | businesstxname | threshold |     sqlid     | lastela | worstela | violations |     enqueued_at     |     dequeued_at     |
+----+----------+----------------+-----------+---------------+---------+----------+------------+---------------------+---------------------+
|  1 | CLOUD2   | EBS/Month End R|       1.0 | acc988uzvjmmt | 100.015 |  100.015 |          1 | 2017-01-30 17:03:58 | 2017-01-30 17:05:53 |
+----+----------+----------------+-----------+---------------+---------+----------+------------+---------------------+---------------------+

```

### RTTAnalyzer: help prepare the SQL input file
Finally, as promised, as a starter or if you really don't
want to trace and to understand what the Order Entry business
transaction actually does to your system, `RTTAnalyzer`
may offer some assistance:

```
$ ./rtta -setup -awr awrrpt_1_20976_20977.txt
Real Time Trace Analyzer (RTTAnalyzer): github.com/borisdali/rttanalyzer

RTTAnalyzer rtta.sqlinput.fromAWR input file has been generated.
Please review and if it looks acceptable, use it as a real SQL input file, i.e.
	$ mv rtta.sqlinput.fromAWR rtta.sqlinput
```

As much as we strive to it, `RTTAnalyzer` is still limited with respect
to the amount of magic we can pull off and so when it comes to helping
with generating the SQL input file based on the representative AWR
snapshot, all we can do is to get the top longest running statements
based on the elapsed time section of AWR. 
These statements from AWR may obviously not belong to the transactions
that business cares about (which should be easy to identify by
looking at the module/action if an app is properly instrumented).

But it's a start. Review that SQL input file and edit it as you see fit.

```
$ cat rtta.sqlinput.fromAWR
#
# Real Time Trace Analyzer (RTTAnalyzer)
# rtta.sqlinput: one of the two input files (the other is rtta.conf) for the RTTA utility:
#
# Formating rules:
# - One line per business transaction that users care about most
# - Each non-commented line must consist of at least 3 comma-delimited fields:
#      "Business Tx Name", "Monitoring/Alerting Threshold", list (but at least one of) SQL IDs.
#
# Each field may be optionally enclosed in double quotes.  Blank lines are ignored.
#

ExpensiveSQL#1, 169, 1cd2krbdzrhvq
ExpensiveSQL#2, 1, bzscyq07w79ab
ExpensiveSQL#3, 1, 6gvch1xu9ca3g
ExpensiveSQL#4, 1, 70vs1d7ywk5m0
ExpensiveSQL#5, 1, c2p32r5mzv8hb
ExpensiveSQL#6, 1, acc988uzvjmmt
ExpensiveSQL#7, 1, g0jvz8csyrtcf
ExpensiveSQL#8, 1, 6ajkhukk78nsr

# Top AWR statements from SQL ordered by Elapsed Time section:
#   ela=141.0, execs=1, elaPerExec=141.04, % Total=6.E+03, % CPU=6.2,% I/O=.0, SQLid=1cd2krbdzrhvq
#   ela=1.3, execs=3, elaPerExec=0.44, % Total=53.7, % CPU=28.6,% I/O=73.5, SQLid=bzscyq07w79ab
#   ela=1.3, execs=60, elaPerExec=0.02, % Total=52.7, % CPU=73.8,% I/O=25.6, SQLid=6gvch1xu9ca3g
#   ela=0.8, execs=20, elaPerExec=0.04, % Total=34.1, % CPU=67.6,% I/O=33.4, SQLid=70vs1d7ywk5m0
#   ela=0.8, execs=1, elaPerExec=0.82, % Total=33.3, % CPU=15.9,% I/O=86.6, SQLid=c2p32r5mzv8hb
#   ela=0.6, execs=1, elaPerExec=0.60, % Total=24.4, % CPU=51.6,% I/O=50.3, SQLid=acc988uzvjmmt
#   ela=0.5, execs=1, elaPerExec=0.51, % Total=20.6, % CPU=7.6,% I/O=95.3, SQLid=g0jvz8csyrtcf
#   ela=0.5, execs=1, elaPerExec=0.47, % Total=19.1, % CPU=15.9,% I/O=86.2, SQLid=6ajkhukk78nsr

# Trace enable helper:
#   alter system set events 'sql_trace [sql:1cd2krbdzrhvq|bzscyq07w79ab|6gvch1xu9ca3g|70vs1d7ywk5m0|c2p32r5mzv8hb|acc988uzvjmmt|g0jvz8csyrtcf|6ajkhukk78nsr]';
#

```

Happy RTTAnalyz-ing and please see the User Guide for details ... oh, wait, there's no User Guide :-(
This is where we invite you to help writing one and in general to share your experience in using
`RTTAnalyzer` and perhaps to consider helping us in improving `RTTAnalyzer`.
Please see the CONTRIBUTING file for details.

