inch
====

Inch is an InfluxDB benchmarking tool for testing with different tag
cardinalities.


## Installing

To install, simply `go get` from the command line:

```sh
$ go get github.com/benbjohnson/inch
```


## Running

The `inch` program accepts several flags to adjust the number of points and
tag values.

```
Usage of inch:
  -b int
    	Batch size (default 5000)
  -c int
    	Concurrency (default 1)
  -consistency string
    	Write consistency (default any) (default "any")
  -db string
    	Database to write to (default "stress")
  -delay duration
    	Delay between writes
  -dry
    	Dry run (maximum writer perf of inch on box)
  -f int
    	Fields per point (default 1)
  -host string
    	Host (default "http://localhost:8086")
  -m int
    	Measurements (default 1)
  -p int
    	Points per series (default 100)
  -report-host string
    	Host to send metrics
  -report-tags string
    	Comma separated k=v tags to report alongside metrics
  -shard-duration string
    	Set shard duration (default 7d)
  -strict
    	Terminate process if error encountered
  -t string
    	Tag cardinality (default "10,10,10")
  -target-latency duration
    	If set inch will attempt to adapt write delay to meet target
  -time duration
    	Time span to spread writes over
  -v	Verbose
```

The `-t` flag specifies the number of tags and the cardinality by using a
comma-separated list of integers. For example, the value `"100,20,4"` means 
that 3 tag keys should be used. The first one has 100 values, the second one
has 20 values, and the last one has 4 values. `inch` will insert a series for
each combination of these values so the total number of series can be computed
by multiplying the values (`100 * 20 * 4`).

By setting the `verbose` flag you can see progress each second.

The `-target-latency` flag will allow `inch` to automatically backoff (add 
delays after writing batches) according to the weighted moving average of 
response times from the InfluxDB server. If the WMA of responses is greater than 
the target latency then delays will be increased in an attempt to allow the 
InfluxDB server time to recover and process in-flight writes. If a delay is in 
place on `inch` clients yet the WMA of response times is lower than the target
latency, then `inch` will reduce the delays in an attempt to increase throughput.

The `-report-host` flag can be used to specify the location of an InfluxDB 
instance, to be used for reporting the results of inch. A local instance to inch
would be specified as `-report-host http://localhost:8086`. Inch will provide 
appropriate tags where possible, but arbitrary tags can be set using the 
`-report-tags` flag. The format for `-report-tags` is a comma separated list of 
key value pairs. For example `-report-tags instance=m4.2xlarge,index=tsi1`.

When `-report-host` is set to a non-empty value, inch will report throughput, 
points and values written, as well as write latency statistics.


