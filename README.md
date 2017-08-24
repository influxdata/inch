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
  -strict
    	Terminate process if error encountered
  -t string
    	Tag cardinality (default "10,10,10")
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

