# Query Benchmarking Tool

This is a demo application.

## Goal

To implement a command line tool to benchmark `SELECT` query performance
using multiple workers against the sqlite3 database. 

Input:

- database file
- CSV file with query parameters received either as an argument or from STDIN
- flag to specify the number of concurrent workers

Task:

For each row in the CSV file (skipping the header) extract the values
(hostname, start_time, end_time) and generate an SQL query which returns
the max and min cpu usage of the given hostname for every minute in the
time range specified by the start_time and end_time. 

Each query should then be executed by one of the concurrent workers with
the constraint that queries with the same hostname be executed by the
same worker each time.

The tool should then measure the processing times of each query and
output the stats.

Output:

- the number of queries processed
- the number of queries which returned some data
- the overall query processing time
- the sum of the individual query times
- the mininum query time
- the maximum query time
- the average query time

## Installation

Dependencies:

- clang 6.0.1 or higher
- libdill 2.10.1 or higher 
- libsqlite3 3.25.0 or higher
- xxhash 0.6.5 or higher
- python3 3.7 or higher
- rust 1.28.0 or higher

```
git clone git@github.com:milancio42/qtool_c.git
cd qtool_c

chmod +x waf
CC=clang ./waf configure
./waf release
./waf test
```

## Example usage

```
# query the 'tests/testdb' database with 5 workers 
# read the parameters from the file 'tests/test_query_params.csv':
build/release/qtool -w 5 tests/tesdb tests/test_query_params.csv

# query the 'tests/testdb' database with the default number of workers 
# read the parameters from stdin:
cat tests/test_query_params.csv | build/release/qtool tests/tesdb 
```

