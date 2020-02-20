# faraway
Faraway facilitates remote Hadoop operations via SSH.
It is distributed as a single file module and has
no dependencies other than the Python Standard Library.

* **simple** - convenient access to ETL functionality on Hadoop
* **portable** - runs on Python 2.7 and 3+, on Windows and Unix

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.dump('select * from dwh.dim_customers',header=True)
run(cmd,out='customers.tsv')
```


## CLI

```
usage: faraway.py [--help] [--version] [-h] [-s] [-v] [-e]
                  [-f {tsv2,csv2,dsv,table,vertical,xmlattr,xmlelements,tsv,csv}]
                  [-sep SEP] [-csep CSEP] [-ksep KSEP] [-n NAME] [-t TAGS]
                  {sql,dump,load,vars} [argv [argv ...]]

Remote Hadoop operations over ssh

positional arguments:
  {sql,dump,load,vars}  action
  argv                  action specific arguments (described below)

optional arguments:
  --help                print help
  --version             show program's version number and exit
  -h, --header          include header (sql, dump) / skip header (load)
  -s, --silent          silent beeline
  -v, --verbose         verbose beeline
  -e, --echo            echo ssh commands
  -f {tsv2,csv2,dsv,table,vertical,xmlattr,xmlelements,tsv,csv}
                        beeline file format, default "tsv2"
  -sep SEP              field separator, default "\t"
  -csep CSEP            collection items separator, default ","
  -ksep KSEP            map keys separator, default ":"
  -n NAME, --name NAME  connection name
  -t TAGS, --tags TAGS  connection tags (comma separated)

Actions:

* sql script                  write sql script results to stdout (low latency)
* dump query                  write single sql query results to stdout (high throughput)
* load path table columns     load data from local file into table
* vars                        show faraway variables

Configuration:

* faraway loads configuration from following .json files:
  - from user directory:
      C:\Users\my_user_name\.faraway\config.json   on Windows
      ~/.faraway/config.json                           on Linux
  - from current directory: ./faraway.py

* faraway reads environment variables prefixed with "faraway_",
  strips the prefix and uses them as faraway variables

Actions - sql vs dump:

* TODO

Examples:

* faraway sql -h "select * from stage.movies"
* faraway dump "select * from stage.movies" >movies.tsv
* faraway load -h movies.tsv stage.movies "id int, title string, year string"
* faraway sql - <transform.sql
* faraway sql -s "show create table stage.movies"
* echo "drop table if exists {env}.movies" | faraway_env=test faraway sql
* echo "select * from stage.movies" | faraway sql -h -
* echo "select * from stage.movies" | faraway dump -h - >movies.tsv
* faraway vars -n my_conn

  where faraway is an alias:
  - doskey faraway=python3 faraway.py $*    on Windows
  - alias faraway=python3 faraway.py        on Linux
```

## API

TODO


## Configuration

TODO


## Output formats

TODO

## Examples

Dump table to tsv file:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.dump('select * from dwh.dim_customers',header=True)
run(cmd,out='customers.tsv')
```

Load tsv file as new table:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.load('customers.tsv', 'stage.customers', 'id int, name string, email string', header=True)
run(cmd)
```

Execute data transformation script:
```
from faraway import hadoop,run
h = hadoop()
my_script = open('my_script.sql').read()
cmd = h.sql(my_script)
run(cmd)
```

Show first rows as table:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.show('select * from dwh.dict_products limit 30',output='table')
run(cmd)
```

Print first records fixing specific typo:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.show('select * from dwh.dict_products limit 30')
proc = run(cmd,mode=4)
for line in proc.stdout:
	rec = line.replace('hamer','hammer').split('\t')
	print(rec)
```
