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

TODO


## API

TODO


## Configuration

TODO


## Examples

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.dump('select * from dwh.dim_customers',header=True)
run(cmd,out='customers.tsv')
```

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.load('customers.tsv', 'stage.customers', 'id int, name string, email string', header=True)
run(cmd)
```

Example:
```
from faraway import hadoop,run
h = hadoop()
my_script = open('my_script.sql').read()
cmd = h.sql(my_script)
run(cmd)
```

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.show('select * from dwh.dict_products limit 30',output='table')
run(cmd)
```

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.show('select * from dwh.dict_products limit 30')
proc = run(cmd,mode=4)
for line in proc.stdout:
	print('>>> '+line)
```
