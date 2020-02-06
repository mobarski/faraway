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
cmd = h.show('select * from dwh.dict_products limit 30',output='table')
run(cmd)
```

Example:
```
from faraway import hadoop,run
h = hadoop()
cmd = h.dump('select * from dwh.dim_customers')
run(cmd,out='customers.tsv')
```
