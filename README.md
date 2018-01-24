# faraway
Faraway facilitates remote Hadoop operations via SSH.
It is distributed as a single file module and has
no dependencies other than the Python Standard Library.

* **simple** - convenient access to hadoop ETL functionality 
* **portable** - runs on Python 2.7 and 3+, on Windows and Unix

Example:
```
from faraway import host
h = host('user@host.com')
h.export('products.csv', 'select * from dwh.dict_products', sep=',')
```
