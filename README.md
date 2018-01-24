# faraway
Faraway facilitates remote Hadoop operations via SSH.
It is distributed as a single file module and has
no dependencies other than the Python Standard Library.

* **simple** - convenient access to ETL functionality on Hadoop
* **portable** - runs on Python 2.7 and 3+, on Windows and Unix

Example:
```
from faraway import host
with host('user123@host.com') as h:
	h.extract('products_sample.csv', 'select * from dwh.dict_products limit 1000', sep=',')
	h.transform('drop table stage.dict_assets; drop table stage.dict_partners;')
	h.load('dict_terminals.tsv','stage.dict_terminals','trm_code INT, trm_name STRING')
	top_10 = h.tmp()
	h.cmd('hdfs dfs -du /user/user123 | sort -nr | head -n 10 >'+top_10)
	h.download('top_10_files.txt',top_10)
```
