# faraway
Faraway facilitates remote Hadoop operations via SSH.
It is distributed as a single file module and has
no dependencies other than the Python Standard Library.

* **simple** - convenient access to ETL functionality on Hadoop
* **portable** - runs on Python 2.7 and 3+, on Windows and Unix

Example:
```
from faraway import hadoop
h = hadoop('user123@host.com')
h.extract('products_sample.csv', 'select * from dwh.dict_products limit 1000', sep=',')
```

Example:
```
import faraway
with faraway.hadoop('user123@host.com') as h:
	h.extract('products_sample.csv', 'select * from dwh.dict_products limit 1000', sep=',')
	h.transform('drop table stage.dict_assets; drop table stage.dict_partners;')
	h.load('dict_terminals.tsv','stage.dict_terminals','trm_code INT, trm_name STRING')
	h.cmd('hdfs dfs -du /user/user123 | sort -nr | head -n 10')
	h.run()
```

Example:
```
from faraway import hadoop

HOST = 'user123@hadoop.company.com'
SSH = r'C:\putty\plink -i C:\putty\id_rsa.ppk'
SCP = r'C:\putty\pscp -i C:\putty\id_rsa.ppk'

with hadoop(HOST, ssh=SSH, scp=SCP) as h:
	h.set('hive','beeline -u "jdbc:hive2://hsv1:10000/;principal=hive/hsv1@company.com?mapreduce.job.queuename=dwh"')
	h.set('hdfs_tmp_dir','/load_data/dwh')
	h.load('dict_terminals.tsv','stage.dict_terminals','trm_code INT, trm_name STRING')
```
