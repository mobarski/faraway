import subprocess
from textwrap import dedent
from hashlib import sha1
import re
import sys

# version x8 - output_mode
# version x7 - plink compatible, run returns stdout, write now works on windows
# version x6 - table, select, drop, limit, where, quote, escape, null_value, escape_quotes, quote_all
# version x5 - py3, config, sep, remove output before run, multiline sql, df_aux
# version x4 - header, app_name, echo

# FUTURE - screen, file import/export, host2host copy, script, alias, stderr

def pipe(*cmds, **kw):
	doc = kw.get('doc',[])
	cmd = ' | '.join(cmds)
	if doc:
		cmd += '\n'
		for d in doc:
			cmd += d.strip()+'\n'
		# TODO rstrip?
	return cmd


class host:
	def __init__(self, prefix='', run=True, echo=True, verbose=True):
		self._run = run
		self._echo = echo
		self._verbose = verbose
		self.prefix = prefix.rstrip()+' ' if prefix else ''

	def os(self, cmd, *a, **kw):
		full_cmd = self._os(cmd,*a,**kw)
		return self.run(full_cmd)

	def dfs(self, cmd, *a, **kw):
		full_cmd = self._dfs(cmd,*a,**kw)
		return self.run(full_cmd)
			
	def write(self, path, text, eof='EOF'):
		# TODO scp based?
		# TODO templates?
		if sys.platform.lower()[:3]=='win':
			from tempfile import TemporaryFile
			with TemporaryFile() as f:
				f.write(text)
				f.seek(0)
				full_cmd = self._os('"cat >{0}"'.format(path))
				out = self.run(full_cmd,stdin=f)
			return out
		else:
			full_cmd = self._write(path, text, eof)
			return self.run(full_cmd)
		
	def run(self, cmd, stdin=None):
		if self._echo:
			print(cmd)
		if self._run:
			out = subprocess.check_output(cmd,stdin=stdin,shell=True)
			if self._verbose:
				print(out)
			return out

	# full command prepparation
	
	def _os(self, cmd, *a, **kw):
		return self.prefix + cmd.format(*a,**kw)

	def _dfs(self, cmd, *a, **kw):
		return self.prefix + 'hdfs dfs -'+cmd.lstrip().format(*a,**kw)

	def _write(self, path, text, eof='EOF'):
		return pipe(
			'cat <<'+eof,
			self._os('"cat >{0}"'.format(path)),
			doc=[text,eof]
			)

	# --- scripts ----------------------------------------------------------

	def spark_run(self, code, script_path, spark_args='', remove=True, log=None):
		# TODO random/dynamic script path
		script = dedent(code).strip()
		self.write(script_path, script)
		log_str = ">{0}".format(log) if log else ''
		self.os("spark2-submit {1} {0} {2}", script_path, spark_args, log_str)
		if remove:
			self.os('rm -f '+script_path)
	
	def load_csv(self): pass # TODO

	def extract_csv(self, path, table, output_dir='', script_path='', config={}, spark_args='', aux='',
			sep=',', header=False, quote=None, escape=None, escape_quotes=None, quote_all=None, null_value=None, mode=None,
			select=None, drop=None, where=None, limit=None,
			remove='all',output_mode='>'):
		# TODO column names
		
		tmp_name = sha1(table.encode()+self.prefix).hexdigest()[:16]
		output_dir = output_dir or tmp_name
		script_path = script_path or '/tmp/{0}.py'.format(tmp_name)
		app_config_str = ''.join([".config('{0}','{1}')".format(k,v) for k,v in config.items()])
		
		# csv options
		csv_aux = ""
		if quote		is not None: csv_aux += ''',quote="""{0}"""'''.format(quote)
		if escape		is not None: csv_aux += ''',escape="""{0}"""'''.format(escape)
		if null_value 		is not None: csv_aux += ''',nullValue="""{0}"""'''.format(null_value)
		if escape_quotes	is not None: csv_aux += ''',escapeQuotes={0}'''.format(escape_quotes)
		if quote_all 		is not None: csv_aux += ''',quoteAll={0}'''.format(quote_all)
		
		# df options 
		df_aux = aux
		if select:
			df_aux += '''.selectExpr({0})'''.format(','.join(["'''{0}'''".format(x) for x in select]))
		if where:
			df_aux += """.where('''{0}''')""".format(where)
		if drop:
			df_aux += """.drop({0})""".format(','.join(["'''{0}'''".format(x) for x in drop]))
		if limit is not None:
			df_aux += '.limit({0})'.format(limit)
		
		code = """
			from pyspark.sql import SparkSession
			spark = SparkSession.builder.appName('utilx5 {1}'){6}.getOrCreate()
			df = spark.table('''{0}'''){2}
			df.write.csv('{1}',mode='overwrite',header={4},sep='''{5}'''{3})
			""".format(table, output_dir, df_aux, csv_aux, header, sep, app_config_str)
		code = re.sub('(?m)^\s+','',code) #UGLY FIX for multiline indented sql code
		
		self.dfs('rm -r -f {0}',output_dir)
		self.spark_run(code, script_path, spark_args=spark_args, remove=remove in ['all','script']) #, log=path+'.log')
		
		# TODO pipe into ???
		self.dfs('text {0}/part* {2}{1}',output_dir,path,output_mode) # getmerge?
		#self.dfs('getmerge {0}/part* {1}',output_dir,path)
		if remove.lower() in ['output','all']:
			self.dfs('rm -r -f {0}',output_dir)

if __name__=="__main__":
	#h=host('ssh userxxx@aaa.bbb.ccc.ddd.pl',run=False)
	#h.extract_csv('test.csv','testdb.example_table',limit=100,select=['a','b','c','string(d)'],drop=['a','b'],where='a < 10',header=True,config={'spark.driver.memory':'1g','spark.executor.memory':'1g'})
	h=host(r'C:\xxx\apps\putty\plink -i C:\xxx\apps\putty\id_rsa.ppk userxxx@aaa.bbb.ccc.ddd.pl')
	h.os('cat /proc/version')
	h.write('~/test_util.txt','to jest test')
	
	