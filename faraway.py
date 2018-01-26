import subprocess
import tempfile
import textwrap

# TODO: przepisac TODO do gita
# TODO: atomic copy -> filename.partial renamed to filename at the end
# TODO: hadoop streaming
# TODO: load partition
# TODO: load przez kopiowanie danych
# TODO: transform (hive sql)
# TODO: pobranie wartosci z wyniku SQLa do zmiennych
# TODO: testy automatyczne
# TODO: przyklady
# TODO: execute przyjmuje str/unicode jako stdin
# TODO: pobieranie metadanych

class unix:
	"unix host"
	def __init__(self, host='', ssh='ssh', scp='scp'):
		self.host = host
		self.ssh = ssh
		self.scp = scp
		self.var = {'host':host,'tmp_dir':'/tmp'}
		self.script = []
		self.after  = []
	
	def __enter__(self):
		return self
	
	def __exit__(self,et,ev,tb):
		#self.run() # TODO handle exceptions and ALWAYS run self.clean()
		self.clean()
	
	def clean(self):
		"remove temporary files"
		if self.after:
			after = self.get_after()
			print(after)
			out = self.execute_script(after)
			self.after = []
	
	def set(self, var_name, value):
		"set variable value"
		self.var[var_name] = value.format(**self.var)
		return self
	
	def get(self, var_name):
		"get variable value or None if var doesnt exists"
		return self.var.get(var_name)
	
	def tmp(self, text='', var='', eof='EOF', prefix='{tmp_dir}', suffix='', dedent=True, clean=True):
		"create temporary file and store its path in a variable"
		if dedent:
			text = textwrap.dedent(text).strip()
		path = prefix +'/'+ random_name(self.host, text, var) + suffix
		path = path.format(**self.var)
		if var:
			self.var[var] = path
		if text:
			self.script += ['cat >{0} <<{2}\n{1}\n{2}'.format(path,text.format(**self.var),eof)]
		else:
			self.script += ['touch '+path]
		if clean:
			self.after += ['rm '+path]
		return path
	
	def cmd(self, text):
		"add command to the main script"
		self.script += [text.format(**self.var)]
		return self

	def download(self, local_path, remote_path): # czy odwrotnie argumenty?
		"download file from host"
		self.run()
		cmd = '{0} {1}:{2} {3}'.format(self.scp, self.host, remote_path.format(**self.var), local_path.format(**self.var))
		print(cmd)
		out = subprocess.check_call(cmd, shell=True) # TODO check_call czy check_output
		return out
	
	def upload(self, local_path, remote_path): # czy odwrotnie argumenty?
		"upload file to host"
		cmd = '{0} {3} {1}:{2}'.format(self.scp, self.host, remote_path.format(**self.var), local_path.format(**self.var))
		print(cmd)
		out = subprocess.check_call(cmd, shell=True) # TODO check_call czy check_output
		return out

	def run(self):
		"run main script"
		if self.script:
			script = self.get_script()
			print(script)
			out = self.execute_script(script)
		else:
			out = None
		self.script = []
		return out

	def get_script(self):
		return '\n'.join(self.script) + '\nexit\n'

	def get_after(self):
		return '\n'.join(self.after) + '\nexit\n'

	def execute_script(self, text):
		"immediate execution of commands passed via stdin to ssh"
		cmd = self.ssh+' '+self.host
		with tempfile.TemporaryFile() as f:
			f.write(text)
			f.seek(0)
			out = subprocess.check_call(cmd, stdin=f, shell=True) # TODO check_call czy check_output
		return out

	def execute(self, cmd, stdin=None, before=''):
		"immediate execution of command passed via args to ssh"
		full_cmd = before + self.ssh +' '+ self.host +' '+ cmd
		full_cmd = full_cmd.format(**self.var)
		# TODO if type(stdin) is str: tempfile
		print(full_cmd)
		out = subprocess.check_call(full_cmd, stdin=stdin, shell=True)
		return out

# ------------------------------------------------------------------------------

class hadoop(unix):
	"hadoop host"
	
	def __init__(self, host='', ssh='ssh', scp='scp'):
		unix.__init__(self, host, ssh, scp)
		self.var['hive'] = 'hive'
		self.var['hdfs_tmp_dir'] = '/tmp'

	def download_from_hdfs(self, local_path, hdfs_path):
		self.run()
		self.execute('"hdfs dfs -text {0}/[^.]*" >{1}'.format(hdfs_path, local_path))
		
	def upload_into_hdfs(self, local_path, hdfs_path):
		self.run()
		self.execute('hdfs dfs -put - {0}'.format(hdfs_path), stdin=open(local_path,'r'))

	def pipe_from_hdfs(self, pipe_cmd, hdfs_path):
		self.run()
		self.execute('"hdfs dfs -text {0}/[^.]*" | {1}'.format(hdfs_path, pipe_cmd))
		
	def pipe_into_hdfs(self, pipe_cmd, hdfs_path, stdin=None):
		self.run()
		self.execute('hdfs dfs -put - {0}'.format(hdfs_path), before=pipe_cmd+' | ', stdin=stdin)

	def load(self, path, table, columns, sep=r'\t', clean=True):
		# TODO zalozenie tabeli w konkretnym miejsu i upload partycji???
		# TODO elegancka obsluga columns???
		# TODO table comment???
		name = random_name(self.host, path+' '+table, 'import')
		hdfs_path = '{}/{}'.format(self.var['hdfs_tmp_dir'],name)
		
		self.cmd('hdfs dfs -rm -r -f '+hdfs_path).run() # TODO jako opcja?
		self.upload_into_hdfs(path, hdfs_path)
		script = """
			DROP TABLE if exists {table};
			CREATE TABLE {table}
				({columns})
				ROW FORMAT delimited fields terminated by '{sep}'
				;
			LOAD DATA
				INPATH '{hdfs_path}'
				OVERWRITE INTO TABLE {table}
				;
			
		""".format(**locals())
		path = self.tmp(script, suffix='.sql')
		self.cmd('{hive} -f '+path)
		if clean:
			self.after += ['hdfs dfs -rm -r -f '+hdfs_path]
		self.run()
	
	def extract(self, path, sql, sep=r'\t', csep=',', ksep=':', clean=True):
		name = random_name(self.host,sql,'extract')
		hdfs_path = '{}/{}'.format(self.var['hdfs_tmp_dir'],name)
		
		#self.cmd('hdfs dfs -rm -r -f '+hdfs_path) # TODO jako opcja?
		script = """
		insert overwrite directory '{hdfs_path}'
			row format delimited
			fields terminated by '{sep}'
			collection items terminated by '{csep}'
			map keys terminated by '{ksep}'
			stored as textfile
			
			{sql}
			;
		""".format(**locals())
		sql_path = self.tmp(script, suffix='.sql')
		self.cmd('{hive} -f '+sql_path)
		if clean:
			self.after += ['hdfs dfs -rm -r -f '+hdfs_path]
		self.download_from_hdfs(path,hdfs_path)
	
	def transform(self, sql):
		sql_path = self.tmp(sql, suffix='.sql')
		self.cmd('{hive} -f '+sql_path)
	
	def mapred(self, m=None, r=None, c=None, input=None, output=None, mcnt=None, rcnt=None, verbose=False):
		# TODO tekst - kod pythona jako m,r,c
		# TODO sum,min,max reducers ???
		self.cmd('hdfs dfs -rm -r -f '+output) # TODO jako opcja
		cmd = "hadoop jar /opt/cloudera/parcels/CDH/jars/hadoop-streaming-2.6.0-mr1-cdh5.10.1.jar" # TODO jako zmienna
		# GENERIC OPTIONS
		if mcnt is not None:
			cmd += ' -D mapred.map.tasks='+str(mcnt)
		if m: # MAPPER
			if callable(m):
				m_py = self.tmp(get_fun_body(m), suffix='.py')
				cmd += ' -file {0} -mapper {0}'.format(m_py)
				self.cmd('chmod u+x '+m_py)
			else:
				cmd += """ -mapper '{0}'""".format(m)
		if r: # REDUCER
			if callable(r):
				r_py = self.tmp(get_fun_body(r), suffix='.py')
				cmd += ' -file {0} -reducer {0}'.format(r_py)
				self.cmd('chmod u+x '+r_py)
			else:
				cmd += """ -reducer '{0}'""".format(r)
		if c: # COMBINER
			if callable(c):
				c_py = self.tmp(get_fun_body(c), suffix='.py')
				cmd += ' -file {0} -combiner {0}'.format(c_py)
				self.cmd('chmod u+x '+c_py)
			else:
				cmd += """ -combiner '{0}'""".format(c)
		# INPUT
		if type(input) in (str,unicode):
			cmd += ' -input '+input
		else:
			for x in input:
				cmd += ' -input '+x
		# OUTPUT
		cmd += ' -output '+output
		# OTHER
		if rcnt is not None:
			cmd += ' -numReduceTasks '+str(rcnt)
		if verbose:
			cmd += ' -verbose'
		self.cmd(cmd)

# --- helpers ------------------------------------------------------------------

def random_name(text='',text2='',label='',length=6,sep='-'):
	import hashlib
	import random
	out = hashlib.sha1(text).hexdigest()[:length]
	out += sep+hashlib.sha1(text2).hexdigest()[:length]
	out += sep
	for i in range(length):
		out += random.choice('qwertyuiopasdfghjklzxcvbnm1234567890')
	if label:
		out += sep+label
	return out


def get_fun_body(f):
	import inspect
	import textwrap
	code = inspect.getsourcelines(f)
	return textwrap.dedent(''.join(code[0][1:]))


def columns(col_str,default='string',types_str='',sep1='\s+',sep2=':'):
	import re
	out = []
	types = {}
	for ts in re.split(sep1,types_str):
		if not ts: continue
		t,s = ts.split(sep2)[:2]
		types[t]=s
	for name_type in re.split(sep1,col_str):
		name,t = (name_type+sep2).split(sep2)[:2]
		t = types.get(t,default)
		out += [(name,t)]
	return ',  '.join([' '.join(c) for c in out])

