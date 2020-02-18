# Copyright (c) 2020 Maciej Obarski
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# version: 0.6

from __future__ import print_function
import os
import sys
import json
from tempfile import TemporaryFile

# CORE ssh+beeline obudowane cli/api "dla ludzi"

# TODO h.load
# TODO minimalizacja ilosci opcji
#      - show -> sql
#      - meta -> show
# TODO silent vs verbose
# TODO -- 0.7 release --
# TODO test named connections
# TODO test tags
# TODO render na konfiguracyjnych plikach json? {bin}{ssh}

DEFAULTS = {
	'ssh':'ssh',
	'cat':'cat',
	'beeline':'beeline',
	'hdfs_load_dir':'/tmp',
	'hdfs_tmp_dir':'/tmp',
	'tmp_dir':'/tmp',
	'cfg':'faraway.json'
}

class Host:


	def __init__(self, **kwargs):
		vars = {}
		vars.update(DEFAULTS)
		vars.update(kwargs)
		
		# CONFIG FILES
		for cfg_path in ['~/.faraway/config.json',vars['cfg']]:
			cfg_path = os.path.expanduser(cfg_path)
			if os.path.exists(cfg_path):
				cfg = json.load(open(cfg_path))
				# DEFAULTS
				vars.update(cfg)
				# TAGS
				if 'tags' in vars:
					for tag in vars['tags']:
						if tag in cfg:
							vars.update(cfg[tag])
				# NAME
				if 'name' in vars and vars['name'] in cfg:
					vars.update(cfg[vars['name']])
		
		# ENV VARIABLES
		for k,v in os.environ.items():
			if k.lower().startswith('faraway_'):
				name = k[len('faraway_'):]
				vars[name] = v
		#
		self.vars = vars

	
	# TODO ??? wybor czy env,plik itp sa przed czy po ARGUMENTS np FARAWAY_ vs faraway_
	def render(self, text, *dicts):
		vars = self.vars.copy()
		# ARGUMENTS
		for d in dicts:
			vars.update(d)
		# RENDER
		output = text.format(**vars)
		
		# VARIABLES STILL IN TEXT
		if re.findall('\{\w+\}',output):
			output = self.render(output,*dicts)
		
		return output



class hadoop(Host):

	# --- SHOW -----------------------------------------------------------------

	# https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-Beeline%E2%80%93CommandLineShell
	
	# TODO ??? kompresja ssh
	# INFO wydajnosc vs tworzenie pliku i pobranie z hdfs:
	#      show:   6s  dump:  48s  na:  1K rekordow ~ 52KB
	#      show:  65s  dump:  72s  na:  1M rekordow ~ 52MB
	#      show: 590s  dump: 110s  na: 10M rekordow ~ 523MB
	
	def show(self, sql, format='tsv2', header=False, raw=False, silent=False, no_col_prefix=True):
				
		# HEADER
		header_str = '--showHeader=' + ('true' if header else 'false')
				
		# OUTPUT FORMAT
		format_str = '--outputformat='+format
		
		# QUERY
		if sql:
			if no_col_prefix:
				sql = 'set hive.resultset.use.unique.column.names=false; '+sql
			sql = sql.replace('"',r'\\"')
			query_str = self.render('-e "{sql}"',locals())
		else:
			query_str = ''
		
		# OTHER
		other = []
		if silent:
			other += ['--silent=true']
		other_str = ' '.join(other)
		
		# SCRIPT
		script = '{beeline} {other_str} {header_str} {format_str} {query_str}'
		if not raw:
			internal = self.render(script,locals()).replace('"',r'\"')
			script = '{ssh} "{internal}"'
			
		# CMD
		cmd = self.render(script,locals())
		return cmd


	# --- DUMP -----------------------------------------------------------------
	
	def dump(self, sql, sep=r'\t', csep=',', ksep=':', silent=False, header=False):
		tmp_name = random_name() # TODO prefix as option
		hdfs_path = '{hdfs_tmp_dir}/{tmp_name}'
		sql_path = '{tmp_dir}/{tmp_name}.sql'
		script = """
				insert overwrite directory '{hdfs_path}'
					row format delimited
					fields terminated by '{sep}'
					collection items terminated by '{csep}'
					map keys terminated by '{ksep}'
					stored as textfile
					
					{sql}
					;
				"""
		script = self.render(dedent(script),locals())

		# OTHER
		other = []
		if silent:
			other += ['--silent=true']
		other_str = ' '.join(other)

		# SQL
		cmd1 = "cat >{sql_path}; {beeline} {other_str} -f {sql_path}; rm {sql_path}"
		cmd2 = self.render(cmd1,locals()).replace('"',r'\"')
		cmd = '{ssh} "{cmd2}"'
		cmd = self.render(cmd, locals())
		run(cmd, input=script, out='&2')
		
		if header:
			header_sql = re.sub(r'(?i)\blimit\s+\d+','',sql)
			header_sql += ' limit 0'
			header_cmd = self.show(header_sql,raw=True,header=True,silent=silent).replace('"',r'\"') # TODO separator
			cmd = '{ssh} "{header_cmd}; hdfs dfs -text {hdfs_path}/[^.]*; hdfs dfs -rm -r -f {hdfs_path}"'
		else:
			cmd = '{ssh} "hdfs dfs -text {hdfs_path}/[^.]*; hdfs dfs -rm -r -f {hdfs_path}"'
		return self.render(cmd, locals())
	
	# --- LOAD -----------------------------------------------------------------

	# TODO replicaton factor
	def load(self, path, table, columns, sep=r'\t', csep=',', ksep=':'):
		cmd = '{cat} {path} | {ssh} "{hdfs_put_cmd}; {sql_cmd}; {hdfs_rm_cmd}"'
		tmp_hdfs_path = '{hdfs_load_dir}/{table}'
		hdfs_put_cmd = 'hdfs dfs -put -f - {tmp_hdfs_path}'
		hdfs_rm_cmd  = 'hdfs dfs -rm -r -f {tmp_hdfs_path}'
		sql = """
			DROP TABLE if exists {table};
			CREATE TABLE {table}
				({columns})
				ROW FORMAT delimited
				FIELDS terminated by '{sep}'
				COLLECTION ITEMS terminated by '{csep}'
				MAP KEYS terminated by '{ksep}'
				;
			LOAD DATA
				INPATH '{tmp_hdfs_path}'
				OVERWRITE INTO TABLE {table}
				;
		"""
		sql = as_one_line(sql)
		sql = self.render(sql,locals())
		sql_cmd = self.show(sql,raw=True).replace('"',r'\"')
		return self.render(cmd,locals())


	# --- SQL ------------------------------------------------------------------
	
	def sql(self, sql, mode=1, echo=0, silent=0, no_col_prefix=True):
		if no_col_prefix:
			sql = 'set hive.resultset.use.unique.column.names=false; '+sql
		sql = self.render(sql)+'\n'
		cmd = self.show('',format='table',header=True,silent=silent)
		return run(cmd, input=sql, mode=mode, echo=echo)
		
	
	# --- COLUMNS --------------------------------------------------------------
	
	def columns(self, table):
		cmd = self.show('describe '+table)
		raw_meta = run(cmd,mode=3)
		lines = raw_meta.split('\n') # py3 ERROR
		out = []
		for line in lines:
			rec = line.split('\t')
			if len(rec)<2:
				continue
			out += ['{} {}'.format(rec[0],rec[1])]
		return ', '.join(out)


	# --- META -----------------------------------------------------------------
	
	def meta(self, table):
		cmd = self.show('show create table '+table,silent=True) # XXX silent=silent
		raw_meta = run(cmd,mode=3)
		meta = {}
		
		meta['create_table'] = raw_meta
		
		# properties
		for (k,v) in re.findall("'([^']+)'='([^']+)'",raw_meta): # py3 ERROR
			if v.isdigit():
				v=int(v)
			meta[k]=v
			
		# location
		loc=re.findall("'(hdfs://[^']+)'",raw_meta)
		if loc:
			meta['location'] = loc[0]
		
		return meta


# ---[ UTILITY ]----------------------------------------------------------------

import sys
import subprocess as sp

# TODO py3: return str vs bytes-like
def run(cmd, out=None, err=None, input=None, aux=None, aux2=None, mode=None, echo=False):
	script = cmd
	if aux:
		script = script + ' ' + aux
	if out:
		script = script + ' >' + out
	if err:
		script = script + ' 2>' + err
	if aux2:
		script = script + ' ' + aux2
	if input:
		f_in = TemporaryFile("w+")
		f_in.write(input)
		f_in.seek(0)
	else:
		f_in = None
	if echo:
		print('\nCMD:',script,file=sys.stderr)
		print('\nINPUT:',file=sys.stderr)
		print(input,file=sys.stderr)
		print('',file=sys.stderr)
	# execute
	if mode==0:
		pass
	elif mode==1 or mode is None:
		return sp.check_call(script, shell=True, stdin=f_in)
	elif mode==2:
		return sp.call(script, shell=True, stdin=f_in)
	elif mode==3:
		return sp.check_output(script, shell=True, stdin=f_in)
	elif mode==4:
		return sp.Popen(script, shell=True, stdin=f_in, stdout=sp.PIPE)
	else:
		raise Exception('Unsupported mode: {}'.format(mode))


import re

def as_one_line(text):
	text = text.strip()
	text = re.sub('\s+',' ',text)
	return text

from time import strftime
from random import sample

def random_name(prefix='faraway'):
	rnd = ''.join(sample("qwertyuiopasdfghjklzxcvbnm",4))
	return strftime(prefix+'_%Y%m%d_%H%M%S_'+rnd)

# ---[ CLI ]--------------------------------------------------------------------

import argparse
from textwrap import dedent

parser = argparse.ArgumentParser(add_help=False
	,formatter_class=argparse.RawDescriptionHelpFormatter
	,description="Remote Hadoop operations over ssh"
	,epilog=dedent(
		"""
		Actions:
		
		* show query                  write query results to stdout (low latency)
		* dump query                  write query results to stdout (high throughput)
		* load path table columns     load data from local file into table
		* sql                         execute sql script from standard input
		* meta table                  fetch table metadata (show create results)
		* vars                        show faraway variables
		
		Configuration:
		
		* faraway loads configuration from following .json files:
		  - from user directory:
		      C:\\Users\\my_user_name\\.faraway\\config.json   on Windows
		      ~/.faraway/config.json                           on Linux
		  - from current directory: ./faraway.py
		  
		* faraway reads environment variables prefixed with "faraway_",
		  strips the prefix and uses them as faraway variables

		Show vs Dump:
		
		* TODO

		Examples:
		
		* faraway show -h "select * from stage.movies"
		* faraway dump "select * from stage.movies" >movies.tsv
		* faraway load -h movies.tsv stage.movies "id int, title string, year string"
		* faraway meta stage.movies
		* faraway sql - <transform.sql
		* echo "drop table if exists {env}.movies" | faraway_env=test faraway sql
		* echo "select * from stage.movies" | faraway show -h -
		* echo "select * from stage.movies" | faraway dump -h - >movies.tsv
		* faraway vars -n my_conn
		
		  where faraway is an alias:
		  - doskey faraway=python3 faraway.py $*    on Windows
		  - alias faraway=python3 faraway.py        on Linux
		"""
		)
	)
parser.add_argument('--help',help='print help',action='help')
parser.add_argument('-h','--header',help='include header',action='store_const',const=1)
parser.add_argument('-s','--silent',help='silent beeline',action='store_const',const=1)
parser.add_argument('-e','--echo',help='echo commands',action='store_const',const=1)
parser.add_argument('action',type=str,help='action',choices=['show','dump','load','sql','meta','vars'])
parser.add_argument('-f',type=str,help='file format (as in beeline)',choices=['tsv2','csv2','dsv','table','vertical','xmlattr','xmlelements','tsv','csv'])
parser.add_argument('args',help='action specific arguments (described below)')
parser.add_argument('-n','--name',type=str,help='connection name')
parser.add_argument('-t','--tags',type=str,help='connection tags (comma separated)')

# ------------------------------------------------------------------------------

if __name__=="__main__":
	
	args = parser.parse_args()
	action = args.action
	header = args.header==1
	silent = args.silent==1
	echo = args.echo==1
	#
	h = hadoop(cfg='tvn_faraway.json') # XXX
	if action=='show':
		sql = args.args
		if sql=='-': sql=sys.stdin.read().rstrip()
		cmd = h.show(sql,header=header,silent=silent)
		run(cmd,mode=1,echo=echo)
	elif action=='dump':
		sql = args.args
		if sql=='-': sql=sys.stdin.read().rstrip()
		cmd = h.dump(sql,header=header,silent=silent)
		run(cmd,mode=1,echo=echo)
	elif action=='sql':
		sql = args.args
		if sql=='-': sql=sys.stdin.read().rstrip()
		h.sql(sql,echo=echo,silent=silent)
	elif action=='load': # TODO # TODO # TODO # TODO # TODO # TODO # TODO
		exit(1)
	elif action=='meta':
		table = args.args
		meta = h.meta(table)
		print(meta['create_table'].rstrip())
	elif action=='vars':
		for k,v in sorted(h.vars.items()):
			print('{} = {}'.format(k,v)) # TODO pprint json
