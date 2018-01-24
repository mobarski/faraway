
# https://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html
# https://hadoop.apache.org/docs/r1.2.1/streaming.html
# http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
# https://blog.cloudera.com/blog/2013/01/a-guide-to-python-frameworks-for-hadoop/

def my_mapper():
	import sys
	import re
	for line in sys.stdin:
		for word in re.findall('(?u)\w+'):
			print word+'\t1'

def my_combiner():
	import sys
	curr_word = None
	value = None
	for line in sys.stdin:
		word,x = line.rstrip().split('\t')[:2]
		if word != curr_word:
			if curr_word is not None:
				print curr_word+'\t'+str(value)
			value = int(x)
		else:
			value += int(x)
		curr_word = word
	if curr_word is not None:
		print curr_word+'\t'+str(value)

def my_reducer():
	pass

def get_fun_body(f):
	import inspect
	import textwrap
	code = inspect.getsourcelines(f)
	return textwrap.dedent(''.join(code[0][1:]))

print(get_fun_body(my_combiner))
exit()

h.mapred(m=my_mapper, r=my_reducer, c=my_combiner, input='/test/in/*', output='/test/out')

"""
hadoop jar contrib/streaming/hadoop-*streaming*.jar \
-file /home/hduser/mapper.py    -mapper /home/hduser/mapper.py \
-file /home/hduser/reducer.py   -reducer /home/hduser/reducer.py \
-input /user/hduser/gutenberg/* -output /user/hduser/gutenberg-outputs
"""
