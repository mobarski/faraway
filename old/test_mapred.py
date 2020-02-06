import faraway

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

h = faraway.hadoop('','','')
h.mapred(m=my_mapper,r=my_combiner,c=my_combiner,input='/tmp/in',output='/tmp/out')
print(h.get_script())
print(h.get_after())

