import faraway

def test1():
	with faraway.unix('','cat','cat') as h:
		h.set('hive','beeline -u "jdbc:hive2://xxx.aaa.bbb.ccc:10000/;principal=hive/xxx.aaa.bbb@zzz.vvv.bbb?mapreduce.job.queuename=abcd" --showHeader=false')
		h.set('x','abc')
		h.set('y','test {x} ok')
		h.tmp('xxx','f1')
		h.tmp('yyy','f2')
		h.tmp('{y}','f3')
		h.tmp(var='f4')
		h.cmd('{hive} -f {f1} >{f4}')
		s = h.get_script()
		print(s)
		h.run()
		h.download('test_f3.txt','{f3}')


def test2():
	print(faraway.columns('a b c d:i e:i f:ai','string','i:bigint ai:array<int>'))

def test3():
	with faraway.unix('','cat','cat') as h:
		h.set('hive','beeline')
		cols = columns('a b c d')
		h.load('ppp','ttt','idir',cols)
		print(h.get_script())

if __name__=="__main__":
	test2()
