
import app.data
from subprocess import PIPE, run

def test_helloworld():
	run('flask data test')
	command = ['flask', 'data', 'test']
	result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
	assert result.stdout == 'Hello World\n'