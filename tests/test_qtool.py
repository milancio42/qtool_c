import unittest
import subprocess

QTOOL = 'build/release/qtool'
TESTDB = 'tests/testdb'
HEADER = 'hostname,start_time,end_time'

class QtoolTest(unittest.TestCase):
    def cmd(self, cmd, data=''):
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1024*1024
        )
        stdout, stderr = proc.communicate(data)

        return (stdout, proc.returncode)

    def test_workers_not_set(self):
        cmd = [QTOOL, TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        out, _ = self.cmd(cmd, data.encode('utf-8'))
        # extract the number of queries which returned some data (3rd line), the number after ':'
        num_queries_ok = out.split(b'\n')[2].split(b':')[1].strip()
        self.assertTrue(int(num_queries_ok), 1)

    def test_workers_set(self):
        cmd = [QTOOL, '-w', '2', TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        out, _ = self.cmd(cmd, data.encode('utf-8'))
        # extract the number of queries which returned some data (3rd line), the number after ':'
        num_queries_ok = out.split(b'\n')[2].split(b':')[1].strip()
        self.assertTrue(int(num_queries_ok) == 1)

    def test_workers_set_wrong(self):
        cmd = [QTOOL, '-w', '0', TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        _, rc = self.cmd(cmd, data.encode('utf-8'))
        self.assertTrue(rc != 0)

        cmd = [QTOOL, '-w', '-1', TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        _, rc = self.cmd(cmd, data.encode('utf-8'))
        self.assertTrue(rc != 0)

        cmd = [QTOOL, '-w', '17', TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        _, rc = self.cmd(cmd, data.encode('utf-8'))
        self.assertTrue(rc == 0)

    def test_empty_query_params(self):
        cmd = [QTOOL, TESTDB]
        data = '\n'.join([HEADER])
        out, _ = self.cmd(cmd, data.encode('utf-8'))
        # extract the number of queries which returned some data (3rd line), the number after ':'
        num_queries_ok = out.split(b'\n')[2].split(b':')[1].strip()
        self.assertTrue(int(num_queries_ok) == 0)

    def test_sql_inject(self):
        cmd = [QTOOL, TESTDB]
        data = '\n'.join([HEADER, 'host_000008;SELECT * FROM CPU_USAGE;,2017-01-01 08:59:22,2017-01-01 09:59:22'])
        out, _ = self.cmd(cmd, data.encode('utf-8'))
        num_queries_ok = out.split(b'\n')[2].split(b':')[1].strip()
        self.assertTrue(int(num_queries_ok) == 0)

    def test_input_incomplete(self):
        cmd = [QTOOL, TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22'])
        _, rc = self.cmd(cmd, data.encode('utf-8'))
        self.assertTrue(rc != 0)
    
    def test_input_too_long(self):
        cmd = [QTOOL, TESTDB]
        data = '\n'.join([HEADER, 'host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22,Upssss'])
        _, rc = self.cmd(cmd, data.encode('utf-8'))
        self.assertTrue(rc != 0)

if __name__ == '__main__':
    unittest.main()

