"""TCP regression tests. Run cargo build, then python3 tests/transactions.py."""
import socket
import subprocess
import time
import unittest
from pathlib import Path


class Client:
    port = 6379
    def __init__(self):
        self.socket = socket.create_connection(('127.0.0.1', self.port), timeout=2)
        self.reader = self.socket.makefile('rb')

    def close(self):
        self.reader.close()
        self.socket.close()

    def command(self, *args):
        parts = [str(arg).encode() for arg in args]
        self.socket.sendall(b'*%d\r\n' % len(parts) + b''.join(
            b'$%d\r\n' % len(part) + part + b'\r\n' for part in parts))
        return self.response()

    def response(self):
        line = self.reader.readline()
        if not line:
            raise AssertionError('server closed connection')
        kind, data = line[:1], line[1:-2]
        if kind in (b'+', b'-'):
            return line[:-2]
        if kind == b':':
            return int(data)
        if kind == b'$':
            length = int(data)
            if length == -1:
                return None
            value = self.reader.read(length)
            assert self.reader.read(2) == b'\r\n'
            return value
        if kind == b'*':
            return 'NULL_ARRAY' if data == b'-1' else [self.response() for _ in range(int(data))]
        raise AssertionError(line)


class Transactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Select an isolated port; never connect to an existing Redis server.
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            Client.port = probe.getsockname()[1]
        root = Path(__file__).resolve().parents[1]
        cls.server = subprocess.Popen([str(root / 'target/debug/codecrafters-redis'),
                                       '--port', str(Client.port)],
                                      stdout=subprocess.DEVNULL)
        cls.addClassCleanup(cls.stop_server)
        for _ in range(100):
            if cls.server.poll() is not None:
                raise RuntimeError('server exited during startup')
            try:
                client = Client()
                client.close()
                return
            except ConnectionRefusedError:
                time.sleep(.02)
        raise RuntimeError('server startup timed out')

    @classmethod
    def stop_server(cls):
        cls.server.terminate()
        cls.server.wait(timeout=5)

    def setUp(self):
        self.a, self.b, self.c, self.d = [Client() for _ in range(4)]
        for client in (self.a, self.b, self.c, self.d):
            self.addCleanup(client.close)
        self.key = self.id()

    def watch_queue(self, key):
        self.assertEqual(self.a.command('WATCH', key), b'+OK')
        self.assertEqual(self.a.command('MULTI'), b'+OK')
        self.assertEqual(self.a.command('SET', self.key + ':out', 'new'), b'+QUEUED')

    def test_wait_a_zero_returns_immediately(self):
        started = time.monotonic()
        self.assertEqual(self.a.command('WAIT', 0, 60000), 0)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertTrue(self.a.command('WAIT', 0).startswith(b'-ERR'))

    def test_wait_returns_all_connected_replicas(self):
        def complete_handshake(replica, listening_port):
            self.assertEqual(replica.command('PING'), b'+PONG')
            self.assertEqual(
                replica.command('REPLCONF', 'listening-port', listening_port),
                b'+OK',
            )
            self.assertEqual(
                replica.command('REPLCONF', 'capa', 'psync2'), b'+OK',
            )
            self.assertTrue(
                replica.command('PSYNC', '?', -1).startswith(b'+FULLRESYNC '),
            )
            header = replica.reader.readline()
            self.assertTrue(header.startswith(b'$'))
            replica.reader.read(int(header[1:-2]))

        for index, replica in enumerate((self.a, self.b, self.c)):
            complete_handshake(replica, 16380 + index)

        for requested_replicas in (0, 3, 9):
            self.assertEqual(self.d.command('WAIT', requested_replicas, 500), 3)

    def test_receive_replication_handshake(self):
        self.assertEqual(self.a.command('PING'), b'+PONG')
        self.assertEqual(self.a.command('REPLCONF', 'listening-port', 6380), b'+OK')
        self.assertEqual(self.a.command('REPLCONF', 'capa', 'psync2'), b'+OK')
        self.assertEqual(self.a.command('replconf', 'capa', 'psync2'), b'+OK')
        self.assertTrue(self.a.command('REPLCONF').startswith(b'-ERR'))
        info = self.a.command('INFO', 'replication')
        fields = dict(line.split(b':', 1) for line in info.splitlines() if line)
        self.assertEqual(fields[b'role'], b'master')
        self.assertEqual(len(fields[b'master_replid']), 40)
        self.assertEqual(fields[b'master_repl_offset'], b'0')

    def test_write_propagation_after_rdb(self):
        replica = self.a
        self.assertEqual(replica.command('PING'), b'+PONG')
        self.assertEqual(replica.command('REPLCONF', 'listening-port', 16380), b'+OK')
        self.assertEqual(replica.command('REPLCONF', 'capa', 'eof', 'capa', 'psync2'), b'+OK')
        fullresync = replica.command('PSYNC', '?', -1)
        self.assertTrue(fullresync.startswith(b'+FULLRESYNC '))
        header = replica.reader.readline()
        self.assertTrue(header.startswith(b'$'))
        length = int(header[1:-2])
        expected = bytes.fromhex(
            '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d'
            '62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa'
            '08616f662d62617365c000fff06e3bfec0ff5aa2')
        self.assertEqual(replica.reader.read(length), expected)
        # These must not appear on the replication stream.
        self.assertEqual(self.b.command('PING'), b'+PONG')
        self.assertEqual(self.b.command('ECHO', 'hello'), b'hello')
        self.b.command('GET', self.key)
        self.assertTrue(self.b.command('SET', self.key).startswith(b'-ERR'))
        for index in range(3):
            key = self.key + str(index)
            self.assertEqual(self.b.command('SET', key, index), b'+OK')
        for index in range(3):
            self.assertEqual(replica.response(),
                             [b'SET', (self.key + str(index)).encode(), str(index).encode()])
        # Discarded writes must not leak into the replication queue.
        self.b.command('MULTI')
        self.b.command('SET', self.key, 'discarded')
        self.b.command('DISCARD')
        self.b.command('MULTI')
        self.b.command('SET', self.key, 'committed')
        self.assertEqual(self.b.command('EXEC'), [b'+OK'])
        self.assertEqual(replica.response(), [b'SET', self.key.encode(), b'committed'])

    def test_four_client_scenarios(self):
        a, b, c, d = self.a, self.b, self.c, self.d
        a.command('SET', 'foo', 100)
        a.command('SET', 'bar', 200)
        a.command('WATCH', 'foo')
        a.command('MULTI')
        self.assertEqual(a.command('SET', 'bar', 300), b'+QUEUED')
        b.command('SET', 'foo', 200)
        self.assertEqual(a.command('EXEC'), 'NULL_ARRAY')
        self.assertEqual(a.command('GET', 'bar'), b'200')
        c.command('SET', 'baz', 100)
        c.command('SET', 'caz', 200)
        c.command('WATCH', 'baz')
        c.command('MULTI')
        c.command('SET', 'caz', 400)
        d.command('SET', 'caz', 300)
        self.assertEqual(c.command('EXEC'), [b'+OK'])
        self.assertEqual(d.command('GET', 'caz'), b'400')

    def test_mutation_paths(self):
        for command, initial in [('SET', ('SET', '1')), ('INCR', ('SET', '1')),
                                 ('RPUSH', ('RPUSH', 'x')), ('LPUSH', ('RPUSH', 'x')),
                                 ('LPOP', ('RPUSH', 'x')), ('BLPOP', ('RPUSH', 'x')),
                                 ('XADD', ('XADD', '1-0', 'f', 'v'))]:
            with self.subTest(command=command):
                key = self.key + command
                self.b.command(initial[0], key, *initial[1:])
                self.watch_queue(key)
                args = {'SET': ['1'], 'INCR': [], 'RPUSH': ['y'], 'LPUSH': ['y'],
                        'LPOP': [], 'BLPOP': [0], 'XADD': ['2-0', 'f', 'v']}[command]
                self.b.command(command, key, *args)
                self.assertEqual(self.a.command('EXEC'), 'NULL_ARRAY')
                self.assertIsNone(self.a.command('GET', self.key + ':out'))

    def test_other_transaction_writes(self):
        for command in ('SET', 'INCR'):
            key = self.key + command
            self.b.command('SET', key, 1)
            self.watch_queue(key)
            self.b.command('MULTI')
            self.b.command(command, key, *([2] if command == 'SET' else []))
            self.b.command('EXEC')
            self.assertEqual(self.a.command('EXEC'), 'NULL_ARRAY')

    def test_repeat_watch_and_restored_value(self):
        self.b.command('SET', self.key, 1)
        self.a.command('WATCH', self.key)
        self.b.command('SET', self.key, 2)
        self.b.command('SET', self.key, 1)
        self.watch_queue(self.key)
        self.assertEqual(self.a.command('EXEC'), 'NULL_ARRAY')

    def test_expiry_without_read(self):
        self.b.command('SET', self.key, 1, 'PX', 80)
        self.watch_queue(self.key)
        time.sleep(.12)
        self.assertEqual(self.a.command('EXEC'), 'NULL_ARRAY')

    def test_cleanup_and_empty_exec(self):
        for finish in ('EXEC', 'DISCARD', 'ABORT'):
            self.a.command('WATCH', self.key)
            self.a.command('MULTI')
            if finish == 'ABORT':
                self.b.command('SET', self.key, 1)
            self.a.command('EXEC' if finish == 'ABORT' else finish)
            self.b.command('SET', self.key, 2)
            self.a.command('MULTI')
            self.assertEqual(self.a.command('EXEC'), [])

    def test_failed_write_and_zero_pop(self):
        self.b.command('SET', self.key, 'text')
        self.watch_queue(self.key)
        self.assertTrue(self.b.command('INCR', self.key).startswith(b'-ERR'))
        self.assertEqual(self.a.command('EXEC'), [b'+OK'])
        key = self.key + ':list'
        self.b.command('RPUSH', key, 'x')
        self.watch_queue(key)
        self.assertEqual(self.b.command('LPOP', key, 0), [])
        self.assertEqual(self.a.command('EXEC'), [b'+OK'])

    def test_order_errors_and_nested_multi(self):
        a = self.a
        self.assertTrue(a.command('EXEC').startswith(b'-ERR'))
        a.command('MULTI')
        a.command('SET', self.key, 'text')
        self.assertTrue(a.command('MULTI').startswith(b'-ERR'))
        self.assertTrue(a.command('watch', self.key).startswith(b'-ERR'))
        a.command('INCR', self.key)
        a.command('GET', self.key)
        result = a.command('EXEC')
        self.assertEqual(result[0], b'+OK')
        self.assertTrue(result[1].startswith(b'-ERR'))
        self.assertEqual(result[2], b'text')


if __name__ == '__main__':
    unittest.main(verbosity=2)
