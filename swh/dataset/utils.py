import subprocess


class ZSTWriter:
    def __init__(self, path, mode='w'):
        self.path = path
        self.mode = mode

    def __enter__(self):
        is_text = not (self.mode == 'wb')
        self.process = subprocess.Popen(
            ['zstd', '-q', '-o', self.path],
            text=is_text, stdin=subprocess.PIPE
        )
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.process.stdin.close()
        self.process.wait()

    def write(self, buf):
        self.process.stdin.write(buf)
