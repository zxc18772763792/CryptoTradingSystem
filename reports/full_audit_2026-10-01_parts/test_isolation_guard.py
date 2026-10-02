import os, sys, socket, pathlib, re
ORIGINAL = os.path.normcase(os.path.abspath(r'F:\9_Crypto\crypto_trading_system'))
RADAR = os.path.normcase(os.path.abspath(r'F:\9_Crypto\_radar_refactor'))
_owned_ports = set()
_original_bind = socket.socket.bind

def isolated_bind(self, address):
    if isinstance(address, tuple) and address[0] in ('127.0.0.1', 'localhost', '::1'):
        result = _original_bind(self, address)
        _owned_ports.add(self.getsockname()[1])
        return result
    raise PermissionError('AUDIT_GUARD: socket bind restricted to test-owned loopback')
socket.socket.bind = isolated_bind

def audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes)):
        p = os.path.normcase(os.path.abspath(os.fsdecode(args[0])))
        if p == ORIGINAL or p.startswith(ORIGINAL + os.sep) or p == RADAR or p.startswith(RADAR + os.sep):
            raise PermissionError('AUDIT_GUARD: original working tree access blocked')
    if event == 'socket.connect':
        address = args[1]
        if not (isinstance(address, tuple) and address[0] in ('127.0.0.1', 'localhost', '::1') and address[1] in _owned_ports):
            raise PermissionError('AUDIT_GUARD: real network connection blocked')
    if event == 'socket.getaddrinfo' and args[0] not in ('127.0.0.1', 'localhost', '::1', None):
        raise PermissionError('AUDIT_GUARD: external DNS blocked')
    if event in ('os.system', 'os.exec', 'os.posix_spawn'):
        raise PermissionError('AUDIT_GUARD: shell execution blocked')
    if event == 'subprocess.Popen':
        candidate = args[0]
        if not candidate:
            command = args[1]
            candidate = command[0] if isinstance(command, (list, tuple)) else re.match(r'^(?:"([^"]+)"|(\S+))', command).group(0).strip('"')
        exe = pathlib.Path(str(candidate)).name.lower()
        if exe not in ('python.exe', 'python', 'powershell.exe'):
            raise PermissionError('AUDIT_GUARD: unreviewed subprocess blocked')
sys.addaudithook(audit)
