"""Isolation guard installed only into the disposable audit subprocess."""
import os
from pathlib import Path
import re
import socket
import sys

BASE=Path(os.environ['CTS_AUDIT_BASE']).resolve()
DEVNULL=Path(os.devnull).resolve()
FORBIDDEN=[Path(r'F:\9_Crypto\crypto_trading_system').resolve(),Path(r'F:\9_Crypto\_radar_refactor').resolve()]
owned_ports=set(); original_bind=socket.socket.bind


def bind(self,address):
    if not isinstance(address,tuple) or address[0] not in ('127.0.0.1','localhost','::1'):
        raise PermissionError('AUDIT_GUARD: test-owned loopback binds only')
    result=original_bind(self,address);owned_ports.add(self.getsockname()[1]);return result
socket.socket.bind=bind


def path_check(value,writing=False):
    if not isinstance(value,(str,bytes,os.PathLike)): return
    p=Path(os.fsdecode(value)).resolve(strict=False)
    if p==DEVNULL: return
    if any(p==root or p.is_relative_to(root) for root in FORBIDDEN):
        raise PermissionError('AUDIT_GUARD: original workspace access blocked')
    if writing and not p.is_relative_to(BASE):
        raise PermissionError('AUDIT_GUARD: writes outside disposable audit directory blocked')
    if not p.is_relative_to(BASE) and (p.name.startswith('.env') or p.name=='keys.txt'):
        raise PermissionError('AUDIT_GUARD: external credential files blocked')


def audit(event,args):
    if event=='open':
        mode=args[1] or '';flags=args[2] or 0
        writing=any(c in mode for c in 'wax+') or bool(flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
        path_check(args[0],writing)
    elif event in ('os.remove','os.rmdir','os.mkdir','os.chmod','os.utime','os.truncate'):
        path_check(args[0],True)
    elif event in ('os.rename','os.link','os.symlink'):
        path_check(args[0],True);path_check(args[1],True)
    elif event in ('os.listdir','os.scandir'):
        path_check(args[0])
    elif event=='socket.connect':
        a=args[1]
        if not (isinstance(a,tuple) and a[0] in ('127.0.0.1','localhost','::1') and a[1] in owned_ports):
            raise PermissionError('AUDIT_GUARD: existing services and external connections blocked')
    elif event=='socket.getaddrinfo' and args[0] not in ('127.0.0.1','localhost','::1',None):
        raise PermissionError('AUDIT_GUARD: external DNS blocked')
    elif event in ('os.system','os.exec','os.posix_spawn'):
        raise PermissionError('AUDIT_GUARD: shell execution blocked')
    elif event=='subprocess.Popen':
        candidate=args[0]
        if not candidate:
            command=args[1]
            candidate=command[0] if isinstance(command,(tuple,list)) else re.match(r'^(?:"([^"]+)"|(\S+))',command).group(0).strip('"')
        if Path(str(candidate)).name.lower() not in ('python.exe','python','powershell.exe'):
            raise PermissionError('AUDIT_GUARD: unreviewed subprocess blocked')
sys.addaudithook(audit)
