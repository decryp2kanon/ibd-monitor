"""Local CLI discovery; never start or stop a node."""
import os
from pathlib import Path
import shlex
import shutil

NODE_NAMES = {'bitcoind', 'bitcoin-qt', 'bitcoin-node', 'bitcoin-gui',
              'sugarchaind', 'sugarchain-qt', 'sugarchain-node', 'sugarchain-gui'}
RPC_FLAGS = ('-rpcport=', '-rpcuser=', '-rpcpassword=', '-rpccookiefile=', '-conf=',
             '-chain=', '-testnet=', '-testnet4=', '-signet=', '-regtest=')
CHAIN_DIRS = {'main': '', 'test': 'testnet3', 'testnet': 'testnet3',
              'testnet4': 'testnet4', 'signet': 'signet', 'regtest': 'regtest'}


def running_nodes(datadir):
    """Match an explicit node datadir, resolving relative paths against its cwd."""
    nodes = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            if proc.stat().st_uid != os.getuid():
                continue
            exe = (proc / 'exe').resolve(strict=True)
            if exe.name not in NODE_NAMES:
                continue
            args = (proc / 'cmdline').read_bytes().decode().split('\0')[1:]
            data = next((a.split('=', 1)[1] for a in args if a.startswith('-datadir=')), None)
            if data is None:
                continue  # Do not guess which default/config-selected datadir it uses.
            data = Path(data).expanduser()
            if not data.is_absolute():
                data = (proc / 'cwd').resolve(strict=True) / data
            if data.resolve() == datadir.resolve():
                flags = [a for a in args if a.startswith(RPC_FLAGS) or
                         a in ('-regtest', '-signet', '-testnet', '-testnet4')]
                nodes.append((exe, flags))
        except (OSError, ValueError, UnicodeError):
            continue
    return nodes


def sibling_cli(exe):
    family = 'bitcoin' if exe.name.startswith('bitcoin') else 'sugarchain'
    for p in (exe.parent / (family + '-cli'), exe.parent.parent / 'bin' / (family + '-cli')):
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return None


def configuration(script_dir, environ=None):
    env = os.environ if environ is None else environ
    root = Path(script_dir)
    home = Path(env.get('HOME', str(Path.home())))
    legacy_data = root / 'test_data'
    if env.get('IBD_DATADIR'):
        datadir = Path(env['IBD_DATADIR']).expanduser().resolve()
    else:
        datadir = next((p for p in (legacy_data, home / '.sugarchain', home / '.bitcoin')
                        if p.is_dir()), home / '.sugarchain').resolve()
    nodes = running_nodes(datadir)
    # More than one matching process is common with multiprocess GUI. Use shared
    # options only; conflicting node choices need an explicit override.
    options = []
    if nodes and all(flags == nodes[0][1] for _, flags in nodes):
        options = nodes[0][1][:]
    cli = env.get('IBD_CLI')
    if not cli and env.get('IBD_DAEMON'):
        daemon = shutil.which(env['IBD_DAEMON']) or env['IBD_DAEMON']
        cli = sibling_cli(Path(daemon).expanduser())
    if not cli:
        candidates = {sibling_cli(exe) for exe, _ in nodes} - {None}
        if len(candidates) == 1:
            cli = candidates.pop()
        elif len(candidates) > 1:
            raise ValueError('Multiple node CLIs match IBD_DATADIR; set IBD_CLI explicitly')
    if not cli:
        families = ('bitcoin', 'sugarchain') if datadir.name == '.bitcoin' else ('sugarchain', 'bitcoin')
        for family in families:
            candidates = [shutil.which(family + '-cli'),
                          str(root / family / 'src' / (family + '-cli')),
                          str(root / (family + '-core31') / 'build/bin' / (family + '-cli'))]
            cli = next((p for p in candidates if p and Path(p).is_file() and os.access(p, os.X_OK)), None)
            if cli:
                break
    cli = os.path.expanduser(cli or 'sugarchain-cli')
    overrides = {'IBD_RPC_PORT': 'rpcport', 'IBD_RPC_USER': 'rpcuser',
                 'IBD_RPC_PASSWORD': 'rpcpassword', 'IBD_RPC_CONNECT': 'rpcconnect',
                 'IBD_CONF': 'conf', 'IBD_CHAIN': 'chain'}
    explicit_rpc = any(key in env for key in overrides) or 'IBD_RPC_ARGS' in env
    # Preserve the original local test setup only when no node/config/cookie or
    # explicit RPC settings are available. Normal CLI config/cookie auth wins.
    if (not nodes and not explicit_rpc and datadir == legacy_data.resolve() and
            not any((datadir / name).exists() for name in
                    ('sugarchain.conf', 'bitcoin.conf', '.cookie'))):
        options = ['-rpcport=11324', '-rpcuser=rpcuser', '-rpcpassword=rpcpassword']
    for key, flag in overrides.items():
        if key in env:
            if flag == 'chain':
                options = [a for a in options if not a.startswith(('-chain=', '-regtest', '-signet', '-testnet'))]
            options = [a for a in options if not a.startswith('-' + flag + '=')]
            options.append('-' + flag + '=' + env[key])
    extra = shlex.split(env.get('IBD_RPC_ARGS', ''))
    if any(not a.startswith('-') for a in extra):
        raise ValueError('IBD_RPC_ARGS must contain only CLI options (-name=value)')
    options += extra
    chain = 'main'
    for arg in options:
        if arg.startswith('-chain='):
            chain = arg.split('=', 1)[1]
        for flag in ('regtest', 'signet', 'testnet', 'testnet4'):
            if arg in ('-' + flag, '-' + flag + '=1'):
                chain = flag
    network_dir = datadir / CHAIN_DIRS.get(chain, chain)
    debug_log = Path(env.get('IBD_DEBUG_LOG', str(network_dir / 'debug.log'))).expanduser()
    return datadir, cli, options, debug_log
