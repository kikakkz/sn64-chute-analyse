import paramiko


def execute_ssh_command(host, username, command):
    cli = paramiko.SSHClient()
    try:
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        cli.connect(hostname=host, username=username)
        _command = f'{command}'
        print(f'  \033[92m{command}\033[0m')
        stdin, stdout, stderr = cli.exec_command(_command)
        return (stderr.read().decode('utf-8'), stdout.read().decode('utf-8'))
    finally:
        cli.close()
