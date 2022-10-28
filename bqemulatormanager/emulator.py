import subprocess

class PortOccupiedError(Exception):
    pass

class Emulator:
    running_flg = False

    def __init__(self, project_name: str, port: int, launch_emulator: bool = True, debug_mode:bool = False):
        self.project_name = project_name

        if launch_emulator:
            log_level = 'debug' if debug_mode else 'info'
            
            if is_port_in_use(port):
                raise PortOccupiedError(f'port {port} is occupied.')
            self.running_flg = True
            self.proc = subprocess.Popen(f"bigquery-emulator --project={project_name} --port={port} --log-level={log_level}", shell=True)
        self.structure = {}

    def __del__(self):
        if self.running_flg:
            self.proc.terminate()

def is_port_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0