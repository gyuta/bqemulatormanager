import subprocess


class Emulator:

    def __init__(self, project_name: str, port: int, launch_emulator: bool = True):
        self.project_name = project_name
        if launch_emulator:
            self.running_flg = True
            self.proc = subprocess.Popen(f"bigquery-emulator --project={project_name} --port={port}", shell=True)
        self.structure = {}

    def __del__(self):
        if self.running_flg:
            self.proc.terminate()
