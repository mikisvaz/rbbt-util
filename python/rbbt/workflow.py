from . import cmd, run_job
import subprocess
import json
import time

def save_inputs(directory, inputs, types):
    return

class Workflow:
    def __init__(self, name):
        self.name = name

    def tasks(self):
        ruby=f'Workflow.require_workflow("{self.name}").tasks.keys * "\n"'
        return cmd(ruby).strip().split("\n")

    def task_info(self, name):
        ruby=f'Workflow.require_workflow("{self.name}").task_info("{name}").to_json'
        return cmd(ruby)

    def run(self, task, **kwargs):
        return run_job(self.name, task, **kwargs)

    def fork(self, task, **kwargs):
        path = run_job(self.name, task, fork=True, **kwargs)
        return Step(path)

class Step:
    def __init__(self, path):
        self.path = path
        self.info_content = None

    def info(self):
        if self.info_content:
            return self.info_content
        ruby=f'puts Step.load("{self.path}").info.to_json'
        txt = cmd(ruby)
        info_content = json.loads(txt)
        status = info_content["status"]
        if status == "done" or status == "error" or status == "aborted":
            self.info_content = info_content
        return info_content

    def status(self):
        return self.info()["status"]

    def done(self):
        return self.status() == 'done'

    def error(self):
        return self.status() == 'error'

    def aborted(self):
        return self.status() == 'aborted'

    def join(self):
        while not (self.done() or self.error() or self.aborted()):
            time.sleep(1)

    def load(self):
        ruby=f'puts Step.load("{self.path}").load.to_json'
        txt = cmd(ruby)
        return json.loads(txt)

