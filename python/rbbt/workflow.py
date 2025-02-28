import requests
import logging
import json
from urllib.parse import urlencode, urljoin
from time import sleep
import itertools

def request_post(url, params):
    response = requests.post(url, params)
    return response

def request_get(url, params):
    query = urlencode(params)
    full_url = f"{url}?{query}"
    response = requests.get(full_url)
    return response

def get_json(url, params={}):
    params['_format'] = 'json'
    response = request_get(url, params)
    if response.status_code == 200:
        return json.loads(response.content)  # parse the JSON content from the response
    else:
        logging.error("Failed to initialize remote tasks")

def get_raw(url, params={}):
    params['_format'] = 'raw'
    response = request_get(url, params)
    if response.status_code == 200:
        return response.content  # parse the JSON content from the response
    else:
        logging.error("Failed to initialize remote tasks")

def join(url, *subpaths):
    return url + "/" + "/".join(subpaths)

class RemoteStep:
    def __init__(self, url):
        self.url = url

    def info(self):
        return get_json(join(self.url, 'info'))
    def status(self):
        return self.info()['status']

    def done(self):
        return self.status() == 'done'

    def error(self):
        return self.status() == 'error' or self.status() == 'aborted'

    def running(self):
        return not (self.done() or self.error())

    def wait(self, time=1):
        while self.running():
            sleep(time)


    def raw(self):
        return get_raw(self.url)

    def json(self):
        return get_json(self.url)

class RemoteWorkflow:
    def __init__(self, url):
        self.url = url
        self.task_exports = {}
        self.init_remote_tasks()

    def init_remote_tasks(self):
        self.task_exports = get_json(self.url)
        self.tasks = []
        self.tasks += self.task_exports['asynchronous']
        self.tasks += self.task_exports['synchronous']
        self.tasks += self.task_exports['exec']

    def task_info(self, name):
        return get_json(join(self.url, name, '/info'))

    def job(self, task, **kwargs):
        kwargs['_format'] = 'jobname'
        response = request_post(join(self.url, task), kwargs)
        if response.status_code == 200:
            jobname = response.content.decode('utf-8')
            step_url = join(self.url, task, jobname)
            print(step_url)
            return RemoteStep(step_url)
        else:
            logging.error("Failed to initialize remote tasks")


if __name__ == "__main__":
    wf = RemoteWorkflow('http://localhost:1900/Baking')
    print(wf.tasks)
    print(wf.task_info('bake_muffin_tray'))

    step = wf.job('bake_muffin_tray', blueberries=True)
    step.wait()
    print(step.json())



