import requests


def request_(method, url,*args, **kwargs):
    '''
  requests.request() 方法
    '''
    s = requests.session()
    s.keep_alive = False
    resp = requests.request(method, url, *args, **kwargs)
    resp.raise_for_status()

    return resp