import time

def retry(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay ** i)