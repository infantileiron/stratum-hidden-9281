from src.services import run as service
def list_items(): return service.list_runs()
def detail(id): return service.detail(id)
def create(data): return service.create(data)
def retry(id): return service.retry(id)
