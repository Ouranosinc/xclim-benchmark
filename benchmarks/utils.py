from dask.distributed import Client
from conftest import get_kwargs

def start_benchmark(request):
    kwargs = get_kwargs(request)
    if kwargs['--with-client']:
        c = Client(n_workers=kwargs["--nworkers"], threads_per_worker=kwargs["--nthreads"], memory_limit=kwargs["--max-mem"])
    else :
        c = None
    return kwargs,c 
def end_benchmark(kwargs,c):
    if kwargs['--with-client']:
        c.close()
