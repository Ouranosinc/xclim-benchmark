import pytest

kwargs = {
    '--with-client': dict(type=bool, help='whether to use a dask client'),
    '--chunk-size': dict(type=str, nargs='*', help='which function to run'),
    '--nworkers': dict(default=4, type=int, help='When using a dask client, number of threads per worker'),
    '--nthreads': dict(default=4, type=int, help='When using a dask client, number of threads per worker'),
    '--max-mem': dict(default='2GB', help='When using a dask client, memory limit'),
}
keys = list(kwargs.keys())

def get_kwargs(request):
    kwargs = {}
    for k in keys:
        kwargs[k] = request.config.getoption(k)
        if k == '--chunk-size':
            kwargs[k] = {arg.split("=")[0]:int(arg.split("=")[1]) for arg in kwargs[k] or []}
    return kwargs



def pytest_addoption(parser):
    for k in kwargs.keys():
        parser.addoption(k, **kwargs[k])

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    for k in kwargs.keys():
        parser.add_argument(k, **kwargs[k])
    args = parser.parse_args()
    args.chunk_size =  {arg.split("=")[0]:int(arg.split("=")[1]) for arg in args.chunk_size or []}
    def dict_to_string(dd, level=1):
        if isinstance(dd, dict):
            string = ""
            for k in dd.keys():
                string = string + "".join(["-"]*level)  + k
                string = string + dict_to_string(dd[k],level+1)
        else: 
            string = "_" + ("None" if dd is None else str(dd))
        return string
    def args_to_string(args):
        return dict_to_string(vars(args))
    print(args_to_string(args)[1:])