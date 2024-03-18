import pytest 
from conftest import get_kwargs
from utils import start_benchmark, end_benchmark
from xclim.testing import open_dataset
from xclim import sdba

dsr = open_dataset("sdba/CanESM2_1950-2100.nc")
dsh = open_dataset("sdba/ahccd_1950-2013.nc"  )
ref, hist = [ds.tasmax.sel(time=slice("1970", "2000")) for ds in [dsr, dsh]]

@pytest.mark.parametrize("nquantiles", [20,50])
def test_eqm(benchmark, request, nquantiles):
    kwargs, c = start_benchmark(request)
    ref =  ref.chunk(kwargs["--chunk-size"])
    hist = hist.chunk(kwargs["--chunk-size"])
    def func():
        sdba.QuantileDeltaMapping.train(ref,hist, nquantiles=nquantiles).adjust(hist).compute()
    benchmark(func)    
    end_benchmark(kwargs, c)

@pytest.mark.parametrize("nquantiles", [20,50])
def test_qdm(benchmark, request, nquantiles):
    kwargs, c = start_benchmark(request)
    ref =  ref.chunk(kwargs["--chunk-size"])
    hist = hist.chunk(kwargs["--chunk-size"])
    def func():
        sdba.QuantileDeltaMapping.train(ref,hist, nquantiles=nquantiles).adjust(hist).compute()
    benchmark(func)    
    end_benchmark(kwargs, c)
