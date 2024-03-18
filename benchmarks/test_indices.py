import pytest 
from conftest import get_kwargs
from utils import start_benchmark, end_benchmark
from xclim.testing import open_dataset
from xclim import sdba
import xclim.indices as xci

ds = open_dataset("sdba/CanESM2_1950-2100.nc")
tx, pr = [ds[v].sel(time=slice("1970", "2000")) for v in ["tasmax", "pr"]]
def test_hot_spell_length(benchmark, request):
    kwargs, c = start_benchmark(request)
    def func():
         xci.hot_spell_total_length(tx).compute()
    benchmark(func)    
    end_benchmark(kwargs, c)

@pytest.mark.parametrize("freq", ["D", "MS", "YS"])
def test_liquid_precip_ratio(benchmark, request, freq):
    kwargs, c = start_benchmark(request)
    def func():
         xci.liquid_precip_ratio(tas=tx, pr=pr).compute()
    benchmark(func)    
    end_benchmark(kwargs, c)
