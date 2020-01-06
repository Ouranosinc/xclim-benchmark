# Imports
import pyperf as perf
# import icclim
import numpy as np

x = np.array(np.random.rand(1000))
P = np.linspace(0.01, 0.99, 50)


def bench_argsort():
    # np.quantile(x, P)
    x.argsort().argsort() / len(x)


def bench_quantile():
    np.quantile(x, P)
    # x.argsort.argsort()


runner = perf.Runner()
runner.bench_func('Quantile - argsort', bench_argsort)
runner.bench_func('Quantile - func', bench_quantile)
