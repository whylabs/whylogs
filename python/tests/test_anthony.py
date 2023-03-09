import cProfile
import pstats
import time
from io import StringIO

#{"datasetId": "model-31", "timestamp": null, "single": null, "multiple": {"columns": ["embeddings"],

data = [
        [0.418, 0.24968, -0.41242, 0.1217, 0.34527, -0.044457, -0.49688, -0.17862, -0.00066023, -0.6566, 0.27843, -0.14767, -0.55677, 0.14658, -0.0095095, 0.011658, 0.10204, -0.12792, -0.8443, -0.12181, -0.016801, -0.33279, -0.1552, -0.23131, -0.19181, -1.8823, -0.76746, 0.099051, -0.42125, -0.19526, 4.0071, -0.18594, -0.52287, -0.31681, 0.00059213, 0.0074449, 0.17778, -0.15897, 0.012041, -0.054223, -0.29871, -0.15749, -0.34758, -0.045637, -0.44251, 0.18785, 0.0027849, -0.18411, -0.11514, -0.78581],
        [0.013441, 0.23682, -0.16899, 0.40951, 0.63812, 0.47709, -0.42852, -0.55641, -0.364, -0.23938, 0.13001, -0.063734, -0.39575, -0.48162, 0.23291, 0.090201, -0.13324, 0.078639, -0.41634, -0.15428, 0.10068, 0.48891, 0.31226, -0.1252, -0.037512, -1.5179, 0.12612, -0.02442, -0.042961, -0.28351, 3.5416, -0.11956, -0.014533, -0.1499, 0.21864, -0.33412, -0.13872, 0.31806, 0.70358, 0.44858, -0.080262, 0.63003, 0.32111, -0.46765, 0.22786, 0.36034, -0.37818, -0.56657, 0.044691, 0.30392],
        [0.15164, 0.30177, -0.16763, 0.17684, 0.31719, 0.33973, -0.43478, -0.31086, -0.44999, -0.29486, 0.16608, 0.11963, -0.41328, -0.42353, 0.59868, 0.28825, -0.11547, -0.041848, -0.67989, -0.25063, 0.18472, 0.086876, 0.46582, 0.015035, 0.043474, -1.4671, -0.30384, -0.023441, 0.30589, -0.21785, 3.746, 0.0042284, -0.18436, -0.46209, 0.098329, -0.11907, 0.23919, 0.1161, 0.41705, 0.056763, -6.3681e-05, 0.068987, 0.087939, -0.10285, -0.13931, 0.22314, -0.080803, -0.35652, 0.016413, 0.10216],
        [0.70853, 0.57088, -0.4716, 0.18048, 0.54449, 0.72603, 0.18157, -0.52393, 0.10381, -0.17566, 0.078852, -0.36216, -0.11829, -0.83336, 0.11917, -0.16605, 0.061555, -0.012719, -0.56623, 0.013616, 0.22851, -0.14396, -0.067549, -0.38157, -0.23698, -1.7037, -0.86692, -0.26704, -0.2589, 0.1767, 3.8676, -0.1613, -0.13273, -0.68881, 0.18444, 0.0052464, -0.33874, -0.078956, 0.24185, 0.36576, -0.34727, 0.28483, 0.075693, -0.062178, -0.38988, 0.22902, -0.21617, -0.22562, -0.093918, -0.80375],
        [0.68047, -0.039263, 0.30186, -0.17792, 0.42962, 0.032246, -0.41376, 0.13228, -0.29847, -0.085253, 0.17118, 0.22419, -0.10046, -0.43653, 0.33418, 0.67846, 0.057204, -0.34448, -0.42785, -0.43275, 0.55963, 0.10032, 0.18677, -0.26854, 0.037334, -2.0932, 0.22171, -0.39868, 0.20912, -0.55725, 3.8826, 0.47466, -0.95658, -0.37788, 0.20869, -0.32752, 0.12751, 0.088359, 0.16351, -0.21634, -0.094375, 0.018324, 0.21048, -0.03088, -0.19722, 0.082279, -0.09434, -0.073297, -0.064699, -0.26044],
        [0.26818, 0.14346, -0.27877, 0.016257, 0.11384, 0.69923, -0.51332, -0.47368, -0.33075, -0.13834, 0.2702, 0.30938, -0.45012, -0.4127, -0.09932, 0.038085, 0.029749, 0.10076, -0.25058, -0.51818, 0.34558, 0.44922, 0.48791, -0.080866, -0.10121, -1.3777, -0.10866, -0.23201, 0.012839, -0.46508, 3.8463, 0.31362, 0.13643, -0.52244, 0.3302, 0.33707, -0.35601, 0.32431, 0.12041, 0.3512, -0.069043, 0.36885, 0.25168, -0.24517, 0.25381, 0.1367, -0.31178, -0.6321, -0.25028, -0.38097],
        [0.33042, 0.24995, -0.60874, 0.10923, 0.036372, 0.151, -0.55083, -0.074239, -0.092307, -0.32821, 0.09598, -0.82269, -0.36717, -0.67009, 0.42909, 0.016496, -0.23573, 0.12864, -1.0953, 0.43334, 0.57067, -0.1036, 0.20422, 0.078308, -0.42795, -1.7984, -0.27865, 0.11954, -0.12689, 0.031744, 3.8631, -0.17786, -0.082434, -0.62698, 0.26497, -0.057185, -0.073521, 0.46103, 0.30862, 0.12498, -0.48609, -0.0080272, 0.031184, -0.36576, -0.42699, 0.42164, -0.11666, -0.50703, -0.027273, -0.53285],
        [0.21705, 0.46515, -0.46757, 0.10082, 1.0135, 0.74845, -0.53104, -0.26256, 0.16812, 0.13182, -0.24909, -0.44185, -0.21739, 0.51004, 0.13448, -0.43141, -0.03123, 0.20674, -0.78138, -0.20148, -0.097401, 0.16088, -0.61836, -0.18504, -0.12461, -2.2526, -0.22321, 0.5043, 0.32257, 0.15313, 3.9636, -0.71365, -0.67012, 0.28388, 0.21738, 0.14433, 0.25926, 0.23434, 0.4274, -0.44451, 0.13813, 0.36973, -0.64289, 0.024142, -0.039315, -0.26037, 0.12017, -0.043782, 0.41013, 0.1796],
        [0.25769, 0.45629, -0.76974, -0.37679, 0.59272, -0.063527, 0.20545, -0.57385, -0.29009, -0.13662, 0.32728, 1.4719, -0.73681, -0.12036, 0.71354, -0.46098, 0.65248, 0.48887, -0.51558, 0.039951, -0.34307, -0.014087, 0.86488, 0.3546, 0.7999, -1.4995, -1.8153, 0.41128, 0.23921, -0.43139, 3.6623, -0.79834, -0.54538, 0.16943, -0.82017, -0.3461, 0.69495, -1.2256, -0.17992, -0.057474, 0.030498, -0.39543, -0.38515, -1.0002, 0.087599, -0.31009, -0.34677, -0.31438, 0.75004, 0.97065],
        [0.23727, 0.40478, -0.20547, 0.58805, 0.65533, 0.32867, -0.81964, -0.23236, 0.27428, 0.24265, 0.054992, 0.16296, -1.2555, -0.086437, 0.44536, 0.096561, -0.16519, 0.058378, -0.38598, 0.086977, 0.0033869, 0.55095, -0.77697, -0.62096, 0.092948, -2.5685, -0.67739, 0.10151, -0.48643, -0.057805, 3.1859, -0.017554, -0.16138, 0.055486, -0.25885, -0.33938, -0.19928, 0.26049, 0.10478, -0.55934, -0.12342, 0.65961, -0.51802, -0.82995, -0.082739, 0.28155, -0.423, -0.27378, -0.007901, -0.030231]
]

import numpy as np
import pandas as pd

from whylogs.core.preprocessing import PreprocessedColumn

def do_it_pandas0():
    series = pd.Series(data * 2000)
    t0 = time.perf_counter()
    col = PreprocessedColumn.apply(series)
    t1 = time.perf_counter()
    print(len(col.pandas.tensors), col.pandas.tensors[0].shape)
    print(f"pandas0 20K rows in {t1-t0:0.4f} sec")


def do_it_pandas0_5():
    series = pd.Series(data)
    t0 = time.perf_counter()
    for i in range(2000):
        col = PreprocessedColumn.apply(series)
    t1 = time.perf_counter()
    print(len(col.pandas.tensors), col.pandas.tensors[0].shape)
    print(f"pandas 0.5 20K rows in {t1-t0:0.4f} sec")


def do_it_pandas1():
    t0 = time.perf_counter()
    profiler = cProfile.Profile()
    series = pd.Series(data)
    string_output_stream = StringIO()
    profiler.enable()
    for _ in range(2000):
        col = PreprocessedColumn.apply(series)
    profiler.disable()
    stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("tottime")
    stats.print_stats(40)
    # save the pstats so you can use flamegraph to turn the profile into an svg file
    # flameprof pandas1.dmp > pandas1.svg
    stats.dump_stats('pandas1.prof') 

    t1 = time.perf_counter()
    print(len(col.pandas.tensors), col.pandas.tensors[0].shape)
    print(f"pandas1 20K rows in {t1-t0:0.4f} sec")
    print( f"\n{string_output_stream.getvalue()}")

 
def do_it_pandas2():
    t0 = time.perf_counter()
    for i in range(2000):
        nps = pd.Series([np.asarray(data)])
        col = PreprocessedColumn.apply(nps)
    t1 = time.perf_counter()
    print(len(col.pandas.tensors if col.pandas.tensors is not None else []), col.pandas.tensors[0].shape)
    print(f"pandas2 20K rows in {t1-t0:0.4f} sec")


def do_it_pandas3():
    t0 = time.perf_counter()
    for i in range(2000):
        series = pd.Series(data)
        for x in series:
            col = PreprocessedColumn._process_scalar_value(x)
    t1 = time.perf_counter()
    print(len(col.list.tensors), col.list.tensors[0].shape)
    print(f"pandas3 20K rows in {t1-t0:0.4f} sec")


def do_it_numpy1():
    t0 = time.perf_counter()
    A = np.asarray(data)
    for i in range(2000-1):
        A = np.vstack((A, np.asarray(data)))
    col = PreprocessedColumn._process_scalar_value(A)
    t1 = time.perf_counter()
    print(len(col.list.tensors))
    print(f"single {col.list.tensors[0].shape} matrix in {t1-t0:0.4f} sec")


def do_it_numpy2():
    t0 = time.perf_counter()
    for i in range(2000):
        col = PreprocessedColumn._process_scalar_value(np.asarray(data))
    t1 = time.perf_counter()
    print(len(col.list.tensors))
    print(f"2K x {col.list.tensors[0].shape} matrix in {t1-t0:0.4f} sec")


def do_it_scalar1():
    X = data * 2000
    t0 = time.perf_counter()
    for x in X:
        col = PreprocessedColumn._process_scalar_value(x)
    t1 = time.perf_counter()
    print(f"20K x {col.list.tensors[0].shape} matrix in {t1-t0:0.4f} sec")


def do_it_scalar2():
    X = data * 2000
    t0 = time.perf_counter()
    for x in X:
        col = PreprocessedColumn._process_scalar_value(np.asarray(x))
    t1 = time.perf_counter()
    print(f"* 20K x {col.list.tensors[0].shape} matrix in {t1-t0:0.4f} sec")

"""
print()
for i in range(10):
    do_it_pandas0()


print()
for i in range(10):
    do_it_pandas0_5()

"""
print()
# for i in range(10):
for i in range(1):
    do_it_pandas1()
"""
print()
for i in range(10):
    do_it_numpy1()



print()
for i in range(10):
    do_it_numpy2()



print()
for i in range(10):
    do_it_scalar1()

    
print()
for i in range(10):
    do_it_scalar2()
"""