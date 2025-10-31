import duckdb
import sys

db = int(sys.argv[1])
build_size = int(sys.argv[2])
build_size_2 = int(sys.argv[3])
probe_size = int(sys.argv[4])
sel1 = float(sys.argv[5])
sel2 = float(sys.argv[6])
payload_size = int(sys.argv[7])
build_side1_load = bool(sys.argv[8]) 
build_side2_load = bool(sys.argv[9])
probe_side_load = bool(sys.argv[10])
payload_file = ''
if len(sys.argv)>11:
    payload_file = sys.argv[11] # real-world data file name, if not None, read the payload set from the file
key_set_file = ''
if len(sys.argv)>12:
    key_set_file = sys.argv[12] # real-world data file name, if not None, read the key set from the file

file_path = '/home/hangrui/numa-join/no-steal/examples/tpch-numa/script/numa_data/'
build1_name = f'build_{build_size}'
build2_name = f'build2_{build_size_2}'
probe_name = f'probe_{build_size}_{build_size_2}_{probe_size}_{int(sel1*10000)}_{int(sel2*10000)}_{payload_size}'


data_build1_path = file_path + build1_name + '.parquet'
data_build2_path = file_path + build2_name + '.parquet'
data_probe_path = file_path + probe_name + '.parquet'


if db == 0: #compressed
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_numa.db")
    if build_side1_load:
        con.execute("PRAGMA force_compression='auto';")
        con.execute(f"Drop TABLE IF EXISTS {build1_name}")
        con.execute(f"CREATE TABLE {build1_name} AS SELECT * FROM '{data_build1_path}'")
    if build_side2_load:
        con.execute("PRAGMA force_compression='auto';")
        con.execute(f"Drop TABLE IF EXISTS {build2_name}")
        con.execute(f"CREATE TABLE {build2_name} AS SELECT * FROM '{data_build2_path}'")
        
    if probe_side_load:
        con.execute("PRAGMA force_compression='auto';")
        con.execute(f"Drop TABLE IF EXISTS {probe_name}")
        con.execute(f"CREATE TABLE {probe_name} AS SELECT * FROM '{data_probe_path}'")
elif db == 1:
    con = duckdb.connect(database = "/home/yihao/duckdb/origin/duckdb/examples/embedded-c++/release/micro_numa_uncom.db")
    if build_side1_load:
        con.execute("PRAGMA force_compression='uncompressed';")
        con.execute(f"Drop TABLE IF EXISTS {build1_name}")
        con.execute(f"CREATE TABLE {build1_name} AS SELECT * FROM '{data_build1_path}'")
    if build_side2_load:
        con.execute("PRAGMA force_compression='uncompressed';")
        con.execute(f"Drop TABLE IF EXISTS {build2_name}")
        con.execute(f"CREATE TABLE {build2_name} AS SELECT * FROM '{data_build2_path}'")
    if probe_side_load:
        con.execute("PRAGMA force_compression='uncompressed';")
        con.execute(f"Drop TABLE IF EXISTS {probe_name}")
        con.execute(f"CREATE TABLE {probe_name} AS SELECT * FROM '{data_probe_path}'")
        

