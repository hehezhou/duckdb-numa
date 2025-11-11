import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import random
import string
import math
import os
# import duckdb
# import struct

# probe_size = 20000000
#parse the input arguments
build_size = int(sys.argv[1])
build_size_2 = int(sys.argv[2])
probe_size = int(sys.argv[3])
sel1 = float(sys.argv[4])
sel2 = float(sys.argv[5])
payload_size = int(sys.argv[6]) # only probe side payload
same_join_key = True

payload_file = ''
if len(sys.argv)>7:
    payload_file = sys.argv[7] # real-world data file name, if not None, read the payload set from the file
key_set_file = ''
if len(sys.argv)>8:
    key_set_file = sys.argv[8] # real-world data file name, if not None, read the key set from the file

    
#! build_{size}_{hit_ratio}_{key_pattern}_{payload_size}.csv
#! probe_{size}_{selectivity}_{key_pattern}_{probe_distribution}.csv

#! unique_key_set & non_hit_key_set: {payload_size}, two sets both have more than max {build_size} keys
file_path = '/home/hangrui/numa-join/no-steal/examples/tpch-numa/script/numa_data/'
build1_name = f'build_{build_size}'
build2_name = f'build2_{build_size_2}'
probe_name = f'probe_{build_size}_{build_size_2}_{probe_size}_{int(sel1*10000)}_{int(sel2*10000)}_{payload_size}'


build_file_name1 = build1_name+ '.parquet'
build_file_name2 = build2_name+ '.parquet'
probe_file_name = probe_name+'.parquet'

generate_build_side1 = True
generate_build_side2 = True
generate_probe_side = True
if os.path.exists(file_path+build_file_name1):
    generate_build_side1 = False
if os.path.exists(file_path+build_file_name2):
    generate_build_side2 = False
if os.path.exists(file_path+probe_file_name):
    generate_probe_side = False
both_hit_key_set = []
with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/keys_both_hit.csv', 'r') as f:
    both_hit_key_set = [int(line[:-1]) for line in f.readlines()]
with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/keys_B_hit.csv', 'r') as f:
    B_key_set = [int(line[:-1]) for line in f.readlines()]
with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/keys_A_hit.csv', 'r') as f:
    A_key_set = [int(line[:-1]) for line in f.readlines()]
with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/keys_non_hit.csv', 'r') as f:
    non_hit_key_set = [int(line[:-1]) for line in f.readlines()]

def generate_build_side1_key(size, both_hit_key_set):
    join_output_size = int(sel1 * probe_size)
    replicate_key_number = 0
    sample_hit_keys = join_output_size
    if join_output_size > size:
        replicate_key_number = (join_output_size - size) / 400
        # replicate the replicate_key set 20 times
        sample_hit_keys = sample_hit_keys - replicate_key_number * 20
    replicate_key = np.random.choice(both_hit_key_set, int(replicate_key_number), replace=False)
    replicate_key_list = np.concatenate([replicate_key for i in range(20)])
    both_hit_key_set = np.setdiff1d(both_hit_key_set, replicate_key)  # Update the local copy
    both_hit_sample = np.random.choice(both_hit_key_set, int(size - len(replicate_key_list)), replace=False)
    unique_hit_key_set = both_hit_sample
    both_hit_sample = np.concatenate([both_hit_sample, replicate_key_list])
    return both_hit_sample, replicate_key, unique_hit_key_set
      

def generate_build_side2_key(size, unique_hit_key, replicate_key):
    join2_output_size = int(sel2 * probe_size)
    build_side_key = []
    if join2_output_size > size:
        # need to replicate some hit keys on both sides
        replicate_key_number = (join2_output_size- size)/2000 
        replicate_key_subset = np.random.choice(replicate_key, int(replicate_key_number), replace=False)
        
        build_side_key = np.concatenate([replicate_key_subset for i in range(5)])
        both_hit_sample_size = size - replicate_key_number* 5
        
        both_hit_sample = np.random.choice(list(unique_hit_key), int(both_hit_sample_size), replace=True)
        build_side_key = np.concatenate([both_hit_sample, build_side_key])
        return build_side_key
    
        
    num_B_hit = size - join2_output_size

    both_hit_sample = np.random.choice(list(unique_hit_key), join2_output_size, replace=True)
    build_side_key = both_hit_sample.tolist() + np.random.choice(B_key_set, num_B_hit, replace=True).tolist()
    
    return build_side_key


def generate_probe_key(size, buildsize1,buildsize2, replicate_key_1, unique_hit_join1):
    join1_output_size = int(sel1 * probe_size)
    
    replicate_key_join1 = []
    if replicate_key_1 is not None:
        replicate_key_join1 = np.concatenate([replicate_key_1 for i in range(20)])
        join1_output_size = join1_output_size - len(replicate_key_1)*400
    join1_hit_unique_key = np.random.choice(unique_hit_join1, join1_output_size, replace=True)
    non_hit_key_num = size - join1_output_size - len(replicate_key_join1)
    non_hit_sample = []
    if non_hit_key_num >0:
        non_hit_sample = np.random.choice(non_hit_key_set, non_hit_key_num, replace=True)
    probe_side_key = np.concatenate([join1_hit_unique_key, replicate_key_join1, non_hit_sample])
    np.random.shuffle(probe_side_key)
    return probe_side_key, set(join1_hit_unique_key), replicate_key_join1
    
    
    
    # num_both_hit = int(sel1 * size)
    # # num_A_hit = int(sel1 * size) - num_both_hit
    # num_non_hit = size - num_both_hit
    
    # both_hit_sample = np.random.choice(both_hit_key_set[: buildsize1], num_both_hit, replace=False)
    # replicate_key = []
    # if int(sel2 * size) > buildsize2:
    #     both_hit_sample_size = int(sel2 * size)
    #     replicate_key_number = (both_hit_sample_size- buildsize2)/400 
    #     replicate_key = np.random.choice(list(set(both_hit_sample)), int(replicate_key_number), replace=False)
    #     both_hit_sample_new = np.concatenate([replicate_key for i in range(20)])
    #     both_hit_sample_new = np.concatenate([both_hit_sample_new, np.random.choice((both_hit_sample), int(num_both_hit- replicate_key_number*20), replace=True)])
    #     both_hit_sample = both_hit_sample_new

   
    # # A_hit_sample = np.random.choice(A_key_set[:int(sel1 * buildsize1)-int(sel_both_hit * buildsize1)], num_A_hit, replace=True)
    # # B_hit_sample = np.random.choice(B_key_set[:buildsize2], num_B_hit, replace=True)
    # non_hit_sample = np.random.choice(non_hit_key_set, num_non_hit, replace=True)
    # probe_side_key = np.concatenate([both_hit_sample, non_hit_sample])

    # np.random.shuffle(probe_side_key)

    # return probe_side_key, set(both_hit_sample), replicate_key
            
            
def gen_random_string(size, length, fixed_length=True):
    if fixed_length:
        return [''.join(random.choices(string.ascii_uppercase + string.digits, k=length)) for i in range(size)]
    else:
        return [''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(length//2, length))) for i in range(size)]   
    


def generate_payload_data(build_size, payload_size, fixed_length):
    payload_data = []

    match payload_size:
        case 4:
            payload_data = np.random.randint(0, 2**31, build_size)
        case 8:
            payload_data = np.random.randint(0, 2**63, build_size)
        case _:
            payload_data = gen_random_string(build_size, payload_size,fixed_length)
                
    # else:
    #     match payload_size:
    #         case 4:
    #             with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/dbint/'+payload_file+'.txt', 'r') as f:
    #                 payload_total = [int(line[:-1]) for line in f.readlines()]
    #                 payload_data = [payload_total[i%len(payload_total)] for i in range(build_size)]
    #         case 8:
    #             with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/dbint/'+payload_file, 'rb') as f:
    #             # currently int64_t file is in binary format
    #                 payload_total = np.fromfile(f, dtype=np.uint64)[1:]
    #                 # payload_total = [int64_t(line[:-1]) for line in f.readlines()]
    #                 payload_data = [payload_total[i%len(payload_total)] for i in range(build_size)]
    #         case _:
    #             with open('/home/yihao/duckdb/ht_tmp/duckdb/examples/embedded-c++/microbench/unique_hit_key_set/dbtext/'+payload_file, 'r') as f:
    #                 payload_total = [(line[:-1]) for line in f.readlines()]
    #                 payload_data = [payload_total[i%len(payload_total)] for i in range(build_size)]
    return payload_data
    
if generate_probe_side:
    print(f'Generating {build1_name} data')
    build_side_key, replicate_key_1, unique_hit_join1 = generate_build_side1_key(build_size, both_hit_key_set)
    build_column_types = [pa.int32()]
    build_table = [build_side_key]
    build_arrays = [pa.array(column, type=col_type) for column, col_type in zip(build_table, build_column_types)]
    build_column_names = ['build_key']
    build_pa_table = pa.Table.from_arrays(build_arrays, names=build_column_names)
    pq.write_table(build_pa_table, file_path+build_file_name1,row_group_size=122880)

    print(f'Generating {probe_name} data')
    probe_side_key, unique_hit_key, replicate_key = generate_probe_key(probe_size, build_size, build_size_2, replicate_key_1, unique_hit_join1)

    fixed_length = True
    if payload_size % 10!=0:
        fixed_length = False
    payload_data = generate_payload_data(probe_size, payload_size,fixed_length)

    probe_table = [probe_side_key, payload_data]
    
    probe_column_types = [pa.int32()]
    match payload_size:
        case 4:
            probe_column_types.append(pa.int32())
        case 8:
            probe_column_types.append(pa.int64())
        case _:
            probe_column_types.append(pa.string())
    probe_arrays = [pa.array(column, type=col_type) for column, col_type in zip(probe_table, probe_column_types)]
    probe_column_names = ['probe_key','payload_0']
    probe_pa_table = pa.Table.from_arrays(probe_arrays, names=probe_column_names)
    pq.write_table(probe_pa_table, file_path+probe_file_name,row_group_size=122880)
    

    print(f'Generating {build2_name} data')
    build_side_key = generate_build_side2_key(build_size_2,unique_hit_key, replicate_key)
    build_column_types = [pa.int32()]
    build_table = [build_side_key]
    build_arrays = [pa.array(column, type=col_type) for column, col_type in zip(build_table, build_column_types)]
    build_column_names = ['build_key']
    build_pa_table = pa.Table.from_arrays(build_arrays, names=build_column_names)
    pq.write_table(build_pa_table, file_path+build_file_name2,row_group_size=122880)

os.system(f'python3 load_db_numa.py 0 {build_size} {build_size_2} {probe_size} {sel1} {sel2} {payload_size} {generate_build_side1} {generate_build_side2} {generate_probe_side} {payload_file} {key_set_file}')
os.system(f'python3 load_db_numa.py 1 {build_size} {build_size_2} {probe_size} {sel1} {sel2} {payload_size} {generate_build_side1} {generate_build_side2} {generate_probe_side} {payload_file} {key_set_file}')