import json
import os
import sys
from pathlib import Path
import subprocess
import select
import time
import math

def fetch_run_info(prof_data):
    join_count = 0
    swap_bitmask = 0
    bitmask_count = 0

    def dfs_tree(node):
        nonlocal join_count
        nonlocal swap_bitmask
        nonlocal bitmask_count
        if node.get('operator_type', None) == 'HASH_JOIN':
            if node.get('extra_info', {}).get('Join Type', '') != 'MARK':
                left_cost = node['children'][0]['operator_cardinality'] * (node['children'][0]['data_width'] + 8)
                right_cost = node['children'][1]['operator_cardinality'] * (node['children'][1]['data_width'] + 8)
                if left_cost < right_cost:
                    swap_bitmask += 2 ** bitmask_count
                    (node['children'][0], node['children'][1]) = (node['children'][1], node['children'][0])
                bitmask_count += 1
            join_count += 1
        for child in node['children']:
            dfs_tree(child)

    # Example usage: print all nodes
    dfs_tree(prof_data)
    return (join_count, swap_bitmask)

def run_nosteal(result_file, sqls, jc_bitmask, threads, endpoint='./build/nosteal'):
    try:
        with result_file.open('r') as f:
            results = json.load(f)
    except Exception as e:
        results = {}
    for sql_name in sqls:
        if results.get(sql_name, []) != []:
            continue
        (jc, swap_bitmask) = jc_bitmask[sql_name]
        

        proc = subprocess.Popen([endpoint, str(threads), sql_name, str(swap_bitmask)],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=True,
                                bufsize=1)
        def run(bitmask):
            nonlocal proc
            print(sql_name, bitmask)
            proc.stdin.write(f"{bitmask}\n")
            proc.stdin.flush()
            abort_cnt = 0
            start = time.time()
            while abort_cnt < 3:
                rlist, _, error = select.select([proc.stdout], [], [proc.stdout], 0.5)
                restart = False
                if error:
                    restart = True
                    print('exception on stdout')
                # timeout check
                if (time.time() - start) > 30:
                    restart = True
                    print('aborted (timeout)')

                if not restart and rlist:
                    # we have data to read
                    try:
                        line = proc.stdout.readline()
                        return float(line)
                    except Exception:
                        restart = True
                        print('no output')

                if restart:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    abort_cnt += 1
                    proc = subprocess.Popen([endpoint, str(threads), sql_name, str(swap_bitmask)],
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.DEVNULL,
                                            text=True,
                                            bufsize=1)
                    proc.stdin.write(f"{bitmask}\n")
                    proc.stdin.flush()
                    start = time.time()
            return -1

        all_pairs = []
        all_pairs.append([0, run(0)])
        for a in range(1, jc + 1):
            all_pairs.append([2**a, run(2**a)])
            for b in range(a + 1, jc + 1):
                all_pairs.append([2**a + 2**b, run(2**a + 2**b)])

        proc.stdin.close()
        proc.wait()

        results[sql_name] = all_pairs

        with result_file.open('w') as rf:
            json.dump(results, rf, indent=2)
    return results

def run_nonuma(result_file, sqls, jc_bitmask, threads, best_pos, interleave=False, endpoint = '/build/nonuma'):
    try:
        with result_file.open('r') as f:
            results = json.load(f)
    except Exception as e:
        results = {}
    for sql_name in sqls:
        if results.get(sql_name, None) != None:
            continue
        (jc, swap_bitmask) = jc_bitmask[sql_name]

        if interleave:
            args = ['numactl', '-i', 'all', endpoint, str(threads), sql_name, str(swap_bitmask)]
        else:
            args = [endpoint, str(threads), sql_name, str(swap_bitmask)]

        proc = subprocess.Popen(args,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=True,
                                bufsize=1)
        def run(bitmask):
            nonlocal proc
            print(sql_name, bitmask)
            proc.stdin.write(f"{bitmask}\n")
            proc.stdin.flush()
            abort_cnt = 0
            start = time.time()
            while abort_cnt < 3:
                rlist, _, error = select.select([proc.stdout], [], [proc.stdout], 0.5)
                restart = False
                if error:
                    restart = True
                    print('exception on stdout')
                # timeout check
                if (time.time() - start) > 30:
                    restart = True
                    print('aborted (timeout)')

                if not restart and rlist:
                    # we have data to read
                    try:
                        line = proc.stdout.readline()
                        return float(line)
                    except Exception:
                        restart = True
                        print('no output')

                if restart:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    abort_cnt += 1
                    proc = subprocess.Popen(args,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.DEVNULL,
                                            text=True,
                                            bufsize=1)
                    proc.stdin.write(f"{bitmask}\n")
                    proc.stdin.flush()
                    start = time.time()
            return -1

        results[sql_name] = run(best_pos[sql_name])

        proc.stdin.close()
        proc.wait()

        with result_file.open('w') as rf:
            json.dump(results, rf, indent=2)
    return results

def run_baseline(result_file, sqls, jc_bitmask, threads, interleave=False):
    try:
        with result_file.open('r') as f:
            results = json.load(f)
    except Exception as e:
        results = {}
    for sql_name in sqls:
        if results.get(sql_name, None) != None:
            continue
        (jc, swap_bitmask) = jc_bitmask[sql_name]

        if interleave:
            args = ['numactl', '-i', 'all', './build/baseline', str(threads), sql_name, str(swap_bitmask)]
        else:
            args = ['./build/baseline', str(threads), sql_name, str(swap_bitmask)]

        proc = subprocess.Popen(args,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=True,
                                bufsize=1)
        def run(bitmask):
            nonlocal proc
            print(sql_name, bitmask)
            proc.stdin.write(f"{bitmask}\n")
            proc.stdin.flush()
            abort_cnt = 0
            start = time.time()
            while abort_cnt < 3:
                rlist, _, error = select.select([proc.stdout], [], [proc.stdout], 0.5)
                restart = False
                if error:
                    restart = True
                    print('exception on stdout')
                # timeout check
                if (time.time() - start) > 30:
                    restart = True
                    print('aborted (timeout)')

                if not restart and rlist:
                    # we have data to read
                    try:
                        line = proc.stdout.readline()
                        return float(line)
                    except Exception:
                        restart = True
                        print('no output')

                if restart:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    abort_cnt += 1
                    proc = subprocess.Popen(args,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.DEVNULL,
                                            text=True,
                                            bufsize=1)
                    proc.stdin.write(f"{bitmask}\n")
                    proc.stdin.flush()
                    start = time.time()
            return -1

        results[sql_name] = run(0)

        proc.stdin.close()
        proc.wait()

        with result_file.open('w') as rf:
            json.dump(results, rf, indent=2)
    return results

def run_nosteal_4node(result_file, sqls, jc_bitmask, threads):
    try:
        with result_file.open('r') as f:
            results = json.load(f)
    except Exception as e:
        results = {}
    for sql_name in sqls:
        if results.get(sql_name, []) != []:
            continue
        (jc, swap_bitmask) = jc_bitmask[sql_name]
        

        proc = subprocess.Popen(['./build/nosteal-4node', str(threads), sql_name, str(swap_bitmask)],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=True,
                                bufsize=1)
        def run(bitmask):
            nonlocal proc
            print(sql_name, bitmask)
            proc.stdin.write(f"{bitmask}\n")
            proc.stdin.flush()
            abort_cnt = 0
            start = time.time()
            while abort_cnt < 3:
                rlist, _, error = select.select([proc.stdout], [], [proc.stdout], 0.5)
                restart = False
                if error:
                    restart = True
                    print('exception on stdout')
                # timeout check
                if (time.time() - start) > 30:
                    restart = True
                    print('aborted (timeout)')

                if not restart and rlist:
                    # we have data to read
                    try:
                        line = proc.stdout.readline()
                        return float(line)
                    except Exception:
                        restart = True
                        print('no output')

                if restart:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    abort_cnt += 1
                    proc = subprocess.Popen(['./build/nosteal-4node', str(threads), sql_name, str(swap_bitmask)],
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.DEVNULL,
                                            text=True,
                                            bufsize=1)
                    proc.stdin.write(f"{bitmask}\n")
                    proc.stdin.flush()
                    start = time.time()
            return -1

        all_pairs = []
        all_pairs.append([0, run(0)])
        for a in range(1, jc + 1):
            all_pairs.append([2 ** a, run(2 ** a)])
            for b in range(a + 1, jc + 1):
                all_pairs.append([2 ** a + 2 ** b, run(2 ** a + 2 ** b)])
                for c in range(b + 1, jc + 1):
                    all_pairs.append([2 ** a + 2 ** b + 2 ** c, run(2 ** a + 2 ** b + 2 ** c)])

        proc.stdin.close()
        proc.wait()

        results[sql_name] = all_pairs

        with result_file.open('w') as rf:
            json.dump(results, rf, indent=2)
    return results

def run_jc_bitmask(result_file, sqls):
    try:
        with result_file.open('r') as f:
            results = json.load(f)
    except Exception as e:
        results = {}
    for sql_name in sqls:
        if results.get(sql_name, None) != None:
            continue

        file_path = os.path.join("/home/hangrui/numa-join/job-learner/tree/", sql_name)
        with open(file_path, 'r') as f:
            prof_data = json.load(f)
        (jc, swap_bitmask) = fetch_run_info(prof_data)

        while True:
            subprocess.run(['./build/prof', sql_name, str(swap_bitmask)])
            with open("./prof.json", 'r') as f:
                prof_data = json.load(f)
            (new_jc, new_swap_bitmask) = fetch_run_info(prof_data)
            if new_swap_bitmask & swap_bitmask == 0:
                break
            swap_bitmask ^= new_swap_bitmask & swap_bitmask

        results[sql_name] = [jc, swap_bitmask]

        with result_file.open('w') as rf:
            json.dump(results, rf, indent=2)
    return results


def main(argv):
    sql_text_dir = Path("/PATH/TO/job/")
    results_dir = "small_rowgroup/"

    sqls = []

    for p in sql_text_dir.iterdir():
        name = p.name
        if name and name[0].isdigit():
            if name.lower().endswith('.sql'):
                sqls.append(name[:-4])
            else:
                sqls.append(name)

    sqls.sort(key=lambda s: (len(s), s))

    jc_bitmask = run_jc_bitmask(Path(results_dir + "results_jc_bitmask.json"), sqls)

    results_nosteal = run_nosteal(Path(results_dir + "results_swap_nosteal.json"), sqls, jc_bitmask, 96)

    best_pos = {}
    for key, item in results_nosteal.items():
        baseline48 = item[0][1]
        numa, pos = min([(i[1], i[0]) for i in item if i[1] != -1])
        best_pos[key] = pos

    results_nonuma48 = run_nonuma(Path(results_dir + "results_swap_nonuma48.json"), sqls, jc_bitmask, 48, best_pos)
    results_baseline48 = run_baseline(Path(results_dir + "results_swap_baseline48.json"), sqls, jc_bitmask, 48)

    speedups_baseline48 = []
    speedups_nonuma48 = []
    for key in sqls:
        baseline48 = results_baseline48[key]
        nonuma48 = results_nonuma48[key]
        numa, pos = min([(i[1], i[0]) for i in results_nosteal[key] if i[1] != -1])

        print(key, baseline48, numa, baseline48 / numa, nonuma48, nonuma48 / numa)
        speedups_baseline48.append(baseline48 / numa)
        speedups_nonuma48.append(nonuma48 / numa)

    print(math.exp(sum([max(0, math.log(i)) for i in speedups_baseline48]) / len(speedups_baseline48)))
    print(math.exp(sum([max(0, math.log(i)) for i in speedups_nonuma48]) / len(speedups_nonuma48)))

    results_nonuma96 = run_nonuma(Path(results_dir + "results_swap_nonuma96.json"), sqls, jc_bitmask, 96, best_pos)
    results_baseline96 = run_baseline(Path(results_dir + "results_swap_baseline96.json"), sqls, jc_bitmask, 96)

    speedups_baseline96 = []
    speedups_nonuma96 = []
    for key in sqls:
        baseline96 = results_baseline96[key]
        nonuma96 = results_nonuma96[key]
        numa, pos = min([(i[1], i[0]) for i in results_nosteal[key] if i[1] != -1])

        print(key, baseline96, numa, baseline96 / numa, nonuma96, nonuma96 / numa)
        speedups_baseline96.append(baseline96 / numa)
        speedups_nonuma96.append(nonuma96 / numa)

    print(math.exp(sum([max(0, math.log(i)) for i in speedups_baseline96]) / len(speedups_baseline96)))
    print(math.exp(sum([max(0, math.log(i)) for i in speedups_nonuma96]) / len(speedups_nonuma96)))

    results_nonuma24 = run_nonuma(Path(results_dir + "results_swap_nonuma24.json"), sqls, jc_bitmask, 24, best_pos)
    results_baseline24 = run_baseline(Path(results_dir + "results_swap_baseline24.json"), sqls, jc_bitmask, 24)
    results_nosteal48 = run_nosteal(Path(results_dir + "results_swap_nosteal48.json"), sqls, jc_bitmask, 48)

    speedups48_baseline24 = []
    speedups48_nonuma24 = []
    for key in sqls:
        baseline24 = results_baseline24[key]
        nonuma24 = results_nonuma24[key]
        numa, pos = min([(i[1], i[0]) for i in results_nosteal48[key] if i[1] != -1])

        print(key, baseline24, numa, baseline24 / numa, nonuma24, nonuma24 / numa)
        speedups48_baseline24.append(baseline24 / numa)
        speedups48_nonuma24.append(nonuma24 / numa)
        
    print(math.exp(sum([math.log(i) for i in speedups48_baseline24]) / len(speedups48_baseline24)))
    print(math.exp(sum([math.log(i) for i in speedups48_nonuma24]) / len(speedups48_nonuma24)))

    speedups48_baseline48 = []
    speedups48_nonuma48 = []
    for key in sqls:
        baseline48 = results_baseline48[key]
        nonuma48 = results_nonuma48[key]
        numa, pos = min([(i[1], i[0]) for i in results_nosteal48[key] if i[1] != -1])

        print(key, baseline48, numa, baseline48 / numa, nonuma48, nonuma48 / numa)
        speedups48_baseline48.append(baseline48 / numa)
        speedups48_nonuma48.append(nonuma48 / numa)
        
    print(math.exp(sum([math.log(i) for i in speedups48_baseline48]) / len(speedups48_baseline48)))
    print(math.exp(sum([math.log(i) for i in speedups48_nonuma48]) / len(speedups48_nonuma48)))

    results_steal = run_nosteal(Path(results_dir + "results_swap_steal.json"), sqls, jc_bitmask, 96, endpoint='./build/steal')

    results_nosteal = [min([i[1] for i in result if i[1] != -1]) for _, result in results_nosteal.items()]
    results_nosteal48 = [min([i[1] for i in result if i[1] != -1]) for _, result in results_nosteal48.items()]

    results_nosteal_twobreaker = [min([i[1] for i in result if i[1] != -1]) for _, result in results_nosteal_twobreaker.items()]

    results_steal = [min([i[1] for i in result if i[1] != -1]) for _, result in results_steal.items()]

    geo_nosteal48 = math.exp(sum([math.log(i) for i in results_nosteal48]) / len(results_nosteal48))
    geo_nosteal96 = math.exp(sum([math.log(i) for i in results_nosteal]) / len(results_nosteal))
    geo_baseline24 = math.exp(sum([math.log(i) for _, i in results_baseline24.items()]) / len(results_baseline24))
    geo_baseline48 = math.exp(sum([math.log(i) for _, i in results_baseline48.items()]) / len(results_baseline48))
    geo_baseline96 = math.exp(sum([math.log(i) for _, i in results_baseline96.items()]) / len(results_baseline96))
    geo_nonuma24 = math.exp(sum([math.log(i) for _, i in results_nonuma24.items()]) / len(results_nonuma24))
    geo_nonuma48 = math.exp(sum([math.log(i) for _, i in results_nonuma48.items()]) / len(results_nonuma48))
    geo_nonuma96 = math.exp(sum([math.log(i) for _, i in results_nonuma96.items()]) / len(results_nonuma96))
    geo_steal = math.exp(sum([math.log(i) for i in results_steal]) / len(results_steal))

    print("\n=== Geo Mean ===")
    print(f"{'Configuration':<30} {'Time':<15}")
    print("-" * 60)
    print(f"{'baseline24':<30} {geo_baseline24:<15.4f}")
    print(f"{'baseline48':<30} {geo_baseline48:<15.4f}")
    print(f"{'baseline96':<30} {geo_baseline96:<15.4f}")
    print(f"{'nonuma24':<30} {geo_nonuma24:<15.4f}")
    print(f"{'nonuma48':<30} {geo_nonuma48:<15.4f}")
    print(f"{'nonuma96':<30} {geo_nonuma96:<15.4f}")
    print(f"{'nosteal48':<30} {geo_nosteal48:<15.4f}")
    print(f"{'nosteal96':<30} {geo_nosteal96:<15.4f}")
    print(f"{'steal96':<30} {geo_steal:<15.4f}")

if __name__ == '__main__':
    sys.exit(main(sys.argv))