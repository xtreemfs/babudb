import os

from executils import exec_ranges
from datafile import DataFile

if __name__ == '__main__':
    base_path = './experiments/diskindex/'

    if not os.path.exists(base_path):
        os.mkdir(base_path)

    config = {
        'blocksize': 16,
        'hitrate': 10,
        'keylength': 8,
        'path': '/tmp/babudb_diskindex_benchmark',
        'num_entries': 100000,
        'num_lookups': 0,
        'base_dir': base_path,
        'seed': 0
        }

    cmd = """java -Xms512M -Xmx1024M -jar babudb.jar -blocksize {blocksize} -hitrate {hitrate} -keylength {keylength} {path} {num_entries} {num_lookups}"""

#    exec_single(cmd, config)
#    exec_repetitions(cmd, config, num_reps=3)

    # centralized
#    centralized_path = os.path.join(base_path, 'centralized')
#    if not os.path.exists(centralized_path):
#        os.mkdir(centralized_path)
#    config['base_dir'] = centralized_path

#    ranges = {'num_entries': [1000,10000,100000, 1000000], 'blocksize': [32,64], 'keylength': [8, 12]}
    ranges = {'num_entries': [1000,10000,100000]} # ,1000000

#    ranges = {'blocksize': [16]} # ,512,1024,2048,4096
    attrs = list(ranges.keys())

    df = DataFile(os.path.join(base_path, 'stats_%s.dat' % '_'.join(attrs)), ['entries', 'lookups', 'hits', 'total time', 'lookup time', 'iter time', 'iter throughput'] + attrs, overwrite=True)

    results = exec_ranges(cmd, config, ranges, num_reps=10, config=base_path)

    for config, reps in results:
        cfg = dict(config)
        # TODO: avg. over the reps
        avg_iter_time = []
        avg_iter_tp = []

        for data in reps:
            entries, lookups, hits, total_time, lookup_time, iter_time, iter_tp = data.split(", ")
            avg_iter_time.append(int(iter_time))
            avg_iter_tp.append(int(iter_tp))

        for attr in ranges:
            df[attr] = cfg[attr]
        
        df['entries'] = entries
        df['lookups'] = lookups
        df['hits'] = hits
        df['total time'] = total_time
        df['lookup time'] = lookup_time
        df['iter time'] = float(sum(avg_iter_time))/len(avg_iter_time)
        df['iter throughput'] = float(sum(avg_iter_tp))/len(avg_iter_tp)
        df.save()

        print(avg_iter_time)
        
    df.close()
        



