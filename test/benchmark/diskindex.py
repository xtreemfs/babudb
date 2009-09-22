import os

from executils import exec_ranges
from datafile import DataFile

def sample_variance(data):
    avg = float(sum(data))/float(len(data))
    variance = sum([(x-avg)**2 for x in data])/float(len(data))
    return variance

def attr_names(name):
    return ['%s %s' % (name, attr) for attr in ["avg", "max", "min", "stddev"]]

class Entry(object):
    def __init__(self, name, vals=[]):
        self.vals = vals
        self.name = name

        self.max = -1
        self.min = -1
        self.avg = -1
        self.stddev = -1

        self._add_entry()

    def _add_entry(self):
        if self.vals == []:
            return

        self.max = max(self.vals)
        self.min = min(self.vals)
        self.avg = float(sum(self.vals))/float(len(self.vals))
        self.stddev = sample_variance(self.vals)
#        self.last_seen = vals[-1].avg        

    def append(self, item):
        self.vals.append(item)
        self._add_entry()

def write_stats(ranges, results, db_path, prefix="stats"):
    attrs = list(ranges.keys())

    df = DataFile(os.path.join(base_path, '%s_%s.dat' % (prefix, '_'.join(attrs))), ['entries', 'lookups', 'hits', 'total time', 'lookup time'] + attr_names("iter time") + attr_names("iter thruput") + attr_names("file size") + attrs, overwrite=True)

    for config, reps in results:
        cfg = dict(config)
        # TODO: avg. over the reps
        iter_times = Entry("iter time")
        iter_tps = Entry("iter thruput")
        file_sizes = Entry("file size")

        for data in reps:
            entries, lookups, hits, total_time, lookup_time, iter_time, iter_tp = data.split(", ")
            iter_times.append(int(iter_time))
            iter_tps.append(int(iter_tp))
            file_sizes.append(int(os.path.getsize(db_path)))

        for attr in ranges:
            df[attr] = cfg[attr]

        df['entries'] = entries
        df['lookups'] = lookups
        df['hits'] = hits
        df['total time'] = total_time
        df['lookup time'] = lookup_time
#        df['file size'] = os.path.getsize(db_path)

        for attr in attr_names("iter time"):
            df[attr] = getattr(iter_times, attr.split()[-1])

        for attr in attr_names("iter thruput"):
            df[attr] = getattr(iter_tps, attr.split()[-1])

        for attr in attr_names("file size"):
            df[attr] = getattr(file_sizes, attr.split()[-1])

        df.save()

    df.close()

if __name__ == '__main__':
    base_path = './experiments/diskindex/'

    if not os.path.exists(base_path):
        os.mkdir(base_path)

    config = {
        'blocksize': 64,
        'hitrate': 10,
        'keylength': 16,
        'path': '/tmp/babudb_diskindex_benchmark',
        'num_entries': 100000,
        'num_lookups': 100000,
        'base_dir': base_path,
        'compression': '',
#        'input': 'random',
        'input': '/home/mikael/lab/babudb-java/test/benchmark/all_files.dat',
        'seed': 0
        }

    cmd = """java -Xms512M -Xmx1024M -jar babudb.jar -blocksize {blocksize} -hitrate {hitrate} -keylength {keylength} -input {input} {compression} {path} {num_entries} {num_lookups}"""

#    exec_single(cmd, config)
#    exec_repetitions(cmd, config, num_reps=3)

    # centralized
#    centralized_path = os.path.join(base_path, 'centralized')
#    if not os.path.exists(centralized_path):
#        os.mkdir(centralized_path)
#    config['base_dir'] = centralized_path

#    ranges = {'num_entries': [1000,10000,100000, 1000000], 'blocksize': [32,64], 'keylength': [8, 12]}
#    ranges = {'num_entries': [1000,10000,100000,1000000,10000000]} # ,1000000
    ranges = {'num_entries': [1000,10000]} #, 100000,1000000,10000000]} # ,1000000

#    ranges = {'blocksize': [16]} # ,512,1024,2048,4096
    results = exec_ranges(cmd, config, ranges, num_reps=3, config=base_path)
    write_stats(ranges, results, config['path'])

    config['compression'] = '-compression'
    results = exec_ranges(cmd, config, ranges, num_reps=3, config=base_path)
    write_stats(ranges, results, config['path'], prefix="stats_compression")
        



