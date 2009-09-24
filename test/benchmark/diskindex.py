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
    def __init__(self, name):
        self.__vals = []
        self.__name = name
#        print "name: ", self.__name, self.__vals

        self.max = -1
        self.min = -1
        self.avg = -1
        self.stddev = -1

        self._add_entry()

    def _add_entry(self):
        if self.__vals == []:
            return

        self.max = max(self.__vals)
        self.min = min(self.__vals)
        self.avg = float(sum(self.__vals))/float(len(self.__vals))
        self.stddev = sample_variance(self.__vals)
#        self.last_seen = vals[-1].avg        

    def add(self, item):
#        print self.__name, "appending item", item
        self.__vals.append(item)
        self._add_entry()

    def __repr__(self):
        return "%s: [%s] - %f, %f, %f" % (self.__name, str(self.__vals), self.min, self.max, self.avg)

def write_stats(ranges, results, prefix="stats"):
    attrs = list(ranges.keys())

    df = DataFile(os.path.join(base_path, '%s_%s.dat' % (prefix, '_'.join(attrs))), ['entries', 'lookups', 'hits', 'total time', 'lookup time', 'scans'] + attr_names("iter time") + attr_names("iter thruput") + attr_names("file size") + attr_names("scan thruput") + attrs, overwrite=True)

    for config, reps in results:
        cfg = dict(config)
        # TODO: avg. over the reps
        iter_times = Entry("iter time")
        iter_tps = Entry("iter thruput")
        file_sizes = Entry("file size")
        scan_tps = Entry("scan thruput")

        for data in reps:
            db_path, entries, lookups, hits, total_time, num_scans, scans_time, lookup_time, iter_time, iter_tp = data.split(", ")
            iter_times.add(int(iter_time))
            iter_tps.add(int(iter_tp))
            file_sizes.add(int(os.path.getsize(db_path)))
            scan_tps.add(float(scans_time)/float(num_scans))

        for attr in ranges:
            df[attr] = cfg[attr]

        df['entries'] = entries
        df['lookups'] = lookups
        df['hits'] = hits
        df['total time'] = total_time
        df['lookup time'] = lookup_time
        df['scans'] = num_scans
#        df['file size'] = os.path.getsize(db_path)

        for attr in attr_names("iter time"):
            df[attr] = getattr(iter_times, attr.split()[-1])

        for attr in attr_names("iter thruput"):
            df[attr] = getattr(iter_tps, attr.split()[-1])

        for attr in attr_names("file size"):
            df[attr] = getattr(file_sizes, attr.split()[-1])

        for attr in attr_names("scan thruput"):
            df[attr] = getattr(scan_tps, attr.split()[-1])
            
        print file_sizes
        print iter_times
        print iter_tps
        print scan_tps

        df.save()

    df.close()

if __name__ == '__main__':
    base_path = './experiments/diskindex/'

    if not os.path.exists(base_path):
        os.mkdir(base_path)

    config = {
        'blocksize': 64,
        'hitrate': 1,
        'keylength': 16,
        'path': '/tmp/babudb_diskindex_benchmark',
        'num_entries': 3000000,
        'num_lookups': 100000,
        'num_scans': 1,
        'base_dir': base_path,
        'compression': '',
#        'input': 'random',
        'input': '/home/mikael/lab/babudb/test/benchmark/all_home.dat',
        'seed': 0
        }

    cmd = """java -Xms512M -Xmx1500M -jar babudb.jar -blocksize {blocksize} -hitrate {hitrate} -keylength {keylength} -input {input} -scans {num_scans} {compression} {path} {num_entries} {num_lookups}"""

#    exec_single(cmd, config)
#    exec_repetitions(cmd, config, num_reps=3)

    # centralized
#    centralized_path = os.path.join(base_path, 'centralized')
#    if not os.path.exists(centralized_path):
#        os.mkdir(centralized_path)
#    config['base_dir'] = centralized_path

#    ranges = {'num_entries': [1000,10000,100000, 1000000], 'blocksize': [32,64], 'keylength': [8, 12]}
#    ranges = {'num_entries': [1000, 10000, 100000, 1000000, 2000000, 3000000, 0]} #, 100000,1000000,10000000]} # ,1000000
#    ranges = {'num_entries': [1000,10000]} # ,1000000
    ranges = {'blocksize': [16,32,64]} #, 100000,1000000,10000000]} # ,1000000

#    ranges = {'blocksize': [16,32,64,128,256,512]} # ,512,1024,2048,4096
    results = exec_ranges(cmd, config, ranges, num_reps=2, config=base_path)
    write_stats(ranges, results)

    config['compression'] = '-compression'
    results = exec_ranges(cmd, config, ranges, num_reps=2, config=base_path)
    write_stats(ranges, results, prefix="stats_compression")
        



