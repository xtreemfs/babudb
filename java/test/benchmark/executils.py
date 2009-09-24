import os

from subprocess import Popen, call, PIPE

from datafile import DataFile

def exec_single(cmd, args):
    pipe = Popen(cmd.format(**args), shell=True, stdout=PIPE).stdout
    return pipe.read().decode().strip()[:]
    
def exec_pbs_single(cmd, args, pbs_queue, pbs_name='storagesim'):
    pbs_config = {
        'cmd': cmd.format(**args),
        'pbs_name': pbs_name,
        'pbs_queue': pbs_queue
        }

    batch_script = """
# generate the job
#!/bin/sh

#PBS -N {pbs_name}
#PBS -q {pbs_queue}
#PBS -l nodes=1
#PBS -l walltime=120:00:00

export PYTHONPATH=.
cd ~/lab/storagesim/
{cmd}
""".format(**pbs_config)
 
    #print(batch_script)
    (fd, filename) = mkstemp()
    f = os.fdopen(fd, "w")
    f.write(batch_script)
    f.close()
    call(["qsub", filename])
    #os.remove(filename)

# execute a single command with several repetitions
# compatible with management.py
def exec_repetitions(cmd, args, num_reps=10, pbs_queue=None, pbs_name='storagesim'):
    args_tmp = args.copy()
    base_path = args_tmp['base_dir']
    seed = args_tmp['seed']
    outputs = []

    for i in range(num_reps):
        exec_path = os.path.join(base_path, '%d' % i)

        args_tmp['base_dir'] = exec_path
        args_tmp['seed'] = seed + i

        print "repetition", i, args_tmp

        if not pbs_queue:
            outputs.append(exec_single(cmd, args_tmp))
        else:
            exec_pbs_single(cmd, args_tmp, pbs_queue, pbs_name=pbs_name)
            
    return outputs

def range_gen(start, stop, step, exp=False):
    """
    Returns a generator for the range. When `exp` is `True`, the steps
    are exponential. 
    """
    val = start
    while val <= stop:
        yield val

        if exp:
            val *= step
        else:
            val += step

def permutations(range_attrs):
#    range(len(range_attrs[attr]))
    attr_indices = dict([(attr, 0) for attr in range_attrs])

#    attr_index = len(range_attrs)-1

    range_attr_items = list(range_attrs.items())
    
    max_num_runs = 1
    for attr in range_attrs:
        max_num_runs *= len(range_attrs[attr])

#    print("runs: ", max_num_runs)

#    curr_attr = range_attr_items[attr_index][0]

    # index into current attribute
    attr_index = 0
    # index into the current value of the current attribute
    attr_val_index = 0
    num_attrs = len(range_attrs)
    num_runs = 0

    # when current val has reached its max

    # increase the next val which has not reached its max with one and
    # return to the current val
    

    finished = False
    while not finished:
        # return list of (attr, val)-pairs for each attr
        yield [(attr, range_attrs[attr][attr_indices[attr]]) for attr in range_attrs]

        # increase the current attr index
#        curr_attr = range_attr_items[attr_index][0]

        # find the value to increase

        # - when a more significant value is increased, reset the
        #   previous values,

        # - if the most significant value (to the "right") is maxed
        #   out, we are finished

#        attr_val_index += 1
#        attr_indices[curr_attr] = attr_val_index

        inc_sig_index = 0
        inc_sig_attr, inc_sig_vals = range_attr_items[inc_sig_index]
#        print("inc sig before: ", inc_sig_attr, inc_sig_vals, attr_indices[inc_sig_attr], len(inc_sig_vals))
        while attr_indices[inc_sig_attr] >= (len(inc_sig_vals)-1):
            inc_sig_index += 1
            if inc_sig_index < num_attrs:
                inc_sig_attr, inc_sig_vals = range_attr_items[inc_sig_index]
            else:
                finished = True
                break

#            print("inc sig: ", inc_sig_attr, inc_sig_vals, attr_indices[inc_sig_attr], len(inc_sig_vals))

#                break

        # reset all attrs up to this index
        reset_attrs = [a for a, l in range_attr_items[:inc_sig_index]]
        for attr in reset_attrs:
            attr_indices[attr] = 0

        # increase the next significant attr index 
        attr_indices[inc_sig_attr] += 1

#        if attr_indices >= len(range_attrs[curr_attr]):
#            attr_val_index = 0
#            attr_index = (attr_index + 1) % num_attrs


#        print("curr attr: ", attr_indices, reset_attrs, inc_sig_index, num_attrs)


        num_runs += 1


def exec_ranges(cmd, args, range_attrs, num_reps=10, pbs_queue=None, pbs_name='storagesim', config=None):
    # range_attrs is in the form:
    # range_val: [list of possible values]
    #
    # when several range_vals and value lists are given, all possible
    # permutations are executed

    base_path = args['base_dir']

    if config != None:
        df = DataFile(os.path.join(config, 'config_ranges.dat'), list(range_attrs.keys()), overwrite=True)

    outputs = []

    for attr_p in permutations(range_attrs):
        # set the attrs accordingly for this exec
        names = [] 
        for attr, val in attr_p:
            args[attr] = val
            names.append('%s_%s' % (str(attr), str(val)))

            if config != None:
                df[attr] = val

        if config != None:
            df.save()

        args['base_dir'] = os.path.join(base_path, '%s' % ('_'.join(names)))
        if not os.path.exists(args['base_dir']):
            os.mkdir(args['base_dir'])

        outputs.append((attr_p, exec_repetitions(cmd, args, num_reps=num_reps, pbs_queue=pbs_queue, pbs_name=pbs_name)))


    if config != None:
        df.close()

    return outputs

if __name__ == '__main__':
    a = permutations({'a': [1,2,3], 'b':[4,8]})
    print([v for v in a])
