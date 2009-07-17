# -*- coding: iso-8859-15 -*-
# (c) Mikael Hoegqvist
import os

class EmptyDataFile(Exception):
    pass

class DataFile(object):
    def __init__(self, path, attributes, buffered_rows = 10000, overwrite=False, append=False):
        self.__null_value = ''
        self.__attributes = attributes
        self.__columns = dict(zip(attributes, [self.__null_value for i in range(len(attributes))]))
        
        if os.path.exists(path) and not overwrite and not append:
            raise Exception("Path %s exist already." % path)

        self.__rows = []
        self.__max_rows = buffered_rows

        if append:
            # only open, dont write out the header
            self.__f = open(path, "a")
        else:
            self.__f = open(path, 'w')
            self.__f.write('# ' + '\t'.join(["(%d) %s" % (i+1, attributes[i]) for i in range(len(attributes))]) + '\n')

    def __dump_rows(self):
        # write out all rows
        self.__f.write('\n'.join(['\t'.join([str(val) for val in row]) for row in self.__rows]) + '\n')
        self.__rows = []

    def save(self):
        self.__rows.append([self.__columns[attr] for attr in self.__attributes])

        if len(self.__rows) > self.__max_rows:
            self.__dump_rows()
            
        # clear the current values
        for name in self.__columns:
            self.__columns[name] = self.__null_value

    def __getitem__(self, name):
        if not name in self.__columns:
            raise KeyError("'%s' not a column name" % (name))

        return self.__columns[name]

    def __setitem__(self, name, value):
        if not name in self.__columns:
            raise KeyError("'%s' not a column name" % (name))

        # we only handle strings when writing to the file
        self.__columns[name] = value

    def flush(self):
        self.__dump_rows()
        self.__f.flush()

    def close(self):
        self.__dump_rows()
        self.__f.close()

    def __del__(self):
        try:
            self.close()
        except ValueError:
            # trying to write to a closed file
            pass

#        print("datafile __del__", self.__f)

class ReadDataFile(object):
    def __init__(self, path):
        try:
            self.attrs = self.__read_header(path)
        except IOError:
            raise EmptyDataFile(path)

        self.columns, self.rows = self.__read_lines(path, self.attrs)

        if self.attrs == []:
            raise EmptyDataFile(path)

    def __read_header(self, path):
        f = open(path, "r")
        l = f.readline()[2:]
        attrs = []

        for attr in l.split("\t"):
            # remove the (<id>)
            val = attr[attr.find(")") + 1:].strip()
            if val != '':
                attrs.append(val)

        f.close()
        return attrs
                
    def __read_lines(self, path, attrs):
        f = open(path, "r")
        
        columns = {}
        rows = []

        for l in f:
            if l.startswith("#"):
                continue

            row = {}
            for (attr, val) in zip(attrs, l.split("\t")):
                val = val.strip()
                try:
                    columns[attr].append(val)
                except KeyError:
                    columns[attr] = [val]

                row[attr] = val
                
            rows.append(dict(row))

        f.close()
        return columns, rows

if __name__ == '__main__':
    df = DataFile('./blub.txt', ['b', 'a'], overwrite=True)
    df['a'] = 'a'
    df['b'] = 'b'
    df.save()

    df['a'] = 'c'
    df['b'] = 'd'
    df.save()

    df['a'] = 'e'
    df['b'] = 'f'
    df.save()

    df['a'] = 'g'
    df['b'] = 'h'
    df.save()
    df.close()

    rdf = ReadDataFile("./blub.txt")
    print(rdf.columns)
    print(rdf.rows)

    rdf = ReadDataFile("./empty.dat")
