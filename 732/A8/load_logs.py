import sys, os, uuid, gzip, re
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def disassemble_lines(line):
    match = re.search(line_re,line)
    if match:
        host = match.group(1)
        datetimes = datetime.strptime(match.group(2),'%d/%b/%Y:%H:%M:%S')
        path = match.group(3)
        bytes_transfer = int(match.group(4))
        return host,datetimes,path,bytes_transfer
    return None

def main(input_dir, keyspace, table):

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    batch = BatchStatement()
    insert_data = session.prepare("INSERT INTO %s(host,id,datetime,path,bytes) VALUES (?,?,?,?,?)"%(table))
    batch_count = 0
    
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                elements = disassemble_lines(line)
                if elements is not None:
                    batch.add(insert_data, (elements[0],uuid.uuid4(),elements[1],elements[2],elements[3]))
                    batch_count += 1
                if batch_count == 100:
                    session.execute(batch)
                    batch.clear()
                    batch_count = 0

if __name__ == '__main__':

    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    main(input_dir, keyspace, table)