import sys
f = open(sys.argv[1])
lines = f.read().split('=' * 32)
lines = lines[1].split('-' * 32)
for line in lines:
    line = line.strip()
    if line:
        sublines = line.split('\n')
        sublines = map(lambda x: x.split('=')[1].strip(), sublines)
        sublines[0] = sublines[0][:-3]
        print ','.join(sublines)
f.close()