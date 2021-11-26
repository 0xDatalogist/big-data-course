# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.

import sys

print 'Creating BFS starting input for character ' + sys.argv[1]

with open("BFS-iteration-0.txt", 'w') as out:

    with open("Marvel-graph.txt") as f:

        for line in f:
            fields = line.split()
            heroID = fields[0]
            numConnections = len(fields) - 1
            connections = fields[-numConnections:]

            color = 'WHITE'
            distance = 9999

            if (heroID == sys.argv[1]) :
                color = 'GRAY'
                distance = 0

            if (heroID != ''):
                edges = ','.join(connections)
                outStr = '|'.join((heroID, edges, str(distance), color))
                out.write(outStr)
                out.write("\n")


    f.close()

out.close()
