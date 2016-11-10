import sys
import csv
import bisect
import json
from timeit import default_timer as timer


def error_Bad_Row (n,r):
    errorlog.write("row " + str(n) + " is broken:" + "\n")
    errorlog.write(str(r) + "\n")


def error_Bad_ID(n,r):
    errorlog.write("row " + str(n) + " does not contain valid IDs:" + "\n")
    errorlog.write(str(r) + "\n")

def bfs_paths(graph, start, goal):
    queue = [(start, [start])]
    while queue:
        (vertex, path) = queue.pop(0)
        for next in graph[vertex] - set(path):
            if next == goal:
                yield path + [next]
            else:
                queue.append((next, path + [next]))


def shortest_path(graph, start, goal):
    try:
        return next(bfs_paths(graph, start, goal))
    except StopIteration:
        return None

start = timer()

# real version
base_transactions_file = open(sys.argv[1], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/batch_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

base_transactions = csv.reader(base_transactions_file)
reader = base_transactions

errorlog = open("feature3-errorlog.txt", "w")

end = timer()

print("\n\nFeature 3: 4th Degree Friend Check\n")
print("Loaded files to intialize database in", int((end - start)*1000), "ms")
print("Intializing database now...")

start = timer()
friends = {}
seg_time = timer()

count = 0
row_num = 0
for row in reader:
    # tracking time per 10k records
    # if count % 10000 == 0:
    #     print("10,000 records in",int((timer()-seg_time)*1000), "ms: ", count)
    #     seg_time = timer()
    #     print("Records for", len(friends), "users")
    try:
        # We assume ID numbers are integers, so no 008 vs 08 etc
        # in this challenge, that's not assured, but the data does look that way
        # and in another setting, that could be discussed beforehand.
        giver_id = int(row[1])
        taker_id = int(row[2])
        # For initialization, we assume taker_id is usually not in friend or secondary
        # so will simply add it to the set. This handles the if/then of membership
        # internally.
        # If the friends list doesn't exist at all, create it with this member.
        try:
            friends[giver_id].add(taker_id)
        except KeyError:
            friends[giver_id] = set([taker_id])        
        try:
            friends[taker_id].add(giver_id)
        except KeyError:
            friends[taker_id] = set([giver_id])
        count+=1
    except IndexError:
        error_Bad_Row(row_num, row)
    except ValueError:
        error_Bad_ID(row_num, row)
    row_num += 1

end = timer()
print("Database of", "{:,}".format(count), "old records intialized in", round((end - start),1), "seconds")
print("{:,}".format(int(count/(end-start))), "records per second")

base_transactions_file.close()







print("Analyzing streaming data now...")


start = timer()

# open payment stream
# stream_transactions_file = open(sys.argv[2], 'r', encoding='utf-8')

# for use in console
cur_file = "./paymo_input/stream_payment.csv"
stream_transactions_file = open(cur_file, 'r', encoding='utf-8')

reader = csv.reader(stream_transactions_file)
# output3 = open(sys.argv[3], "w")
output3 = open("./paymo_output/output3.txt", "w")


end = timer()
print("Files for streaming loaded in ", int((end - start)*1000), "ms")


start = timer()
seg_time = timer()

row_num = 0
count = 0

for row in reader:    
    # if count % 10000 == 0:
    #     print("10,000 records in",int((timer()-seg_time)*1000), "ms: ", count)
    #     seg_time = timer()
    verified = False
    try:
        giver_id = int(row[1])
        taker_id = int(row[2])
        try:
            # Treat friends dict as a graph structure and find shortest path
            # Begin search from user with less friends
            if len(friends[giver_id]) > len(friends[taker_id]):
                short_path = shortest_path(friends, giver_id, taker_id)
            else:
                short_path = shortest_path(friends, taker_id, giver_id)
            print("path from", giver_id, "to", taker_id, "is", short_path)
            # add taker to giver's friend list
            try:
                friends[giver_id].add(taker_id)
            except KeyError:
                friends[giver_id] = set([taker_id])        
            # add giver to taker's friend list
            try:
                friends[taker_id].add(giver_id)
            except KeyError:
                friends[taker_id] = set([giver_id])
        except KeyError:
            # someone has no friend or secondary list
            if giver_id not in friends:
                friends[giver_id] = set([taker_id])
            if taker_id not in friends:
                friends[taker_id] = set([giver_id])
        if verified: 
            output3.write("trusted\n")
        else:
            output3.write("unverified\n")
        count+=1
    except IndexError:
        error_Bad_Row(row_num, row)
    except ValueError:
        error_Bad_ID(row_num, row)
    row_num += 1

end = timer()
print("Database of", "{:,}".format(count), "streamed records analyzed in", round((end - start),1), "seconds")
print("{:,}".format(int(count/(end-start))), "records per second")

output3.close()
stream_transactions_file.close()
errorlog.close()