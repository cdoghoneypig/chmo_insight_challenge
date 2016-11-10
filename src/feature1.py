import sys
import csv
import bisect
import json
from timeit import default_timer as timer

# class SetEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, set):
#             return list(obj)
#         return json.JSONEncoder.default(self, obj)


def error_Bad_Row (n,r):
    errorlog.write("row " + str(n) + " is broken:" + "\n")
    errorlog.write(str(r) + "\n")


def error_Bad_ID(n,r):
    errorlog.write("row " + str(n) + " does not contain valid IDs:" + "\n")
    errorlog.write(str(r) + "\n")




start = timer()

# real version
base_transactions_file = open(sys.argv[1], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/batch_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

base_transactions = csv.reader(base_transactions_file)
reader = base_transactions

errorlog = open("feature1-errorlog.txt", "w")


end = timer()
print("\n\nFeature 1: 1st Degree Friend Check\n")
print("Loaded files to intialize database in", int((end - start)*1000), "ms")
print("Intializing database now...")

start = timer()

takers = {}
count = 0
row_num = 0

seg_time = timer()
for row in reader:
    # tracking time per 10k records
    # if count % 10000 == 0:
    #     print("10,000 records in",int((timer()-seg_time)*1000), "ms: ", count)
    #     seg_time = timer()

    try:
        # We assume ID numbers are integers, so no 008 vs 08 etc
        # in this challenge, that's not assured, but the data does look that way
        # and in another setting, that could be discussed beforehand.
        giver_id = int(row[1])
        taker_id = int(row[2])
        try:
            if taker_id not in takers[giver_id]:
                if giver_id not in takers[taker_id]:
                    # bisect.insort(takers[giver_id],taker_id)
                    # testing with set
                    takers[giver_id].add(taker_id)

        except KeyError:
            # takers[giver_id] = [taker_id]
            
            # testing with set
            takers[giver_id] = set([taker_id])
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

print("\n\Analyzing streaming data now...\n")


start = timer()

# open payment stream
stream_transactions_file = open(sys.argv[2], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/stream_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

reader = csv.reader(stream_transactions_file)


errorlog.write("\n\n\n\nStreaming Data\n\n\n\n")
# open output file
output1 = open(sys.argv[3], "w")
# output1 = open("./paymo_output/output1.txt", "w")


end = timer()
print("Files for streaming loaded in ", int((end - start)*1000), "ms")


start = timer()
seg_time = timer()

count = 0
row_num = 0

for row in reader:    
    # if count % 10000 == 0:
    #     print("10,000 records in",int((timer()-seg_time)*1000), "ms: ", count)
    #     seg_time = timer()

    try:
        giver_id = int(row[1])
        taker_id = int(row[2])
        try:
            if taker_id in takers[giver_id]:
                output1.write("trusted\n")
            else:
                if giver_id in takers[taker_id]:
                    output1.write("trusted\n")
                else:
                    output1.write("unverified\n")
                    # bisect.insort(takers[giver_id],taker_id)
                    
                    # now using sets
                    takers[giver_id].add(taker_id)
        except KeyError:
            takers[giver_id] = set([taker_id])

            # print (takers[row[1]])
        count+=1
    except IndexError:
        error_Bad_Row(row_num, row)
    except ValueError:
        error_Bad_ID(row_num, row)
    row_num += 1

end = timer()
print("Database of", "{:,}".format(count), "streamed records analyzed in", round((end - start),1), "seconds")
print("{:,}".format(int(count/(end-start))), "records per second")

output1.close()
stream_transactions_file.close()
errorlog.close()

    