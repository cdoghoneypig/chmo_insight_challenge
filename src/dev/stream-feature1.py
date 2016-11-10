import sys
import csv
import bisect
import json
from timeit import default_timer as timer

'''
load the json database initialized in last script
use same process on 2nd input stream
write to txt file

'''


start = timer()

# open payment database
# real version
base_transactions_file = open(sys.argv[1], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/stream_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

reader = csv.reader(base_transactions_file)

# open error log, json, and output
errorlog = open("feature1-error-log.txt", "w")
with open(sys.argv[2]) as data_file:    
    takers = json.load(data_file)
output1 = open(sys.argv[3], "w")
# output1 = open("./paymo_output/output1.txt", "w")


end = timer()
print("loading files", int((end - start)*1000), "ms")


start = timer()
count = 0
seg_time = timer()

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
                    bisect.insort(takers[giver_id],taker_id)
        except KeyError:
            takers[giver_id] = [taker_id]
            # print (takers[row[1]])
        count+=1
    except IndexError:
        errorlog.write("row" + str(count) + "is broken:" + "\n")
        errorlog.write(str(row) + "\n")
    except ValueError:
        errorlog.write("row" + str(count) + "does not contain valid IDs:" + "\n")
        errorlog.write(str(row) + "\n")

end = timer()
print("database of", "{:,}".format(count), "records intialized in", round((end - start),1), "seconds")

output1.close()
base_transactions_file.close()
errorlog.close()

with open ("./initalized_database/payee_lists_endstate.json", "w") as outfile:
    json.dump(takers, outfile, indent=4)