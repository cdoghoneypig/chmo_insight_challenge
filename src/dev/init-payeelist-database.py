import sys
import csv
import bisect
import json
from timeit import default_timer as timer

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

start = timer()

# real version
base_transactions_file = open(sys.argv[1], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/batch_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

base_transactions = csv.reader(base_transactions_file)
reader = base_transactions

errorlog = open("feature1-init-error-log.txt", "w")


end = timer()
print("loading files", int((end - start)*1000), "ms")

start = timer()

takers = {}
count = 0
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
        errorlog.write("row " + str(count) + " is broken:" + "\n")
        # errorlog.write(str(row) + "\n")
    except ValueError:
        errorlog.write("row " + str(count) + " does not contain valid IDs:" + "\n")
        # errorlog.write(str(row) + "\n")

end = timer()
print("database of", "{:,}".format(count), "records intialized in", round((end - start),1), "seconds")


base_transactions_file.close()

with open (sys.argv[2], "w") as outfile:
    json.dump(takers, outfile, cls=SetEncoder, indent=4)


    