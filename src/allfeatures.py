import sys
import csv
import json
from timeit import default_timer as timer


def error_Bad_Row (n,r):
    errorlog.write("row " + str(n) + " is broken:" + "\n")
    errorlog.write(str(r) + "\n")


def error_Bad_ID(n,r):
    errorlog.write("row " + str(n) + " does not contain valid IDs:" + "\n")
    errorlog.write(str(r) + "\n")


base_transactions_file = open(sys.argv[1], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/batch_payment.csv"
# base_transactions_file = open(cur_file, 'r', encoding='utf-8')

base_transactions = csv.reader(base_transactions_file)
reader = base_transactions

errorlog = open("errorlog.txt", "w")

print("\n\nIntializing database now...\n")

friends = {}

count = 0
row_num = 0

start = timer()
seg_time = timer()
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
        '''
        For initialization, we assume taker_id is usually not in friend or 
        secondary so will simply add it to the set.
        This handles the if/then of membershipinternally.
        However, if the relevant friends list doesn't exist at all,
        we create it with this member.
        '''
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




print("\n\nAnalyzing streaming data now...\n")

# open payment stream
stream_transactions_file = open(sys.argv[2], 'r', encoding='utf-8')

# for use in console
# cur_file = "./paymo_input/stream_payment.csv"
# stream_transactions_file = open(cur_file, 'r', encoding='utf-8')

reader = csv.reader(stream_transactions_file)

output = [
            open(sys.argv[3], "w"),
            open(sys.argv[4], "w"),
            open(sys.argv[5], "w")
        ]

# for use in console
# output1 = open("./paymo_output/output1.txt", "w")

next(reader)
row_num = 1
count = 0
last_count = 0

start = timer()
seg_time = timer()

trusted_msg = "trusted\n"
unverified_msg = "unverified\n"

for row in reader:    
    if count % 10000 == 0:
        try:
            1/(count-last_count)
            print("{:,}".format(round((count-last_count)/(timer()-seg_time))), 
                "records/sec at row", "{:,}".format(row_num))
            # print("10k records in",int((timer()-seg_time)*1000), "ms: ", count)
            seg_time = timer()
            last_count = count
        except:
            pass
    try:
        verified = [False, False, False]
        giver_id = int(row[1])
        taker_id = int(row[2])
        try:
            # Starting from most likely relationship to least
            # If no relation at that level, try next
            # if any if statement fails, verified = true
            # check if 1st degree friends
            if giver_id not in friends[taker_id]:
                # check if 2nd degree
                if friends[giver_id].isdisjoint(friends[taker_id]):
                    # check if third degree
                    secondary = {}
                    secondary[giver_id] = set([])
                    for each_friend in friends[giver_id]:
                        secondary[giver_id] = secondary[giver_id] | friends[each_friend]
                    if friends[taker_id].isdisjoint(secondary[giver_id]):
                        # check if fourth degree
                        secondary[taker_id] = set([])
                        for each_friend in friends[taker_id]:
                            secondary[taker_id] = secondary[taker_id] | friends[each_friend]
                        if secondary[giver_id].isdisjoint(secondary[taker_id]):
                            # not even a fourth degree friend
                            pass
                        else:
                            verified[2] = True
                    else:
                        verified[2] = True
                else:
                    verified[2] = True
                    verified[1] = True
            else:
                verified[2] = True
                verified[1] = True
                verified[0] = True
            # Absorb each other's id to friend list
            # and each other's friend list to secondary list
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
            # someone has no friend list
            if giver_id not in friends:
                friends[giver_id] = set([taker_id])
            if taker_id not in friends:
                friends[taker_id] = set([giver_id])
        for idx, msg in enumerate(verified):
            if msg: 
                output[idx].write(trusted_msg)
            else:
                output[idx].write(unverified_msg)
        count+=1
    except IndexError:
        error_Bad_Row(row_num, row)
    except ValueError:
        error_Bad_ID(row_num, row)
    row_num += 1

end = timer()
print("Database of", "{:,}".format(count), "streamed records analyzed in", round((end - start),1), "seconds")
print("{:,}".format(int(count/(end-start))), "records per second")

for each_out in output:
    each_out.close()
stream_transactions_file.close()
errorlog.close()