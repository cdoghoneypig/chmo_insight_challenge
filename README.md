# chmo_insight_challenge
Insight coding challenge Nov 2016

Written in Python 3.5
Using sys, csv, json, and timeit

All features conducted in a single pass of the streaming data. Feature 3 is much much slower than features 1 and 2 by themselves, however, since it's all ultimately run from one script, I've combined the features together as the analytic work of features 1 and 2 is already included in feature 3. 
Features 4 and 5 piggyback in the analytic cycle, and also draw their own data from the original source.

Overall, the script opens the batch input and creates a list(a set, actually) of first degree connections for each user, as well as a dictionary of total amounts received and transactions in which a user received money. 
In its second phase, the script loads streaming data and processes each row in the following way:
1. Updates the total amount and quantity of transactions per user
2. Checks for degree of connection. First, it asks if the taker is on the giver's friends list.
If not, it checks if there is a 2nd degree connection—any overlap between the two users' friends lists (using isdisjoint). 
If not, it checks for 3rd degree. To do this, it generates a list of 2nd degree connections for the giver, joining the friends list of every user on the giver's friends list, then check for an overlap between this "secondary" list of the giver and the friends list of the taker.
If that fails, it checks for a 4th degree connection by creating a "secondary" list for the taker, then checking for an overlap between this list and the giver's secondary list.
3. We, nonetheless, count the transaction as provisionally valid, adding the taker to the giver's friends list and vice versa.

Feature 3 could have been written many other ways, but the temporary "secondary" lists, which are built then disposed of as needed on the fly, perform much better than BFS and other approaches that I tried. Sadly, it is still rather slow, compared to the other features, processing only about 1,000 records per second on my computer, with that number falling as the dataset grows.

Features 4 and 5 seem the most revealing, and would be particularly effective if paired with features 1 and 2. A surprising number of users collect many tens of thousands of dollars during the 3 hour period under review, and over a hundred users made more than 10 transactions per minute during this time.
The output from features 4 and 5 are csv files listing the most suspicious users. This could be used in establishing reputation ratings or directing some secondary automated (or manual) investigation process.

Of course, IP tracking, geolocation, stricter authentication procedures, and checks for user name/profile spoofing can help monitor payments for fraud, but they are outside the scope of this challenge.

Thanks for the challenge!
-Chuk
