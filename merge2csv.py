import csv

in_1_name = "/Users/akashmalla/Documents/COEN 242/spark dataset/WDATA_Q1.csv"
in_2_name = "/Users/akashmalla/Documents/COEN 242/spark dataset/wdata_results/wdata_q1.csv"
out_name = "/Users/akashmalla/Documents/COEN 242/spark dataset/wdata_results/wdata_q1_cluster.csv"

with open(in_1_name) as in_1, open(in_2_name) as in_2, open(out_name, 'w') as out:
    reader1 = csv.reader(in_1, delimiter=",")
    reader2 = csv.reader(in_2, delimiter=",")
    writer = csv.writer(out, delimiter=",")
    for row1, row2 in zip(reader1, reader2):
        writer.writerow([row1[0], row1[1], row1[6], row1[7], row1[8], row1[9], row2[0]])