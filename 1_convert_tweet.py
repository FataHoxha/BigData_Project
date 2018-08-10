import csv

file_name = 'tweets2009-06.txt'
tweet_dict = {}
with open(file_name, 'r') as in_file:
    user_id = []
    text = []
    date = []
    counter = 0
    for line in in_file:
        if (line.startswith("U")):
            user_id.append(line[2:].rstrip())
        elif (line.startswith("W")):
            text.append(line[2:].rstrip())
        elif (line.startswith("T")):
            date.append(line[2:].rstrip())
        counter += 1
        values = zip(user_id, text, date)
    tweet_dict[counter] = (values)


    with open('tweet.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(('user_id', 'text', 'date'))
        for key, values in tweet_dict.items():
            writer.writerows(values)
