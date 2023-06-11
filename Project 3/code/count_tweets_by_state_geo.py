#!/usr/bin python3

from mrjob.job import MRJob
import csv

class CountTweetsByStateGeo(MRJob):

    def mapper(self, _, line):
        reader = csv.reader([line])
        fields = next(reader)
        if len(fields) >= 7 and fields[0] != 'created_at':
            tweet = fields[2]
            created_at = fields[0]
            lat = float(fields[13]) if fields[13] else 0
            long = float(fields[14]) if fields[14] else 0

            if (45.0153 <= lat <= 79.7624) and (32.5121 <= long <= 124.6509):
                state = "California"
            elif (40.4772 <= lat <= 45.0153) and (-79.7624 <= long <= -71.7517):
                state = "New York"
            else:
                return

            if "Donald Trump" in tweet or "Joe Biden" in tweet:
                yield state, (1 if "Donald Trump" in tweet else 0, 1 if "Joe Biden" in tweet else 0)

    def reducer(self, key, values):
        total_tweets = 0
        both_tweets = 0
        total_trump = 0
        total_biden = 0

        for trump, biden in values:
            total_tweets += 1
            total_trump += trump
            total_biden += biden
            if trump and biden:
                both_tweets += 1

        both_percentage = both_tweets / total_tweets
        biden_percentage = total_biden / total_tweets
        trump_percentage = total_trump / total_tweets
        output = f"{both_percentage:.8f}, {biden_percentage:.4f}, {trump_percentage:.4f}, {total_tweets}"
        yield (key, output)

if __name__ == '__main__':
    CountTweetsByStateGeo.run()
