#!/usr/bin python3

from mrjob.job import MRJob
import csv

class CountCandidates(MRJob):

    def mapper(self, _, line):
        reader = csv.reader([line])
        fields = next(reader)
        if len(fields) >= 7 and fields[0] != 'created_at':
            tweet = fields[2]
            likes = float(fields[3])
            retweets = float(fields[4])
            source = fields[5]
            candidates = ["Both Candidate", 'Donald Trump', 'Joe Biden']

            for candidate in candidates:
                if candidate.lower() in tweet.lower():
                    yield (candidate, (1, likes, retweets, source))

    def reducer(self, key, values):
        count = 0
        total_likes = 0
        total_retweets = 0
        source_counts = {'Twitter Web App': 0, 'Twitter for iPhone': 0, 'Twitter for Android': 0}

        for value in values:
            count += value[0]
            total_likes += value[1]
            total_retweets += value[2]
            if value[3] in source_counts:
                source_counts[value[3]] += 1

        yield (key, (total_likes, total_retweets, *source_counts.values()))

if __name__ == '__main__':
    CountCandidates.run()
