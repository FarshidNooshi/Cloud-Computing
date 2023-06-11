#!/usr/bin python3

from mrjob.job import MRJob
import csv

class TweetsByState(MRJob):

    def mapper(self, _, line):
        reader = csv.reader([line])
        fields = next(reader)
        if len(fields) >= 18 and fields[0] != 'created_at':
            state = fields[18]
            tweet_time = fields[0]
            tweet = fields[2]
            possible_states = ['New York', 'Texas', 'California', 'Florida']
            if 'Joe Biden' in tweet or 'Donald Trump' in tweet:
                hour = int(tweet_time[11:13])
                is_state_in_possible_states = any(state.lower() == possible_state.lower() for possible_state in possible_states)
                if hour >= 9 and hour <= 17 and is_state_in_possible_states:
                    yield (state, (1, 'both' if 'Joe Biden' in tweet and 'Donald Trump' in tweet else 'biden' if 'Joe Biden' in tweet else 'trump'))

    def reducer(self, key, values):
        total_tweets = 0
        both_tweets = 0
        biden_tweets = 0
        trump_tweets = 0

        for value in values:
            total_tweets += 1
            if value[1] == 'both':
                both_tweets += 1
            elif value[1] == 'biden':
                biden_tweets += 1
            elif value[1] == 'trump':
                trump_tweets += 1

        both_percentage = both_tweets / total_tweets
        biden_percentage = biden_tweets / total_tweets
        trump_percentage = trump_tweets / total_tweets
        output = f"{both_percentage:.4f}, {biden_percentage:.4f}, {trump_percentage:.4f}, {total_tweets}"
        yield (key, output)

if __name__ == '__main__':
    TweetsByState.run()
