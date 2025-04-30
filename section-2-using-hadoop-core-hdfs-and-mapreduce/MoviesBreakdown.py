from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_views)
        ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_views(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MoviesBreakdown.run()
