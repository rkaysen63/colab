# import dependency
from mrjob.job import MRJob

# Create a class which inherits (takes properties) from the MRJob class.
class Bacon_count(MRJob):
    # Create a mapper function to assign the nput to key-value pairs
    def mapper(self, _, line):
        for word in line.split():
            if word.lower() == "bacon":
                yield "bacon", 1
    # Create a reducer function to sum all values created in mappef function
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == "__main__":
   Bacon_count.run()