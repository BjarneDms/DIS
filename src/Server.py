"""This is the server class script used in data generation"""

import random
class Server:
    def __init__(self, name, timestamp, response):
        self.name = name
        self.response = response
        self.timestamp = self.retime(timestamp)

    def retime(self, timestamp):
        """Correct timestamp based on request or response status """
        #If response add 50-75 ms
        if self.response:
            return timestamp + random.randint(50,75)

        #If request add 0-10 ms
        return timestamp + random.randint(0,10)
