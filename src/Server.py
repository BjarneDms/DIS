"""This is the server class script used in data generation"""

import random
class Server:
    def __init__(self, name, response, timestamp, pred, dup):
        self.name = name  #name of server (S1): string
        self.response = response  #is the process passing a request or response: boolean
        self.timestamp = self.retime(timestamp)  #request or response time: int
        self.pred = pred  #predecessor(s) of current node: [Server]
        self.dup = dup  #duplicates of the current server: [Server]


    def retime(self, timestamp):
        """Correct timestamp based on request or response status """
        #If response add 50-75 ms
        if self.response:
            return timestamp + random.randint(50,75)

        #If request add 0-10 ms
        return timestamp + random.randint(0,10)
