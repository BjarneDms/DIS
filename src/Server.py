"""This is the server class script used in data generation"""

import random
class Server:
    def __init__(self, name, response=None, timestamp=None, pred=None, suc=None, dup=None):
        self.name = name  #name of server (S1): string
        self.response = response  #is the process passing a request or response: boolean
        self.timestamp = timestamp #self.retime(timestamp)  #request or response time: int
        self.pred = pred  #predecessor(s) of current node: [Server]
        self.suc = suc
        self.dup = dup  #duplicates of the current server: [Server]

    def __repr__(self):
        return f"Server(name='{self.name}', response={self.response}, timestamp={self.timestamp}, pred={[self.pred]}, suc={[self.suc]}, dup={[self.dup]})"


"""
    def retime(self, timestamp):
        #Correct timestamp based on request or response status 
        #If response add 50-75 ms
        if self.response:
            return timestamp + random.randint(50,75)

        #If request add 0-10 ms
        return timestamp + random.randint(0,10)
"""
