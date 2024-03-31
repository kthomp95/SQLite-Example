import sqlite3
import json
import urllib.request
import matplotlib.pyplot as plt
import re
# import numpy as np
import time
import os

# create variables for sql tables

cmd = "DROP TABLE Tweets2;"
tbl = """
CREATE TABLE Tweets2
(
  created_at       VARCHAR2(35),
  id_str      VARCHAR2(35),
  user_id VARCHAR(35),
  text VARCHAR2(284),
  source VARCHAR2(70),
  in_reply_to_user_id VARCHAR2(50),
  in_reply_to_screen_name VARCHAR2(50),
  in_reply_to_status_id VARCHAR2(20),
  retweet_count NUMBER(100000),
  contributors VARCHAR2(100),
  CONSTRAINT tweets_PK
    PRIMARY KEY (created_at, id_str,source,in_reply_to_user_id)
);
"""

cmd2 = "DROP TABLE UserTweets;"
tbl2 = """
CREATE TABLE UserTweets
(
  id      NUMBER(25),
  name VARCHAR2(284),
  screen_name VARCHAR2(70),
  description VARCHAR2(50),
  friend_count NUMBER(50),
  CONSTRAINT user_PK
    PRIMARY KEY (id),

    FOREIGN KEY (id)
        REFERENCES Tweets2(user_id)
);
"""

cmd3 = "DROP TABLE Geo;"
tbl3 = """
CREATE TABLE Geo
(
  created_at VARCHAR2(35), 
  Userid      NUMBER(25), 
  type VARCHAR2(20),
  longitude VARCHAR2(28),
  latitude VARCHAR(28),
  CONSTRAINT geo_PK
    PRIMARY KEY (created_at, longitude, latitude),

    FOREIGN KEY (Userid, created_at)
        REFERENCES Tweets2(user_id,created_at)
);
"""

# open sql database connection
conn = sqlite3.connect('DSC 450.db')
cursor = conn.cursor()


# Part 1a)
def part1a_110():
    """Open, read and write the tweets to a new .txt file"""
    print('Part 1a w/ 110k tweets:')
    startTime = time.time()
    webFD = urllib.request.urlopen('http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt')
    linesRead = 0 #initialize variable for how many lines are read
    if os.path.isfile("finalTweets.txt"):
        os.remove("finalTweets.txt")  # remove if it exists
    f = open('finalTweets.txt', 'w')  # create file to write to
    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w')

    for i in webFD:
        if linesRead >= 110000:
            break # end once 110k lines are read
        try:  # catch json decoding errors
            decoded_line = i.decode('utf-8')
            f.write((decoded_line))
            f.write('\n')

            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i)+'\n')
        except Exception as e: # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i)+'\n')
    endTime = time.time()
    f.close()
    webFD.close()
    errorTweets.close()

    totalTime = endTime - startTime
    print('total time to complete part 1a 110k:', totalTime)
    print('------------------------------------------------------------------')
    return totalTime


def part1a_550():
    """Open, read and write the tweets to a new .txt file"""
    print('Part 1a w/ 550k tweets:')
    startTime = time.time()
    webFD = urllib.request.urlopen('http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt')
    linesRead = 0
    if os.path.isfile("finalTweets.txt"):
        os.remove("finalTweets.txt")  # remove if it exists
    f = open('finalTweets.txt', 'w')  # create file to write to
    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w')

    for i in webFD:
        if linesRead >= 550000:
            break
        try:  # catch json decoding errors
            decoded_line = i.decode('utf-8')
            f.write((decoded_line))
            f.write('\n')

            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
    endTime = time.time()
    f.close()
    webFD.close()
    errorTweets.close()

    totalTime = endTime - startTime
    print('total time to complete part 1a 550k:', totalTime)
    print('------------------------------------------------------------------')
    return totalTime


# Part 1b)
def part1B_110():
    """This function will read and populate tables one line at a time from web page. Also this function will time
    how long it takes"""

    print('Part 1b w/ 110k Tweets:')

    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w') # .txt file to log errors

    # drop and create tables
    try:
        cursor.execute(cmd)
    except:
        pass
    cursor.execute(tbl)
    try:
        cursor.execute(cmd2)
    except:
        pass
    cursor.execute(tbl2)
    try:
        cursor.execute(cmd3)
    except:
        pass
    cursor.execute(tbl3)

    userIDs = [] # save ids not to duplicate users b/c PK

    startTime = time.time()
    webFD = urllib.request.urlopen('http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt')
    linesRead = 0
    for i in webFD:
        if linesRead >= 110000:
            break
        try:  # getting similar error to previous project where error tweets were logged
            decoded_line = i.decode('utf-8')  # Decode bytes to string
            tdict = json.loads(decoded_line)

            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = list(tweetInfo.values())
            cursor.execute("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", (tweetInsert[0], tweetInsert[1],
                                                                                tweetInsert[2], tweetInsert[3],
                                                                                tweetInsert[4], tweetInsert[5],
                                                                                tweetInsert[6],
                                                                                tweetInsert[7], tweetInsert[8],
                                                                                tweetInsert[9]))

            userInfo = {'id': tdict['user']['id'], 'name': tdict['user']['name'],
                        'screen_name': tdict['user']['screen_name'],
                        'description': tdict['user']['description'],
                        'friend_count': tdict['user']['friends_count']}
            if tdict['user']['id'] in userIDs:
                # don't want to put duplicate user into the list multiple times as ID will be primary key
                pass
            elif tdict['user']['id'] is None:
                pass
            else:
                userIDs.append(tdict['user']['id'])
                userInsert = list(userInfo.values())
                cursor.execute("INSERT INTO UserTweets VALUES(?,?,?,?,?);", (userInsert[0], userInsert[1],
                                                                             userInsert[2], userInsert[3],
                                                                             userInsert[4]))
            if tdict['geo'] is not None:
                geoInfo = {"created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                           "type": tdict['geo']['type'], "longitude": tdict['geo']['coordinates'][0],
                    "latitude": tdict['geo']['coordinates'][1]}
                geoInsert = list(geoInfo.values())
                cursor.execute("INSERT INTO Geo VALUES(?,?,?,?,?);", (geoInsert[0], geoInsert[1],
                                                                    geoInsert[2], geoInsert[3], geoInsert[4]))
            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

    tweetCount = cursor.execute("SELECT COUNT(DISTINCT id_str) FROM Tweets2;").fetchall()
    userCount = cursor.execute("SELECT COUNT(DISTINCT id) FROM UserTweets;").fetchall()
    geoCount = cursor.execute("SELECT COUNT(DISTINCT longitude) FROM Geo;").fetchall()

    endTime = time.time()
    webFD.close()
    errorTweets.close()
    oneB110 = endTime - startTime
    print('Total Tweets:', tweetCount)
    print('Total Users:', userCount)
    print('Total Geos:', geoCount)
    print("Total time to complete 1b) ", endTime - startTime)
    print('------------------------------------------------------------------')
    return oneB110


def part1B_550():
    print('Part 1b w/ 550k Tweets')

    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w')

    cursor.execute(cmd)
    cursor.execute(tbl)
    cursor.execute(cmd2)
    cursor.execute(tbl2)
    cursor.execute(cmd3)
    cursor.execute(tbl3)
    userIDs = []
    webFD = urllib.request.urlopen('http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt')

    startTime = time.time()
    linesRead = 0
    for i in webFD:
        if linesRead >= 550000:
            break
        try:  # getting similar error to previous project where error tweets were logged
            decoded_line = i.decode('utf-8')  # Decode bytes to string
            tdict = json.loads(decoded_line)

            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = list(tweetInfo.values())
            cursor.execute("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", (tweetInsert[0], tweetInsert[1],
                                                                                tweetInsert[2], tweetInsert[3],
                                                                                tweetInsert[4], tweetInsert[5],
                                                                                tweetInsert[6],
                                                                                tweetInsert[7], tweetInsert[8],
                                                                                tweetInsert[9]))

            userInfo = {'id': tdict['user']['id'], 'name': tdict['user']['name'],
                        'screen_name': tdict['user']['screen_name'],
                        'description': tdict['user']['description'],
                        'friend_count': tdict['user']['friends_count']}
            if tdict['user']['id'] in userIDs:
                # don't want to put duplicate user into the list multiple times as ID will be primary key
                pass
            elif tdict['user']['id'] is None:
                pass
            else:
                userIDs.append(tdict['user']['id'])
                userInsert = list(userInfo.values())
                cursor.execute("INSERT INTO UserTweets VALUES(?,?,?,?,?);", (userInsert[0], userInsert[1],
                                                                             userInsert[2], userInsert[3],
                                                                             userInsert[4]))
            if tdict['geo'] is not None:
                geoInfo = {"created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                           "type": tdict['geo']['type'], "longitude": tdict['geo']['coordinates'][0],
                    "latitude": tdict['geo']['coordinates'][1]}
                geoInsert = list(geoInfo.values())
                cursor.execute("INSERT INTO Geo VALUES(?,?,?,?,?);", (geoInsert[0], geoInsert[1],
                                                                      geoInsert[2], geoInsert[3], geoInsert[4]))
            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

    tweetCount = cursor.execute("SELECT COUNT(DISTINCT id_str) FROM Tweets2;").fetchall()
    userCount = cursor.execute("SELECT COUNT(DISTINCT id) FROM UserTweets;").fetchall()
    geoCount = cursor.execute("SELECT COUNT(DISTINCT longitude) FROM Geo;").fetchall()


    webFD.close()
    endTime = time.time()
    errorTweets.close()
    oneB550 = endTime - startTime
    print('Total Tweets:', tweetCount)
    print('Total Users:', userCount)
    print('Total Geos:', geoCount)
    print("Total time to complete 1b) ", endTime - startTime)

    print('------------------------------------------------------------------')
    return oneB550


# Part 1c)

def part1C_110():
    """This function will read and populate, line by line, from the .txt file created earlier. """
    print('Part 1 C w/ 110k Tweets: ')

    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w') # .txt file to log errors

    cursor.execute(cmd)
    cursor.execute(tbl)
    cursor.execute(cmd2)
    cursor.execute(tbl2)
    cursor.execute(cmd3)
    cursor.execute(tbl3)

    userIDs = []

    start = time.time()
    linesRead = 0
    f = open('finalTweets.txt', 'r')
    for i in f:
        if linesRead >= 110000:
            break
        try:

            tdict = json.loads(i)
            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = list(tweetInfo.values())
            cursor.execute("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", (tweetInsert[0], tweetInsert[1],
                                                                                tweetInsert[2], tweetInsert[3],
                                                                                tweetInsert[4],
                                                                                tweetInsert[5], tweetInsert[6],
                                                                                tweetInsert[7], tweetInsert[8],
                                                                                tweetInsert[9]))

            userInfo = {'id': tdict['user']['id'], 'name': tdict['user']['name'],
                        'screen_name': tdict['user']['screen_name'],
                        'description': tdict['user']['description'],
                        'friend_count': tdict['user']['friends_count']}
            if tdict['user']['id'] in userIDs:
                # don't want to put duplicate user into the list multiple times as ID will be primary key
                pass
            elif tdict['user']['id'] is None:
                pass
            else:
                userIDs.append(tdict['user']['id'])
                userInsert = list(userInfo.values())
                cursor.execute("INSERT INTO UserTweets VALUES(?,?,?,?,?);", (userInsert[0], userInsert[1],
                                                                             userInsert[2], userInsert[3],
                                                                             userInsert[4]))
            if tdict['geo'] is not None:
                geoInfo = {"created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                           "type": tdict['geo']['type'], "coordinates": str(tdict['geo']['coordinates'])[1:-1]}
                geoInsert = list(geoInfo.values())
                cursor.execute("INSERT INTO Geo VALUES(?,?,?,?,?);", (geoInsert[0], geoInsert[1],
                                                                      geoInsert[2], geoInsert[3], geoInsert[4]))
            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

    end = time.time()
    f.close()
    errorTweets.close()
    print('Total Time to complete 1c 110,000:', end - start)

    totalTime = end - start
    print('------------------------------------------------------------------')
    return totalTime


# part1C_110()
def part1C_550():
    print('Part 1 C w/ 550k Tweets: ')

    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w') # .txt file to log errors

    cursor.execute(cmd)
    cursor.execute(tbl)
    cursor.execute(cmd2)
    cursor.execute(tbl2)
    cursor.execute(cmd3)
    cursor.execute(tbl3)

    userIDs = []

    start = time.time()
    linesRead = 0
    f = open('finalTweets.txt', 'r')
    for i in f:
        if linesRead >= 550000:
            break
        try:

            tdict = json.loads(i)
            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = list(tweetInfo.values())
            cursor.execute("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", (tweetInsert[0], tweetInsert[1],
                                                                                tweetInsert[2], tweetInsert[3],
                                                                                tweetInsert[4],
                                                                                tweetInsert[5], tweetInsert[6],
                                                                                tweetInsert[7], tweetInsert[8],
                                                                                tweetInsert[9]))

            userInfo = {'id': tdict['user']['id'], 'name': tdict['user']['name'],
                        'screen_name': tdict['user']['screen_name'],
                        'description': tdict['user']['description'],
                        'friend_count': tdict['user']['friends_count']}
            if tdict['user']['id'] in userIDs:
                # don't want to put duplicate user into the list multiple times as ID will be primary key
                pass
            elif tdict['user']['id'] is None:
                pass
            else:
                userIDs.append(tdict['user']['id'])
                userInsert = list(userInfo.values())
                cursor.execute("INSERT INTO UserTweets VALUES(?,?,?,?,?);", (userInsert[0], userInsert[1],
                                                                             userInsert[2], userInsert[3],
                                                                             userInsert[4]))
            if tdict['geo'] is not None:
                geoInfo = {"created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                           "type": tdict['geo']['type'], "longitude": tdict['geo']['coordinates'][0],
                           "latitude": tdict['geo']['coordinates'][1]}
                geoInsert = list(geoInfo.values())
                cursor.execute("INSERT INTO Geo VALUES(?,?,?,?,?);", (geoInsert[0], geoInsert[1],
                                                                      geoInsert[2], geoInsert[3], geoInsert[4]))
            linesRead += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

    end = time.time()

    print('Total Time to complete 1c 550,000:', end - start)
    f.close()
    errorTweets.close()
    totalTime = end - start
    print('------------------------------------------------------------------')
    return totalTime


# Part 1d)

def part1D_110():
    """This function will read batches from the .txt file and insert them as chunks rather than 1 line at a time
    This will also be timed."""
    print('Part 1d w/ 110k Tweets: ')

    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w') # .txt file to log errors

    cursor.execute(cmd)
    cursor.execute(tbl)
    cursor.execute(cmd2)
    cursor.execute(tbl2)
    cursor.execute(cmd3)
    cursor.execute(tbl3)

    userIDs = []
    batchSize = 0
    f = open('finalTweets.txt', 'r')

    tweet_values = set()
    user_values = set()
    geo_values = set()
    linesRead = 0

    start = time.time()

    for i in f:
        if linesRead >= 110000:
            break
        try:
            tdict = json.loads(i)

            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = tuple(tweetInfo.values())
            tweet_values.add(tweetInsert)

            userInfo = {
                'id': tdict['user']['id'], 'name': tdict['user']['name'],
                'screen_name': tdict['user']['screen_name'],
                'description': tdict['user']['description'],
                'friend_count': tdict['user']['friends_count']
            }
            if tdict['user']['id'] not in userIDs and tdict['user']['id'] is not None:
                userIDs.append(tdict['user']['id'])
                userInsert = tuple(userInfo.values())
                user_values.add(userInsert)

            if tdict['geo'] is not None:
                geoInfo = {
                    "created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                    "type": tdict['geo']['type'], "longitude": tdict['geo']['coordinates'][0],
                    "latitude": tdict['geo']['coordinates'][1]
                }
                geoInsert = tuple(geoInfo.values())
                geo_values.add(geoInsert)
            linesRead += 1
            batchSize += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

        if len(geo_values) == 250:
            geo_values = list(geo_values)
            cursor.executemany("INSERT INTO Geo VALUES(?,?,?,?,?);", geo_values)
            geo_values = set()

        if batchSize == 2000:
            tweet_values = list(tweet_values)
            user_values = list(user_values)
            # Insert batches into the database
            cursor.executemany("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", tweet_values)
            cursor.executemany("INSERT INTO UserTweets VALUES(?,?,?,?,?);", user_values)
            # Reset lists for next batch
            tweet_values = set()
            user_values = set()
            batchSize = 0

    # Insert any remaining records if left overs after the last full batch is inserted
    if tweet_values:
        tweet_values = list(tweet_values)
        cursor.executemany("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", tweet_values)
    if user_values:
        user_values = list(user_values)
        cursor.executemany("INSERT INTO UserTweets VALUES(?,?,?,?,?);", user_values)
    if geo_values:
        geo_values = list(geo_values)
        cursor.executemany("INSERT INTO Geo VALUES(?,?,?,?,?);", geo_values)

    end = time.time()

    print('Total Time to complete 1d 110,000:', end - start)
    f.close()
    errorTweets.close()
    totalTime = end - start
    print('------------------------------------------------------------------')
    return totalTime


def part1D_550():
    print('Part 1d w/ 550k Tweets: ')
    cursor.execute(cmd)
    cursor.execute(tbl)
    cursor.execute(cmd2)
    cursor.execute(tbl2)
    cursor.execute(cmd3)
    cursor.execute(tbl3)

    userIDs = []
    batchSize = 0
    f = open('finalTweets.txt', 'r')
    if os.path.isfile("errorTweets.txt"):
        os.remove("errorTweets.txt")
    errorTweets = open('errorTweets.txt', 'w') # .txt file to log errors

    tweet_values = set()
    user_values = set()
    geo_values = set()
    linesRead = 0

    start = time.time()

    for i in f:
        if linesRead >= 550000:
            break
        try:
            tdict = json.loads(i)

            tweetInfo = {"created_at": tdict["created_at"], "id_str": tdict["id_str"],
                         "user_id": tdict["user"]["id"], "text": tdict["text"],
                         "source": tdict["source"], "in_reply_to_user_id": tdict["in_reply_to_user_id"],
                         "in_reply_to_screen_name": tdict["in_reply_to_screen_name"],
                         "in_reply_to_status_id": tdict["in_reply_to_status_id"],
                         "retweet_count": tdict["retweet_count"], "contributors": tdict["contributors"]}
            tweetInsert = tuple(tweetInfo.values())
            tweet_values.add(tweetInsert)

            userInfo = {
                'id': tdict['user']['id'], 'name': tdict['user']['name'],
                'screen_name': tdict['user']['screen_name'],
                'description': tdict['user']['description'],
                'friend_count': tdict['user']['friends_count']
            }
            if tdict['user']['id'] not in userIDs and tdict['user']['id'] is not None:
                userIDs.append(tdict['user']['id'])
                userInsert = tuple(userInfo.values())
                user_values.add(userInsert)

            if tdict['geo'] is not None:
                geoInfo = {
                    "created_at": tdict["created_at"], "Userid": tdict["user"]["id"],
                    "type": tdict['geo']['type'], "longitude": tdict['geo']['coordinates'][0],
                    "latitude": tdict['geo']['coordinates'][1]
                }
                geoInsert = tuple(geoInfo.values())
                geo_values.add(geoInsert)
            linesRead += 1
            batchSize += 1
        except json.JSONDecodeError as e:
            errorTweets.write(f"JSON decoding error: {str(e)}\n")
            errorTweets.write(str(i) + '\n')
        except Exception as e:  # all other exceptions
            errorTweets.write(f"An unexpected error occurred: {str(e)}\n")
            errorTweets.write(str(i) + '\n')

        if len(geo_values) == 250:
            geo_values = list(geo_values)
            cursor.executemany("INSERT INTO Geo VALUES(?,?,?,?,?);", geo_values)
            geo_values = set()

        if batchSize == 2000:
            tweet_values = list(tweet_values)
            user_values = list(user_values)
            # Insert batches into the database
            cursor.executemany("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", tweet_values)
            cursor.executemany("INSERT INTO UserTweets VALUES(?,?,?,?,?);", user_values)
            # Reset lists for next batch
            tweet_values = set()
            user_values = set()
            batchSize = 0

    # Insert any remaining records if left overs after the last full batch is inserted
    if tweet_values:
        tweet_values = list(tweet_values)
        cursor.executemany("INSERT INTO Tweets2 VALUES(?,?,?,?,?,?,?,?,?,?);", tweet_values)
    if user_values:
        user_values = list(user_values)
        cursor.executemany("INSERT INTO UserTweets VALUES(?,?,?,?,?);", user_values)
    if geo_values:
        geo_values = list(geo_values)
        cursor.executemany("INSERT INTO Geo VALUES(?,?,?,?,?);", geo_values)

    end = time.time()

    print('Total Time to complete 1d 550,000:', end - start)
    f.close()
    errorTweets.close()
    totalTime = end - start
    print('------------------------------------------------------------------')
    return totalTime


def part1():
    # print('With 110k Tweets')
    # part1a_110()
    # part1B_110()
    # part1C_110()
    # part1D_110()
    print('-------------------------------------------------------------')
    print('-------------------------------------------------------------')
    # print('With 550k Tweets')

    # part1E:

    xaxis = ['Write File From Web', 'Gather/Insert From Web', 'Gather/Insert From Local', 'Batched Inserts']
    print('With 110k Tweets')
    y1 = [part1a_110(), part1B_110(), part1C_110(), part1D_110()]
    print('With 550k Tweets')
    y2 = [part1a_550(), part1B_550(), part1C_550(), part1D_550()]
    plt.plot(xaxis, y1, label='110,000 tweets')
    plt.plot(xaxis, y2, label='550,000 tweets')
    plt.title('Comparing RunTime of Tweet amounts and Batching Inserts')
    plt.xlabel('Problem')
    plt.xticks(rotation = 29)
    plt.ylabel('Seconds to complete task')
    plt.legend()
    plt.show()





########################################################################################################################
########################################################################################################################
# Part 2
"""Purpose of this section is to compare how long it takes to do the same processes in SQLand python"""
# 2a)
# sql command to select lon/lat and average the two
cmd2a = "SELECT longitude, latitude, (longitude + latitude) / 2 AS average_coordinates FROM Geo;"


# 2b)
def part2b():
    print('Part 2b: ')
    f = open('finalTweets.txt', 'r')
    individualRunTime = []

    start = time.time()
    for i in range(5):
        individualStart = time.time()
        cursor.execute(cmd2a).fetchall()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time1 = end - start

    start = time.time()
    for i in range(20):
        individualStart = time.time()
        cursor.execute(cmd2a).fetchall()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time2 = end - start

    print('The average individual runtime is: ', sum(individualRunTime) / len(individualRunTime))
    print('Time to run 5X: ', time1)
    print('Time to run 20X: ', time2)
    f.close()
    print('-------------------------------------------------------------------------------------------')
    return time1, time2
#part2b()

# part 2c)

def part2c():
    f = open('finalTweets.txt', 'r')
    f.seek(0)
    userCoord = []
    avgCoords = {}

    for i in f:
        try:
            tdict = json.loads(i)

            if tdict['geo'] is not None:
                geoInfo = {
                        "id_str": tdict["id_str"],
                        "longitude": tdict['geo']['coordinates'][0],
                        "latitude": tdict['geo']['coordinates'][1]
                    }
                userCoord.append(geoInfo)

        except:
            pass

    for i in userCoord:
        values = []
        x = (list(i.values()))
        for a in x[1:]:
            values.append(float(a))
        avgCoords[x[0]] = sum(values) / len(values)
    f.close()
    return avgCoords
#part2c()

def part2d():
    print('Part 2d: ')
    individualRunTime = []

    start = time.time()
    for i in range(5):
        individualStart = time.time()
        part2c()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time1 = end - start

    start = time.time()
    for i in range(20):
        individualStart = time.time()
        part2c()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time2 = end - start

    print('The average individual runtime is: ', sum(individualRunTime) / len(individualRunTime))
    print('Time to run 5X: ', time1)
    print('Time to run 20X: ', time2)
    print('------------------------------------------------------------------------------------------------------------------')

    return time1, time2
#part2d()

# part 2e)

def part2e():
    """This function will do the same as the previous but do so using regex to find the lon/lat from the .txt file"""
    f = open('finalTweets.txt', 'r')
    f.seek(0)
    geo_data = {} # empty dicitonary to contain key = str_id, value = average coordinate
    for i in f:
        i = i.strip()  # I originally entered every line with \n after which is causing issues
        if not i:  # after stripping if line is empty
            continue  # skip the line
        userReg = re.findall('user":\D"id":\d*\D"', i)
        userID = re.findall('\d+', str(userReg[0]))

        coordinate = re.findall('coordinates":\D-?\d*\D\d*\D-?\d*\D\d*\D', i)
        cord_reg = re.findall('-?\d+\D\d+,-?.*\d*?\d', str(coordinate[0]))


        listToAverage = []

        if len(cord_reg) > 0:
            for i in cord_reg:
                y = i.split(',')
                for a in y:
                    a = float(a)
                    listToAverage.append(a)
            finalCord = sum(listToAverage)/len(listToAverage)

            geo_data[userID[0]] = finalCord

#part2e()

# part 2f)

def part2f():
    print('Part 2f:')
    individualRunTime = []

    start = time.time()
    for i in range(5):
        individualStart = time.time()
        part2e()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time1 = end - start

    start = time.time()
    for i in range(20):
        individualStart = time.time()
        part2e()
        individualEnd = time.time()
        individualTime = individualEnd - individualStart
        individualRunTime.append(individualTime)

    end = time.time()
    time2 = end - start

    print('The average individual runtime is: ', sum(individualRunTime) / len(individualRunTime))
    print('Time to run 5X: ', time1)
    print('Time to run 20X: ', time2)
    print('---------------------------------------------------------------------------------------------')

#part2f()

def part2():
    part2b()
    x1, x2 = part2d()
    part2f()

    #part e)

    plt.bar('5 runs',x1, label = '5 Runs')
    plt.bar('20 runs', x2, label = '20 Runs')
    plt.xlabel('# of Runs')
    plt.ylabel('Minutes')
    plt.title('Comparing how long it takes to run 5 vs 20 times')
    plt.legend()

    plt.show()


########################################################################################################################
########################################################################################################################
# Part 3

def part3():
    """Create a pseudo materialized View in SQLite3, write contents to both json and csv files and compare sizes"""
    # create new materialized view using create table
    part3a = """CREATE TABLE MaterializedView AS SELECT 
        Tweets2.created_at,
        Tweets2.id_str,
        Tweets2.user_id,
        Tweets2.text,
        Tweets2.source,
        Tweets2.in_reply_to_user_id,
        Tweets2.in_reply_to_screen_name,
        Tweets2.in_reply_to_status_id,
        Tweets2.retweet_count,
        Tweets2.contributors,
        UserTweets.name,
        UserTweets.screen_name,
        UserTweets.description,
        UserTweets.friend_count,
        Geo.type,
        Geo.longitude,
        Geo.latitude
    FROM Tweets2
    LEFT JOIN UserTweets ON Tweets2.user_id = UserTweets.id
    LEFT JOIN Geo ON Tweets2.user_id = Geo.Userid AND Tweets2.created_at = Geo.created_at;
    """



    start = time.time()
    cursor.execute('DROP TABLE MaterializedView')
    cursor.execute(part3a)
    print('There are a total of: ',cursor.execute("SELECT COUNT(*) FROM MaterializedView").fetchall()[0][0], 'rows')
    end = time.time()
    print('It took',end-start, 'seconds to finish creating this table.')

    #part 3b)

    Tweets2 = cursor.execute("SELECT * FROM Tweets2;").fetchall()
    tweetJson = [] #initialize empty list to contain each row
    for i in Tweets2: # create dictionary with column name: value
        tdict = {
        'created_at': i[0],
        'id_str': i[1],
        'user_id': i[2],
        'text' : i[3],
        'source':i[4],
        'in_reply_to_user_id':i[5],
        'in_reply_to_screen_name':i[6],
        'in_reply_to_status_id':i[7],
        'retweet_count':i[8],
        'contributors':i[9]
        }
        tweetJson.append(tdict) # add each row as its own dictionary into list
    if os.path.isfile("tweets.json"): # check if file already exists
        os.remove("tweets.json")  # remove if it exists
    tweets = open('tweets.json', 'w') # create json file
    json.dump(tweetJson, tweets) # use json function dump() to put the contents of list into the json file
    tweets.close() # close file

    # the rest repeats the same pattern but later using csv

    MaterializedView = cursor.execute("SELECT * FROM MaterializedView;").fetchall()
    materializedJson = []
    for i in MaterializedView:
        tdict = {
            'Tweets2.created_at':i[0],
            'Tweets2.id_str':i[1],
            'Tweets2.user_id':i[2],
            'Tweets2.text':i[3],
            'Tweets2.source':i[4],
            'Tweets2.in_reply_to_user_id':i[5],
            'Tweets2.in_reply_to_screen_name':i[6],
            'Tweets2.in_reply_to_status_id':i[7],
            'Tweets2.retweet_count':i[8],
            'Tweets2.contributors':i[9],
            'UserTweets.name':i[10],
            'UserTweets.screen_name':i[11],
            'UserTweets.description':i[12],
            'UserTweets.friend_count':i[13],
            'Geo.type':i[14],
            'Geo.longitude':i[15],
            'Geo.latitude':i[16]
        }
        materializedJson.append(tdict)
    if os.path.isfile("materializedView.json"):
        os.remove("materializedView.json")
    mview = open('materializedView.json','w')
    json.dump(materializedJson, mview)
    mview.close()

    # part 3c)

    tweets2Columns = ['created_at', 'id_str','user_id','text','source',
        'in_reply_to_user_id','in_reply_to_screen_name','in_reply_to_status_id',
        'retweet_count', 'contributors']

    if os.path.isfile("tweetcsv.csv.json"):
        os.remove("tweetcsv.csv.json")
    tweetsCSV = open('tweetcsv.csv','w', encoding = 'utf-8')
    tweetsCSV.write(','.join(tweets2Columns)+'\n')

    for i in Tweets2:
        row = []
        for a in i:
            a = str(a)
            row.append(a)
        tweetsCSV.write(','.join(row) + '\n')
    tweetsCSV.close()

    mViewColumns = ['Tweets2.created_at',
            'Tweets2.id_str',
            'Tweets2.user_id',
            'Tweets2.text',
            'Tweets2.source',
            'Tweets2.in_reply_to_user_id',
            'Tweets2.in_reply_to_screen_name',
            'Tweets2.in_reply_to_status_id',
            'Tweets2.retweet_count',
            'Tweets2.contributors',
            'UserTweets.name',
            'UserTweets.screen_name',
            'UserTweets.description',
            'UserTweets.friend_count',
            'Geo.type',
            'Geo.longitude',
            'Geo.latitude']

    if os.path.isfile('mViewCSV.csv'):
        os.remove('mViewCSV.csv')
    mViewCSV = open('mViewCSV.csv', 'w', encoding = 'utf-8') # need to open in proper encoding to avoid errors with non default characters
    mViewCSV.write(','.join(mViewColumns) + '\n')

    for i in MaterializedView:
        row = []
        for a in i: # change contents of each row into string so they can be joined by ','
            a = str(a)
            row.append(a)
        mViewCSV.write(','.join(row) + '\n')
    mViewCSV.close()

part1()
part2()
part3()

conn.commit()
conn.close()
