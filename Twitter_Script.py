import tweepy
from tweepy import OAuthHandler
import pandas as pd
import os
import datetime
import time


#Please input your own Key and secret
consumer_key = 'xxxxxxxxxxxx'
consumer_secret = 'xxxxxxx'
access_token = 'xxxxxxxxx'
access_secret = 'xxxxxxxx'


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)
#to check your connection
user = api.me()
print('Name: ' + user.name + ' '+ 'Location: ' + user.location +'Friends: ' + str(user.friends_count))


#change location at all respective document locations
#Accounts List and applying a for loop in function
#Lets start with two Accounts
Twitter_Accounts_List=["narendramodi","PutinRF_Eng"]
outtweets=[] #declaring a blank list
start=time.time()
if os.path.exists('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv'): #replace with your desired location
   os.remove('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv')
   print('\033[1m' + " Deleted last Interm pull" + '\033[0m')
else:
   print('\033[1m' + "On your way to your own Twitter Data" + '\033[0m')





for screen_name in Twitter_Accounts_List:
   try:
       alltweets =[]
       #make initial request for most recent tweets (200 is the maximum allowed count)
       new_tweets = api.user_timeline(screen_name = screen_name,count=200)
        #save most recent tweets
       alltweets.extend(new_tweets)
        #save the id of the oldest tweet less one
       oldest = alltweets[-1].id - 1
           #keep pulling the tweets until there are no tweets left to pull
       while len(new_tweets) > 0:
           print ("getting tweets before %s" % (oldest))
           #all further requests use the max_id param to prevent duplicates
           new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
           #save most recent tweets
           alltweets.extend(new_tweets)
               #update the id of the oldest tweet less one
           oldest = alltweets[-1].id - 1
           print ("...%s tweets downloaded so far" % (len(alltweets)))
           #transform the tweepy tweets into a 2D array that will populate the csv
       outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8"),
                         tweet.favorite_count, tweet.retweet_count,tweet.user.screen_name
                         ,tweet.user.followers_count]
                          for tweet in alltweets]
       data_tod = pd.DataFrame(outtweets)
       data_tod.columns=["ID", "Date","Tweet","Favorite","Retweet","Account_Name","Account_Followers"]
       cycle =Twitter_Accounts_List.index(screen_name) +1
       if os.path.exists('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv'): #for appending data for all the accounts
           data_tod.to_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv', mode='a',header=False)
       else:
           data_tod.to_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv',header=True)
       print (" Pull Account:%s Completed!:-)"%cycle ) 
   except : Exception
   pass
end = time.time()
t=round((end - start)/60,1)
print (" Total Time taken in the Pull: %s mins" % (t))



#UPDATE- APPEND PROCESS
if os.path.exists('C:\\Users\\Manu Sharma\\Documents\\Twitter\\master.csv'):
   print ('\033[1m' +"This File Already exists, Time for some update append!" + '\033[0m')
   master_data = pd.read_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\master.csv',index_col=0)# for no unnamed column
   #Update on matched IDs between Master and interim Data
   data_com=pd.read_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv',index_col=0)
   data_com['ID'] = data_com['ID'].astype('int64')  # bringing ID as same datatype
   master_data['ID'] = master_data['ID'].astype('int64')
   data_com['modified_date'] = datetime.date.today() # modified date to know last update date
   master_data = master_data.merge(data_com, on='ID', how='left')
   master_data['Favorite'] = master_data['Favorite_y'].fillna(master_data['Favorite_x'])
   master_data['Retweet'] = master_data['Retweet_y'].fillna(master_data['Retweet_x'])
   master_data['modified_date'] = master_data['modified_date_y'].fillna(master_data['modified_date_x'])
   master_data = master_data.rename(columns={'Date_x': 'Date', 'Tweet_x': 'Tweet', 'Account_Name_x': 'Account_Name',
                                 'Account_Followers_x': 'Account_Followers'})
   # Dropping not required columns
   master_data = master_data.drop(
       ['Date_y', 'Tweet_y', 'Account_Name_y', 'Account_Followers_y', 'Favorite_y', 'Favorite_x', 'Retweet_x',
        'Retweet_y','modified_date_x','modified_date_y'], axis=1)
   match=data_com[data_com['ID'].isin(master_data['ID'])].dropna(how='all')
   match_len=len(match)
   print( '\033[1m' + "%s Tweets are Updated in the Master data" % (match_len) +'\033[0m')
   #Append on unmatched IDs between Master and interim Data
   data_unmatch = data_com[~data_com['ID'].isin(master_data['ID'])].dropna(how='all')
   master_data=data_unmatch.append(master_data)
   data_unmatch_len=len(data_unmatch)
   print ('\033[1m' + "%s Tweets are appended in the Master data"%(data_unmatch_len) +'\033[0m')
   master_data = master_data[
       ['Account_Name', 'Account_Followers', 'Date','ID','Tweet', 'Favorite', 'Retweet', 'modified_date']]
   master_data.to_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\master.csv', header=True)
   print (u'UPDATE-APPEND COMPLETED \U0001f604')
else: # for First time activity
   data_com = pd.read_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\interim.csv',index_col=0)
   data_com['modified_date'] = datetime.date.today()
   data_com.to_csv('C:\\Users\\Manu Sharma\\Documents\\Twitter\\master.csv', header=True)
   print (u'NEW MASTER FILE SAVED \U0001f604' )
