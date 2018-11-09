# LastFm Spark Stats

Scala/Spark application designed to calculate stats out of a Last.fm dataset (http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html).
The application can be run via command line by passing two arguments:

* Spark Data Format: "DF" or "RDD"
* Question Number: 1, 2 or 3

The questions the app can answer are:

1) Create a list of user IDs, along with the number of distinct songs each user has played.

2) Create a list of the 100 most popular songs (artist and title) in the dataset, with the number of times each was played.

3) Create a list of the top 10 longest sessions, with the following information about each session of 20 minutes: userid, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).
