"""Search movies for "Monty Python" and output the results.

Note: You should edit APIKEY to yours or you may be banned by service!
"""

import douban.service

# Please use your own api key instead. e.g. :
APIKEY = '076763036d48e6d01636290a47dab470'
SECRET = ''

client = douban.service.DoubanService(api_key=APIKEY)
feed = client.SearchMovie("Monty Python")
for movie in feed.entry:
    print "%s: %s" % (movie.title.text, movie.GetAlternateLink().href)
