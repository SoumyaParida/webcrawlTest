import csv
import re
import codecs
import sys

maxInt = sys.maxsize
decrement = True
from urlparse import urlparse

while decrement:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/10)
        decrement = True

#wordcount=set()

def getsecondleveldomain(url):
    with open("effective_tld_names.dat") as tld_file:
        tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
    url_elements = urlparse(url)[1].split('.')
    for i in range(-len(url_elements), 0):
        last_i_elements = url_elements[i:]
        candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
        wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
        exception_candidate = "!" + candidate
        # match tlds: 
        if (exception_candidate in tlds):
            return url_elements[i:][0]
        if (candidate in tlds or wildcard_candidate in tlds):
            return url_elements[i-1:][0]
def getsecondleveldomainValue(value):
    domain=value
    # for domain in urls :
    if domain.startswith('www.'):
        domain = domain.replace("www","")
    if domain.endswith('.'):
        domain=domain[:-1]
    if not domain.startswith('http://'):
        domain = 'http://%s' % domain
    secondlevelurl=str(getsecondleveldomain(domain))
    return (secondlevelurl)


# logFile = codecs.open("finalCheck.csv",'rU')
# outputReader = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL,dialect=csv.excel_tab)

# count=0
# iplist=list()

#++++++++++++++++++++++++++++++++++++++++++
#Find out Number of Urls
# for line in outputReader:
#     if len(line)==16:
#         count=count+1
# print count

#++++++++++++++++++++++++++++++++++++++++++
#Find out Number of Unique web servers
# count=0
# wordcount=set()
# for line in outputReader:
#     if len(line)==16:
#         if line[7] is not '-':
#         	urllist=line[7].split(';')
#         	for url in urllist:
# 	        	domain=getsecondleveldomainValue(url)
#                 if domain not in wordcount:
#                     wordcount.add(domain)
# 	        		wordcount[domain] = 1
# 	        	else:
# 	        		wordcount[domain] += 1
# 		# if len(line)==27:
# 		# 	wordcount.add(line[0])
# print "unique servers",len(wordcount)
# # sortedlist=sorted(wordcount.items(), key=lambda item: item[1])
# # print sortedlist[:50]
# # for k,v in wordcount.items():
# #     print k, v
# logFile.close()

import pygal
from pygal.style import Style
custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
line_chart.title = 'Top 20 CDNs in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 4504857)'
#line_chart.x_labels = ['akamai','akamai','akamai','google','facebook','amazon','incapdns','ourwebpic','google','cdn20','wscdns','edgecastcdn','fastly','ccgslb','tlgslb','akadns','netdna-cdn','amazon','cdngc']

line_chart.add('edgekey',  [191600, None, None, None, None, None, None, None,None, None])
line_chart.add('akamaiedge',  [None, 188903,   None,   None, None, None, None, None,None, None])
line_chart.add('edgesuite',  [None, None,   171631,   None, None, None, None, None,None, None])
line_chart.add('google',  [None, None, None,163286,None, None, None, None, None, None])
line_chart.add('akamai',  [None, None,   None,   None, 155886, None, None, None,None, None])
line_chart.add('facebook',  [ None, None, None,None, None, 72397, None, None, None, None])
line_chart.add('us-east-1',  [None, None, None, None, None, None, 62121, None,   None,   None])
line_chart.add('incapdns',  [None, None, None,None,None, None, None, 57841, None, None])
line_chart.add('ourwebpic',  [None, None, None,   None,  None,None, None, None, 53046, None])
line_chart.add('googleusercontent',  [None, None, None,None,None, None, None, None, None, 45583])
line_chart.add('cdn20',  [None, None, None,   None,   None, None, None, None,None,None, 41968])
line_chart.add('wscdns',  [None, None, None,   None,   None, None, None, None,None,None, None,40767])
line_chart.add('edgecastcdn',  [None, None, None,   None,   None, None, None, None,None,None, None,None,39784])
line_chart.add('fastly',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,33571])
line_chart.add('ccgslb',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,33452])
line_chart.add('tlgslb',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,None,32293])
line_chart.add('akadns',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,None,None,31361])
line_chart.add('netdna-cdn',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,None,None,None,31210])
line_chart.add('eu-west-1',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,None,None,None,None,28288])
line_chart.add('cdngc',  [None, None, None,   None,   None, None, None, None,None,None, None,None,None,None,None,None,None,None,None,24550])
#line_chart.render()
line_chart.render_to_png('/home/soumya/Documents/results_30th_april/top20CDN.png')

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for link type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 138171)'
# line_chart.x_labels = ['akamai','google','amazon','netdna-cdn','fastly','incapdns','edgecastcdn','cdngc','footprint','llnwd']
# line_chart.add('akamai',  [6600, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [6201, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [7115, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgekey',  [6282, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [1222, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('google',  [None, 16463, None,None,None, None, None, None, None, None])
# line_chart.add('us-east-1',  [ None, None, 1990,None, None, None, None, None, None, None])
# line_chart.add('eu-west-1',  [ None, None, 1228,None, None, None, None, None, None, None])
# line_chart.add('amazonaws',  [ None, None, 779, None, None,None, None, None, None, None])
# line_chart.add('netdna-cdn',  [None, None, None, 3603, None, None, None, None,   None,   None])
# line_chart.add('fastly',  [None, None, None,None,2122, None, None, None, None, None])
# line_chart.add('incapdns',  [None, None, None,   None,  None,1875, None, None, None, None])
# line_chart.add('edgecastcdn',  [None, None, None,   None,   None, None, 1824, None,None, None])
# line_chart.add('cdngc',  [None, None, None,   None,   None, None,None, 1035,None,None])
# line_chart.add('footprint',  [None, None, None,   None,   None, None, None,None,973,None])
# line_chart.add('llnwd',  [None, None, None,   None,   None, None, None,None,None,771])

# line_chart.render_to_png('result_graphs/top10CDN_link.png',style=custom_style)

#Anchor type of tag
# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04000','#e04000'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for anchor type of html object in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 1951449)'
# line_chart.x_labels = ['akamai','google','facebook','amazon','incapdns','ourwebpic','fc2','wscdns','cdn20','edgecastcdn']
# line_chart.add('edgekey',  [88479, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [88252, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [80709, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamai',  [66215, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [14908, None, None, None, None, None,None, None, None, None])

# #line
# line_chart.add('google',  [None, 77677, None,None,None, None, None, None, None, None])
# line_chart.add('googleusercontent',  [None, 24072, None,None,None, None, None, None, None, None])
# line_chart.add('facebook',  [None, None, 42696,None,None, None, None, None, None, None])

# line_chart.add('us-east-1',  [None, None, None,   28018, None, None, None, None, None, None])
# line_chart.add('eu-west-1',  [None, None, None,   14294, None, None, None, None, None, None])

# line_chart.add('incapdns',  [ None, None, None, None,26793, None, None, None, None, None])
# line_chart.add('ourwebpic', [None, None, None,   None, None, 21966, None, None, None, None])
# line_chart.add('fc2',  [None, None, None,   None,   None, None,15594, None, None, None])
# line_chart.add('wscdns',  [None, None, None,   None,   None, None, None,15481, None, None])
# line_chart.add('cdn20',  [None, None, None,   None,   None, None, None,None,13749,None])
# line_chart.add('edgecastcdn',  [None, None, None,   None,   None, None, None,None,None,12127])
# #line_chart.add('ccgslb',  [None, None, None,   None,   None, None, None,None,None,11443])

# line_chart.render_to_png('result_graphs/top10CDN_anchor.png',style=custom_style)

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for script type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 138043)'
# line_chart.x_labels = ['akamai','google','amazon','netdna-cdn','edgecastcdn','fastly','incapdns','cloudflare','cdngc','tbcache']
# line_chart.add('akamai',  [7653, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [6913, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [7770, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgekey',  [8517, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [1057, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('google',  [None, 6019, None,None,None, None, None, None, None, None])
# line_chart.add('us-east-1',  [None, None, 2254,None,   None,  None, None, None, None, None])
# line_chart.add('eu-west-1',  [None, None, 1235,None,   None,  None, None, None, None, None])
# line_chart.add('amazonaws',  [None, None, 1087, None,   None,  None,None, None, None, None])
# line_chart.add('netdna-cdn',  [ None, None, None,4376, None, None, None, None, None, None])
# #line_chart.add('eu-west-1',  [ None, None, 1228,None, None, None, None, None, None, None])
# #line_chart.add('amazonaws',  [ None, None, 779, None, None,None, None, None, None, None])
# line_chart.add('edgecastcdn',  [None, None, None,None, 2623, None, None, None,   None,   None])
# line_chart.add('fastly',  [None, None, None,None,None,2414, None, None, None, None])

# line_chart.add('incapdns',  [None, None, None,   None,   None, None, 1959, None,None, None])
# line_chart.add('cloudflare',  [None, None, None,   None,   None, None,None, 1316,None,None])
# line_chart.add('cdngc',  [None, None, None,   None,   None, None, None,None,1192,None])
# line_chart.add('tbcache',  [None, None, None,   None,   None, None, None,None,None,1017])

# line_chart.render_to_png('result_graphs/top10CDN_script.png',style=custom_style)

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for input type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 1574)'
# line_chart.x_labels = ['akamai','hitwebcounter','edgecastcdn','google','incapdns','netdna-cdn','cdngc','internet-filiale','footprint','amazon']
# line_chart.add('akamai',  [64, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [74, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [93, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgekey',  [138, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [36, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('hitwebcounter',  [None, 50, None,None,None, None, None, None, None, None])
# line_chart.add('edgecastcdn',  [None, None, 25,None,   None,  None, None, None, None, None])
# #line_chart.add('eu-west-1',  [None, None, 1235,None,   None,  None, None, None, None, None])
# #line_chart.add('amazonaws',  [None, None, 1087, None,   None,  None,None, None, None, None])
# line_chart.add('googleusercontent',  [ None, None, None,19, None, None, None, None, None, None])
# #line_chart.add('eu-west-1',  [ None, None, 1228,None, None, None, None, None, None, None])
# #line_chart.add('amazonaws',  [ None, None, 779, None, None,None, None, None, None, None])
# line_chart.add('incapdns',  [None, None, None,None, 19, None, None, None,   None,   None])
# line_chart.add('netdna-cdn',  [None, None, None,None,None,17, None, None, None, None])

# line_chart.add('cdngc',  [None, None, None,   None,   None, None, 15, None,None, None])
# line_chart.add('internet-filiale',  [None, None, None,   None,   None, None,None, 14,None,None])
# line_chart.add('footprint',  [None, None, None,   None,   None, None, None,None,12,None])
# line_chart.add('us-east-1',  [None, None, None,   None,   None, None, None,None,None,11])

# line_chart.render_to_png('result_graphs/top10CDN_input.png',style=custom_style)

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for iframe type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 19727)'
# line_chart.x_labels = ['google','facebook','doubleclick','edgecastcdn','thebrighttag','assoc-amazon','incapdns','edgesuite','twitter','weibo']
# line_chart.add('google',  [9191, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('facebook',  [None, 4940, None,None,None, None, None, None, None, None])
# line_chart.add('doubleclick',  [None, None, 513,None,   None,  None, None, None, None, None])
# line_chart.add('edgecastcdn',  [ None, None, None,335, None, None, None, None, None, None])
# line_chart.add('thebrighttag',  [None, None, None,None, 313, None, None, None,   None,   None])
# line_chart.add('assoc-amazon',  [None, None, None,None,None,220, None, None, None, None])
# line_chart.add('incapdns',  [None, None, None,   None,   None, None, 186, None,None, None])
# line_chart.add('edgesuite',  [None, None, None,   None,   None, None,None, 175,None,None])
# line_chart.add('twitter',  [None, None, None,   None,   None, None, None,None,159,None])
# line_chart.add('weibo',  [None, None, None,   None,   None, None, None,None,None,137])

# line_chart.render_to_png('result_graphs/top10CDN_iframe.png',style=custom_style)

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for form type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 15018)'
# line_chart.x_labels = ['akamai','amazon','incapdns','google','cloudflare','mailchimp','fastly','edgecastcdn','list-manage','yunjiasu-cdn']
# line_chart.add('akamai',  [564, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [560, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [928, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgekey',  [1069, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [171, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('us-east-1',  [None, 366, None,None,None, None, None, None, None, None])
# line_chart.add('eu-west-1',  [None, 175, None,None,None, None, None, None, None, None])
# line_chart.add('incapdns',  [None, None, 260,None,   None,  None, None, None, None, None])
# #line_chart.add('eu-west-1',  [None, None, 1235,None,   None,  None, None, None, None, None])
# #line_chart.add('amazonaws',  [None, None, 1087, None,   None,  None,None, None, None, None])
# line_chart.add('google',  [ None, None, None,251, None, None, None, None, None, None])
# #line_chart.add('eu-west-1',  [ None, None, 1228,None, None, None, None, None, None, None])
# #line_chart.add('amazonaws',  [eu-west-1 None, None, 779, None, None,None, None, None, None, None])
# line_chart.add('cloudflare',  [None, None, None,None, 228, None, None, None,   None,   None])
# line_chart.add('mailchimp',  [None, None, None,None,None,224, None, None, None, None])

# line_chart.add('fastly',  [None, None, None,   None,   None, None, 118, None,None, None])
# line_chart.add('edgecastcdn',  [None, None, None,   None,   None, None,None, 102,None,None])
# line_chart.add('list-manage',  [None, None, None,   None,   None, None, None,None,75,None])
# line_chart.add('yunjiasu-cdn',  [None, None, None,   None,   None, None, None,None,None,71])

# line_chart.render_to_png('result_graphs/top10CDN_form.png',style=custom_style)

# import pygal
# from pygal.style import Style
# custom_style = Style(value_font_size=8,colors=('orange', 'purple', 'blue', '#18453b', 'red','brown','green','aqua','pink','gold','#ee82ee','#c8a2c8','#32cd32','#c04000','gray','#d04231','#e04123','#12353b'))
# line_chart = pygal.StackedBar(y_title='number of urls-->',x_title='cdns-->',legend_at_bottom=True,legend_box_size=18,truncate_legend=17)
# line_chart.title = 'Top 10 CDNs for form type of tag in the alexa 100,000 websites' + '\n' + '(sample size (no. of urls fetched) = 19727)'
# line_chart.x_labels = ['akamai','amazon','incapdns','google','cloudflare','mailchimp','fastly','edgecastcdn','list-manage','yunjiasu-cdn']
# line_chart.add('akamai',  [564, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgesuite',  [560, None, None, None, None, None, None, None,None, None])
# line_chart.add('akamaiedge',  [928, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('edgekey',  [1069, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('akadns',  [171, None,   None,   None, None, None, None, None,None, None])
# line_chart.add('us-east-1',  [None, 366, None,None,None, None, None, None, None, None])
# line_chart.add('eu-west-1',  [None, 175, None,None,None, None, None, None, None, None])
# line_chart.add('incapdns',  [None, None, 260,None,   None,  None, None, None, None, None])
# #line_chart.add('eu-west-1',  [None, None, 1235,None,   None,  None, None, None, None, None])
# #line_chart.add('amazonaws',  [None, None, 1087, None,   None,  None,None, None, None, None])
# line_chart.add('google',  [ None, None, None,251, None, None, None, None, None, None])
# #line_chart.add('eu-west-1',  [ None, None, 1228,None, None, None, None, None, None, None])
# #line_chart.add('amazonaws',  [eu-west-1 None, None, 779, None, None,None, None, None, None, None])
# line_chart.add('cloudflare',  [None, None, None,None, 228, None, None, None,   None,   None])
# line_chart.add('mailchimp',  [None, None, None,None,None,224, None, None, None, None])

# line_chart.add('fastly',  [None, None, None,   None,   None, None, 118, None,None, None])
# line_chart.add('edgecastcdn',  [None, None, None,   None,   None, None,None, 102,None,None])
# line_chart.add('list-manage',  [None, None, None,   None,   None, None, None,None,75,None])
# line_chart.add('yunjiasu-cdn',  [None, None, None,   None,   None, None, None,None,None,71])

# line_chart.render_to_png('result_graphs/top10CDN_form.png',style=custom_style)