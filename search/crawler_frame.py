import re, os
from time import time
import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from bs4 import BeautifulSoup  # added
import requests
from collections import Counter
import urllib2
import urllib
import urlparse
from urlparse import urljoin
import pickle
import robotparser
from datetime import datetime

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "13179240"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Undergrad 13179240"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        download_time_count = open("download_time_counts.txt", "a+")
        download_time_count.write(str((len(url_count) / (time() - self.starttime))) + '\n')
        download_time_count.close()
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):

    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    # 1. get [(link, raw_data)...up to five links)
    # 2. parse this raw data
    # 3. <a>
    # 4. make it absolute something/something - relative url -> www.facebook.com/something/something
    # 5. put it into list(urls)
    outputLinks = list()
    base_urls_file = open('base_urls.txt', 'a+')
    subdomain_counts_file = open("subdomain_counts.txt", "a+")
    outlinks_counts_file = open("outlinks_counts.txt", "a+")
    urls_marked_bad_file = open('urls_marked_bad.txt', "a+")
    soup_test_file = open("soup_test.txt", "w+")
    urls_added_to_frontier_file = open('urls_added_to_frontier.txt', "a+")


    try:
        links_file = open('links.txt', 'a+')
        for x in rawDatas:
            write_raw_content_obj_to_file(x)  # for testing
            base_url = x.url  # the page we are currently "visiting"
            base_url = base_url.encode('ascii', 'ignore')  # was crashing with non-ascii chars

            if check_if_url_good(base_url) and is_valid(base_url) and check_raw_obj_good(x):
                base_urls_file.write(base_url + '\n')  # Prints the same thing as successful URLs
                r = requests.get(base_url)
                soup = BeautifulSoup(r.content)
                soup.prettify()
                soup_test_file.write(str(soup))
                num_outlinks = 0

                for tag in soup.findAll('a', href=True):  # find all links on the base url's page
                    url = urljoin(base_url, tag['href'])
                    url = url.encode('ascii', 'ignore')  # was crashing with non-ascii chars
                    if check_if_url_good(url) and is_valid(url):
                        links_file.write(url + '\n')
                        url_parsed = urlparse(url)
                        if url_parsed.hostname is not None:  # bad urls messing it up
                            subdomain = url_parsed.hostname
                            subdomain_counts_file.write(str(subdomain) + '\n')

                        outputLinks.append(url)
                        outlinks_counts_file.write(str(base_url) + '\n')
                        urls_added_to_frontier_file.write(str(url) + '\n')
            else:
                write_num_invalid_links_to_file()
                urls_marked_bad_file.write(str(x.url) + '\n')
                x.bad_url = True

    except:
        write_num_invalid_links_to_file()

    # close the files
    base_urls_file.close()
    subdomain_counts_file.close()
    outlinks_counts_file.close()
    urls_marked_bad_file.close()
    soup_test_file.close()
    urls_added_to_frontier_file.close()
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    # print 'in is_valid() url gotten: ' + url

    url = url.encode('ascii', 'ignore')  # was crashing with non-ascii chars TEST JUST ADDED

    print 'in IS_VALID FUNCTION URL = ' + str(url)
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        write_num_invalid_links_to_file()
        return False


    if len(url) > 100:
        return False

    if is_url_blocked(url) == True:
        return False
    try:


        #boolean_return = ".ics.uci.edu" in parsed.hostname \ TOOK THIS OUT AND ADDED BELOW LINE PER ED's SUGGESTION
        boolean_return = re.search("\.ics\.uci\.edu\.?$", parsed.hostname) \
                         and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                          + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                          + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                          + "|thmx|mso|arff|rtf|jar|csv" \
                                          + "|rm|smil|wmv|swf|wma|zip|rar|gz|h5)$", parsed.path.lower())

        if boolean_return == False:  # keep count of invalid links
            write_num_invalid_links_to_file()

        return boolean_return
    except TypeError:


def write_num_invalid_links_to_file():
    with open('num_invalid_links.txt') as f:
        last = f.readline().strip()
    f.close()
    curr_invalid_count = int(last) + 1
    num_invalid_links_file = open('num_invalid_links.txt', 'w+')
    num_invalid_links_file.write(str(curr_invalid_count) + '\n')

def check_if_url_good(url):
    if len(url) > 100:
        return False
    if is_url_blocked(url) == True:
        return False

    return True

def is_url_blocked(url):
    if 'hall_of_fame/' in url:  # hardcoding probably not the best way
        return True

    if '~mlearn/datasets/' in url:
        return True
    if 'machine-learning-databases' in url and '?' in url:
        return True

    #http: // archive.ics.uci.edu / ml / machine - learning - databases / madelon?C = D;
    #O = A < Links like that, a couple of them slipped through

    if 'archive.ics.uci.edu/ml/datasets.html' in url and '?' in url:
        return True

    if 'ganglia' in url:
        return True
    if 'calendar' in url:
        return True

    if '/grad/resources' in url:
        return True

    if 'mailto:' in url:
        return True


    if 'DataGuard' in url or 'dataguard' in url or 'dataGuard' in url:
        return True

    # to filter out garbage URLS like
    # http://www.ics.uci.edu/about/about_contact.php/mailto:ucounsel@uci.edu/search/search_sao.php/about_mission.php/
    # !!!!!!!!!!!!!!!!!!!though this may filter out valid URLS as well !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if '.php/' in url:
        return True

    #http://www.ics.uci.edu/prospective/en/contact/contact/student-affairs
    if 'contact/student-affairs' in url:
        return True

    if 'leadership_council' in url:
        return True

    if '/~minhaenl/' in url and '?' in url:
        return True

    if '?C=' in url:
        return True

    if '/..' in url:
        return True

    return False



def write_subdomain_analytics(subdomain_counts_file):
    subdomain_analytics_file = open('subdomain_analytics.txt', 'w+')  # for part 1 of analytics
    token_list = []

    subdomain_counts_file = open("subdomain_counts.txt", "r+")

    lines = subdomain_counts_file.readlines()
    for line in lines:
       if line != '':
         token_list.append(line.lower().strip())
    subdomain_counts_file.close()
    subdomain_frequencies = Counter()

    for token in token_list:
        subdomain_frequencies[token] += 1

    for key, value in subdomain_frequencies.most_common():
        subdomain_analytics_file.write(key + ": " + str(value) + "\n")

    subdomain_analytics_file.close()
    subdomain_counts_file.close()

def write_outlinks_analytics(outlinks_counts_file):
    outlinks_analytics_file = open('outlinks_analytics.txt', 'w+')  # for part 3 of analyics
    outlinks_counts_file = open("outlinks_counts.txt", "r+")

    token_list = []
    lines = outlinks_counts_file.readlines()

    for line in lines:
        if line != '':
            token_list.append(line.lower().strip())

    outlinks_counts_file.close()
    outlinks_frequencies = Counter()

    for token in token_list:
        outlinks_frequencies[token] += 1

    for key, value in outlinks_frequencies.most_common():
        outlinks_analytics_file.write(key + ": " + str(value) + "\n")

    outlinks_analytics_file.close()


def write_raw_content_obj_to_file(x):
    test_file = open("test_file.txt", "a+")
    test_file.write('x.url: ' + str(x.url) + '\n')
    test_file.write('x.content: ' + str(x.content) + '\n')
    test_file.write('x.error_message: ' + str(x.error_message) + '\n')
    test_file.write('x.headers: ' + str(x.headers) + '\n')
    test_file.write('x.http_code: ' + str(x.http_code) + '\n')
    test_file.write('x.is_redirected: ' + str(x.is_redirected) + '\n')
    test_file.write('x.fi nal_url: ' + str(x.final_url) + '\n')
    test_file.write('x.bad_url: ' + str(x.bad_url) + '\n')
    test_file.write('x.out_links: ' + str(x.out_links) + '\n')
    test_file.write('\n')
    test_file.close()

def check_raw_obj_good(x):
    bool_ret = True

    #if x.is_redirected: DOES NOT NECESSARILY MEAN BAD URL. GOT URLS THAT WERE GOOD & REDIRECTED
     #   print('X REDIRECTED!' + '\n')
     #  bool_ret = False

    if x.http_code != 200 and x.http_code != 499: 
        bool_ret = False

    if x.error_message.strip() == 'Not Found':
        bool_ret = False

    return bool_ret
