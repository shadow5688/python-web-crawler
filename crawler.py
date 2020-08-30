import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from cfg import root_url, mongo_url, database_name, collection_name


# Creating Database on MongoDB
try:
    print("Connecting to MongoDB client...\n")
    client = MongoClient(mongo_url)
    print("Connected to MongoDB Client\n")
    print("Creating new Databse with name {}\n".format(database_name))
    new_db = client[database_name]
    table = new_db[collection_name]
    print("Successfully created new database named {}\n".format(database_name))
except:
    print("Failed to connect to MongoDB Client\n")



# Updates database fields
def update_db(link_id, is_crawled, last_crawl_date, response_status, content_type, content_length, file_path):
    query = {"_id" : link_id}
    data = {"$set" : 
        {
            'Is Crawled' : is_crawled,
            'Last Crawl Dt' : last_crawl_date,
            'Response Status' : response_status,
            'Content type' : content_type,
            'Content length' : content_length,
            'File path' : file_path
        }    
}
    table.update_one(query, data)

# Crawls page using beautifulsoup and returns a list of links
def crawl_link(url):
    found_links_list = []
    try:
        response = requests.get(url)
        html = BeautifulSoup(response.content, 'html.parser')
        for a_link in html.find_all('a'):
            if "href" in a_link.attrs:
                link = (a_link.attrs["href"])
                if link.startswith("/"):
                    link = url.rstrip("/") + link
                if link.startswith("http"):
                    found_links_list.append((link))
    except:
        print("Failed to crawl {}".format(url))
    return found_links_list

# Uploads links on Database
def upload_link(link, source_link):
    data = {
        'Link' : link,
        'Source Link' : source_link,
        'Created at' : datetime.datetime.now()
    }  
    temp = table.insert_one(data)
    link_id = temp.inserted_id
    return link_id

# Returns response status, content type, content lenght, etc of any url
def get_link_info(url):
    response = requests.get(url)
    response_status = response.status_code
    crawl_date = datetime.datetime.now()

    try:
        content_type = response.headers['Content-Type']
    except:
        content_type = None

    try:
        content_length = response.headers.get('Content-Length', len(response.content))
    except:
        content_length = None

    if url == root_url:
        is_crawled = False
    else:
        is_crawled = True
    return (response.content, response_status, crawl_date, content_type, content_length, is_crawled)


'''
Overview:
    Search for pending links, initially just root url will be pending link
    Get oldest pending link
    Get that link details and content using requests
    Update that link info to database (initially just link and source link were there, after updating content type, length, etc will be added)
    (Initially it was pending link after update its no longer pending)
    Crawl that link if its html and return list of all links found
    Add new links found to database (just link and source link)(called as pending links in code)
    If any new link found is already in crawled then check last crawl date
    If its more than 24 hours old, update details in database and replace old stored file with new one
    else skip that link 
    Again take oldest pending link

    if total link count (including pending and completed) > 5000 then stop crawling and just complete pending ones
    if there is no pending one left then print "All links crawled"
    if completed links >= 5000 print "Maximum limit reached"
    then sleep for 5 seconds
'''

def update_and_crawl(oldest_pending_link_details):
    link_id = oldest_pending_link_details['_id']
    oldest_pending_link = oldest_pending_link_details['Link']
    print("Getting and uploading info of {}".format(oldest_pending_link))
    # Getting other info of the link
    (content, response_status, crawl_date, content_type, content_length, is_crawled) = get_link_info(oldest_pending_link)
    # Getting extension from content type
    extension = (content_type.split(";")[0].split("/")[-1]).strip()
    file_path = "./files/{}.{}".format(link_id, extension)
        
    # Saving content to files folder in current directory
    with open(file_path, "wb") as f:
        f.write(content)
        f.close()

    # Updating link detail in database
    update_db(link_id, is_crawled, crawl_date, response_status, content_type, content_length, file_path)

    # Crawling page if content type is html and if there are not more then 5000 links in database
    if extension == 'html' and table.count() <= 5000:
        #print("Crawling {}\n".format(oldest_pending_link))
        # Calling crawl_link function which crawls page and returns list of all valid links
        list_of_link_found = crawl_link(oldest_pending_link)
        # iterating through list of links
        for link in list_of_link_found:
            # checking if link is already in database or not
            if not table.find_one({'Link' : link}):
                upload_link(link, oldest_pending_link)
            elif table.find_one({'Link' : link}) and table.find_one({'Link' : link}).get("Last Crawl Dt", False):
                # getting last crawl date and time
                last_date = table.find_one({'Link' : link})["Last Crawl Dt"]
                # Checking if link was crawled before 24 hrs or not
                if datetime.datetime.now() - last_date > datetime.timedelta(hours=24):
                    #print("Crawled before 24hrs, updating details of {}".format(link))
                    updt_link_id = table.find_one({'Link' : link})["_id"]
                    updt_file_path = table.find_one({'Link' : link})["File path"]
                    (updt_content, updt_response_status, updt_crawl_date, updt_content_type, updt_content_length, updt_is_crawled) = get_link_info(link)
                    update_db(updt_link_id, updt_is_crawled, updt_crawl_date, updt_response_status, updt_content_type, updt_content_length, updt_file_path)
                    with open(updt_file_path, "wb") as updt_f:
                        updt_f.write(updt_content)
                        updt_f.close()
                else:
                    #print("Crawled within 24hrs, skipping {}".format(link))
                    pass
            else:
                #print("Already added {}".format(link))
                pass
def main():
    url = root_url
    source_link = None
    upload_link(url, source_link)

    while True:
        # total_crawled_links are those links which have all their info added to database and are crawled
        total_crawled_links = table.find({'Is Crawled': {'$exists': True }}).count()
        # pending_links are those links which are crawled from another link
        # they have only link and source link saved in database

        # Those link which doesn't have 'Is Crawled' in their data are called as pending links in this code
        pending_links_count = table.find({'Is Crawled': {'$exists': False }}).count()
        print("Total crawled: {}".format(total_crawled_links))
        print("Pending: {}".format(pending_links_count))

        # Making query for one link that doesn't contain 'Is Crawled'
        # By default it will be the oldest one
        
        oldest_five_pending_link_details = table.find({'Is Crawled': {'$exists': False }}).limit(5)
        executor = ThreadPoolExecutor(max_workers=5)
        for oldest_pending_link_details in oldest_five_pending_link_details:
            executor.submit(update_and_crawl, oldest_pending_link_details)
        executor.shutdown()

        ##################################################
        # Checking if total links in database are not more than 5000
        if total_crawled_links >= 5000:
            print("Maximum Limit Reached")
            break
        # Checking if there are still pending links or not
        if pending_links_count == 0:
            print("All links crawled")
            break
        print("Completed Cycle\n")
        time.sleep(5)


if __name__ == "__main__":
    main()