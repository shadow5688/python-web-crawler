import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
import datetime
from cfg import root_url, mongo_client_config
from concurrent.futures import ThreadPoolExecutor

try:
    print("Creating MongoDB Database")
    client = MongoClient(mongo_client_config)
    new_db = client.crawler_database
    table = new_db.crawled_links
    print("Successfully created database")
except:
    print("Failed to create database")

links_to_be_crawled = {(root_url, root_url)}
crawled = []

def store_db(link, source_link, is_crawled, last_crawl_date, response_status, content_type, content_length, file_path, created_at):
    data = {
        'Link' : link,
        'Source Link' : source_link,
        'Is Crawled' : is_crawled,
        'Last Crawl Dt' : last_crawl_date,
        'Response Status' : response_status,
        'Content type' : content_type,
        'Content length' : content_length,
        'File path' : file_path,
        'Created at' : created_at
}
    inserted_table = table.insert_one(data)
    return inserted_table.inserted_id

def get_links(url):
    response = requests.get(url)
    html = BeautifulSoup(response.content, 'html.parser')
    for a_link in html.find_all('a'):
        if "href" in a_link.attrs:
            link = (a_link.attrs["href"])
            if link.startswith("/"):
                link = url.rstrip("/") + link
            if link.startswith("http"):
                #print(link)
                links_to_be_crawled.add((link, url))

def get_link_info(url):
    response = requests.get(url)
    response_status = response.status_code
    crawl_date = response.headers['Date']
    content_type = response.headers['Content-Type']
    try:
        content_length = response.headers['Content-Length']
    except:
        content_length = len(response.content)
    if url == root_url:
        is_crawled = False
    else:
        is_crawled = True

    return (response.content, crawl_date, response_status, content_type, content_length, is_crawled)



def main():
    url = root_url
    source_link = root_url
    while True:
        print("crawling {}".format(url, source_link))
        try:
            if (url, source_link) in crawled:
                print("Already crawled")
                print("Skipping {}".format(url))
            else:
                if len(links_to_be_crawled) + len(crawled) <= 5000:
                    get_links(url)
                else:
                    print("Already have enough links")
                (content, crawl_date, response_status, content_type, content_length, is_crawled) = get_link_info(url)
                created_at = datetime.datetime.now()
                file_path = './files/'
                print("Storing {}".format(url))
                db_id = store_db(url, source_link, is_crawled, crawl_date, response_status, content_type, content_length, file_path, created_at)
                file_name = './files/{}.html'.format(db_id)
                with open(file_name, 'wb') as f:
                    f.write(content)
                    f.close()
                crawled.append((url, source_link))
        except:
            print("Failed to crawl {}".format(url))
        links_to_be_crawled.remove((url, source_link))
        print("Links crawled: {}".format(len(crawled)))
        print("Links in queue: {}".format(len(links_to_be_crawled)))
        (url, source_link) = list(links_to_be_crawled)[0]
        if len(crawled) == 5000:
            print("Maximum limit reached”")
            break
        if len(links_to_be_crawled) == 0:
            print("All links crawled")
            break
        time.sleep(5)

        
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(main(), range(5))