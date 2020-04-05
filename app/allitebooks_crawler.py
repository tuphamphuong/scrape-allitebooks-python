#! /usr/bin/env python3.4
import requests
import os
import sys
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import lxml
import pandas as pd
import psycopg2
import traceback
import uuid
import configparser

# create logger with 'crawl_allitebooks'
logger = logging.getLogger('crawl_allitebooks')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('crawl_allitebooks.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

config = configparser.ConfigParser()
config.read("app/conf.ini")
logger.info("config sections %s", config.sections())
# Exit if config not found
if len(config.sections()) == 0:
    sys.exit()

book_pages_path = "data/book_pages.txt"
book_sites_path = "data/book_sites.txt"
connection = None
cursor = None


def init_db_connection():
    try:
        global connection
        connection = psycopg2.connect(user=config["postgres"]["user"],
                                      password=config["postgres"]["password"],
                                      host=config["postgres"]["host"],
                                      port=config["postgres"]["port"],
                                      database=config["postgres"]["database"])
        connection.autocommit = True

        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        cursor.close()
        logger.info("You are connected to %s with prarams %s", record, connection.get_dsn_parameters())
    except Exception as e:
        logger.warning(str(e))
        traceback.print_exc()
        # Exit if db init has error
        sys.exit()


def get_request(url):
    """
	using requests to replace urllib.requests.urlopen
	return an html
	"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome"}
    r = requests.get(url, headers=headers)
    return r.text


def generate_pages(from_page, to_page, sub_title):
    """
    generate list of page based on indexes
	return page sites url list
	"""
    pages = []
    if 0 < from_page < to_page:
        for i in range(from_page, to_page + 1):
            pages.append('http://www.allitebooks.com' + sub_title + '/page/' + str(i))
    logger.debug("pages: %s", pages)

    # save to file
    book_pages_file = open(book_pages_path, 'w')
    for page in pages:
        book_pages_file.write(page + "\n")

    return pages


def crawl_pages(pages):
    try:
        book_sites = []

        # TODO: Apply worker to make it faster
        for page in pages:
            book_site_of_one_page = get_book_sites_of_one_page(page)
            book_sites.extend(book_site_of_one_page)

        book_sites_file = open(book_sites_path, 'w')
        for book_site in book_sites:
            book_sites_file.write(book_site + "\n")

        return book_sites
    except Exception as e:
        logger.warning("Error on %s with message %s", str(e))
        traceback.print_exc()


def get_book_sites_of_one_page(url):
    """
	get book site's url in one page
	input: page site url
	output: book site urls list
	"""
    try:
        html = get_request(url)
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find('main').findAll('a', {'rel': 'bookmark'})
        book_sites = []
        for link in links[::2]:
            if 'href' in link.attrs:
                book_sites.append(link.attrs['href'])
        logger.debug("book_sites: %s", book_sites)
    except Exception as e:
        logger.warning(str(e))
        traceback.print_exc()
    return book_sites


def crawl_book(book_site):
    """
	input a book site
	find book detail in this book site
	return them as a list
	"""
    try:
        html = get_request(book_site)
        soup = BeautifulSoup(html, 'lxml')

        article_box = soup.find("article", attrs={"class": "single-post"})

        entry_header_box = article_box.find("header", attrs={"class": "entry-header"})

        logger.debug("book_site %s", book_site)

        title = entry_header_box.find("h1", attrs={"class": "single-title"}).text
        logger.debug("title %s", title)

        short_description = ""
        if entry_header_box.find("h4"):
            short_description = entry_header_box.find("h4").text
            # remove empty lines
            short_description = os.linesep.join([s for s in short_description.splitlines() if s])
        logger.debug("short_description %s", short_description)

        description = article_box.find("div", attrs={"class": "entry-content"}).text
        # remove empty lines
        description = os.linesep.join([s for s in description.splitlines() if s])
        logger.debug("description %s", description)

        image_url = entry_header_box.find("img", attrs={"class": "attachment-post-thumbnail"})["src"]
        logger.debug("image_url %s", image_url)

        detail_box = entry_header_box.find("div", attrs={"class": "book-detail"})
        dt_data = detail_box.find_all("dt")
        dl_data = detail_box.find_all("dd")

        # Clear html in detail
        for i in range(len(dt_data)):
            dt_data[i] = dt_data[i].text
        for i in range(len(dl_data)):
            dl_data[i] = dl_data[i].text

        # Convert to Pandas data frame
        detail_df = pd.DataFrame()
        detail_df["meta_name"] = dt_data
        detail_df["meta_value"] = dl_data
        # logger.debug("detail_df.to_dict(orient='records' %s", detail_df.to_dict(orient='records'))

        # Parse meta data
        author_name = ""
        isbn_10 = ""
        publication_year = 0
        pages = 0
        language = ""
        file_size = ""
        file_format = ""
        category_name = ""
        for dict in detail_df.to_dict(orient='records'):
            meta_name_value = dict["meta_name"]
            meta_value_value = dict["meta_value"]
            # logger.debug("dict %s name %s value %s", dict, meta_name_value, meta_value_value)
            if meta_name_value == "Author:":
                author_name = meta_value_value
                logger.debug("author_name %s", author_name)
            elif meta_name_value == "ISBN-10:":
                isbn_10 = meta_value_value
                logger.debug("isbn_10 %s", isbn_10)
            elif meta_name_value == "Year:":
                publication_year = int(meta_value_value.strip())
                logger.debug("publication_year %d", )
            elif meta_name_value == "Pages:":
                pages = int(meta_value_value.strip())
                logger.debug("pages %d", pages)
            elif meta_name_value == "Language:":
                language = meta_value_value
                logger.debug("language %s", language)
            elif meta_name_value == "File size:":
                file_size = meta_value_value
                logger.debug("file_size %s", file_size)
            elif meta_name_value == "File format:":
                file_format = meta_value_value
                logger.debug("file_format %s", file_format)
            elif meta_name_value == "Category:":
                category_name = meta_value_value
                logger.debug("category_name %s", category_name)
        logger.debug(
            "author_name %s isbn_10 %s publication_year %d pages %d language %s file_size %s file_format %s category_name %s",
            author_name, isbn_10, publication_year, pages, language, file_size, file_format, category_name)

        download_urls = []
        download_url_pdf = ""
        download_url_epub = ""
        download_url_mobi = ""
        download_data = article_box.findAll("span", attrs={"class": "download-links"})
        for download_item in download_data:
            a_element = download_item.find("a", attrs={"target": "_blank"})
            download_url = a_element.attrs['href']
            download_urls.append(download_url)
            if ".pdf" in download_url:
                download_url_pdf = download_url
            elif ".epub" in download_url:
                download_url_epub = download_url
            elif ".mobi" in download_url:
                download_url_mobi = download_url
        logger.debug("download_url_pdf %s download_url_epub %s download_url_mobi %s", download_url_pdf,
                     download_url_epub, download_url_mobi)

        # Insert to database
        sql = """INSERT INTO books(id, title, author_name, isbn_10, publication_year, pages, language, file_size, file_format, category_name, short_description, description, img_url, download_url_pdf, download_url_epub, download_url_mobi, source, created) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s);"""
        logger.info("sql %s", sql)
        # Allitebooks source = 1
        source = 1
        record_to_insert = (
            str(uuid.uuid4()), str(title), str(author_name), str(isbn_10), str(publication_year), str(pages),
            str(language),
            str(file_size), str(file_format), str(category_name), str(short_description), str(description),
            str(image_url),
            str(download_url_pdf), str(download_url_epub), str(download_url_mobi), source, datetime.now())

        global connection
        cursor = connection.cursor()
        cursor.execute(sql, record_to_insert)
        cursor.close()
        connection.commit()
    except Exception as e:
        logger.warning(str(e))
        traceback.print_exc()


def main():
    try:
        logger.info("Number of arguments: %d", len(sys.argv))

        # default args
        from_page = 1
        to_page = 2
        # to_page = 852
        sub_title = ""
        step = ""

        if len(sys.argv) == 1:
            # For debug crawl_book purpose
            crawl_book("http://www.allitebooks.org/data-structures-and-algorithms-in-swift/")
        if len(sys.argv) == 2:
            step = sys.argv[2]
        if len(sys.argv) == 4:
            step = sys.argv[2]
            from_page = sys.argv[3]
            to_page = sys.argv[4]
        if len(sys.argv) == 5:
            step = sys.argv[2]
            from_page = sys.argv[3]
            to_page = sys.argv[4]
            sub_title = sys.argv[5]
            if sub_title == "0":
                sub_title = ""

        if step == "generate-pages":
            logger.info("Jump to step generate-pages")
            generate_pages(from_page, to_page, sub_title)
        elif step == "crawl_pages":
            logger.info("Jump to step crawl_pages")
            try:
                book_pages_file = open(book_pages_path, 'r')
                pages = book_pages_file.readlines()
                crawl_pages(pages)
            except Exception as e:
                logger.warning("Error on %s with message %s", str(e))
                traceback.print_exc()
        elif step == "crawl_books":
            logger.info("Jump to step crawl_books")
            try:
                # TODO: Apply worker to make it faster
                book_sites_file = open(book_sites_path, 'r')
                book_sites = book_sites_file.readlines()
                for book_site in book_sites:
                    crawl_book(book_site)
                    logger.info("Crawl success with url: %s", book_site)
            except Exception as e:
                logger.warning("Error on %s with message %s", str(e))
                traceback.print_exc()
        elif step == "download_resources":
            logger.info("Jump to step download_resources")

        logger.info("Program Finished")
    except Exception as e:
        logger.warning(str(e))
        traceback.print_exc()


if __name__ == '__main__':
    init_db_connection()
    main()
