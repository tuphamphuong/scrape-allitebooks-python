# scrape-allitebooks-python
A deadly simple python crawler to scrap all data of books from www.allitebooks.com then save the metadata to PostgreSQL and books to default directory 

## requirements
```
pip install -r requirements.txt
```

## How to use it? 
Run the command
```
python3 allitebooks_crawler.py <step> 
```

## Anything special?
```
This program using JobLib: https://joblib.readthedocs.io/en/latest/ to do simple & resonable parallel computing for learning purposes
```   

By default, it will scrape all pages of allitebooks.com from page 1 to the limit
 
  