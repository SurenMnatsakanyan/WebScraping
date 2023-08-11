from datetime import datetime

import requests
import mysql.connector
from bs4 import BeautifulSoup

# List of tuples containing domain and Google Play URL
app_list = []
try:
    with open('gplay_urls.txt', 'r') as file:
        # Skip the first line (header)
        next(file)

        for line in file:
            try:
                parts = line.split()  # Split by spaces
                domain = parts[0].strip()
                url = parts[1].strip()
                app_list.append((domain, url))
            except IndexError:
                print(f"Error processing line: {line.strip()}")
except FileNotFoundError:
    print("File not found")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="googlePlay"
)

cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS app_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        domain VARCHAR(255),
        average_rating FLOAT,
        num_reviews BIGINT,
        num_installations BIGINT,
        rating_of_app VARCHAR(255),
        update_date_of_app Date
    )
''')


# Function to extract data from Google Play page
def extract_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    date_text = soup.find('div', class_='xg1aie').text.strip()
    date_object = datetime.strptime(date_text, '%b %d, %Y')
    divs = soup.findAll('div', {'class': 'wVqUob'})
    average_rating = divs[0].find('div', {'class': 'TT9eCd'}).text
    split_rating = average_rating.split("s")[0]
    num_rating = float(split_rating)
    num_reviews = divs[0].find('div', {'class': 'g1rdde'}).text
    indexForReviews = num_reviews.find("reviews")
    num_reviews = num_reviews[:indexForReviews]
    num_reviews = convertFromStringToNumber(num_reviews)
    num_installations = None
    for div in divs:
        inner_div = div.find('div', {'class': 'g1rdde'})
        if 'Downloads' in inner_div.text:
            num_downloads_div = div.find('div', {'class': 'ClM7O'})
            num_installations = num_downloads_div.text
    divOfRating = divs[2].find('div', {'class': 'g1rdde'})
    spanOfDiv = divOfRating.find('span', itemprop='contentRating').text
    indexForInstallation = num_installations.find("+")
    num_installations = num_installations[:indexForInstallation]
    num_installations = convertFromStringToNumber(num_installations)
    return num_rating, num_reviews, num_installations, spanOfDiv, date_object


def convertFromStringToNumber(sampleString):
    if 'K' in sampleString:
        sampleString = float(sampleString.split('K')[0])
        sampleString = int(1000 * sampleString)
    elif 'M' in sampleString:
        sampleString = float(sampleString.split('M')[0])
        sampleString = int(1000000 * sampleString)
    elif 'B' in sampleString:
        sampleString = float(sampleString.split('B')[0])
        sampleString = int(1000000000 * sampleString)
    else:
        sampleString = int(sampleString)
    return sampleString


# Collect and store data
for domain, url in app_list:
    average_rating, num_reviews, num_installations, rating_of_app, update_date_of_app = extract_data(url)
    cursor.execute('''
            INSERT INTO app_data (domain, average_rating, num_reviews, num_installations, rating_of_app,update_date_of_app)
            VALUES (%s, %s, %s, %s, %s,%s)
        ''', (domain, average_rating, num_reviews,num_installations,rating_of_app,update_date_of_app))
    conn.commit()

# Close the connection
conn.close()
