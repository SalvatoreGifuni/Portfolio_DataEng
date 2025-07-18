"""
Project Main Info: 
BookSmart Solutions S.r.l. wants to improve its market analysis by collecting structured data from the books of a competing online bookstore. 
The goal is to develop a web scraping script that extracts key information such as title, star rating, price and availability of books from https://books.toscrape.com. 
The data will then be organised in a table format and saved in a CSV file. 
This project will offer benefits such as advanced competitive analysis, optimisation of product offerings and integration with internal systems. 
Exceptions will be handled to ensure the robustness of the script.
"""

# Modules

from requests import get, RequestException, HTTPError
from bs4 import BeautifulSoup as BS
from urllib.parse import urljoin
from pandas import DataFrame as pd
import time
import logging
import random
from typing import Optional

# Functions

def create_log_file() -> None:
    """
    Create a log file if it doesn't exist.
    """
    try:
        # Create a file handler
        handler = logging.FileHandler('log_file.log', mode='w')
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # Add the handler to the root logger
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    except Exception as e:
        print(f"Error setting up logging: {e}")

def fetch_url_with_retry(url: str, retries: int = 3, delay: int = 5) -> Optional[str]:
    """
    Fetch the content of a URL with retries.

    Args:
        url (str): The URL to fetch.
        retries (int): Number of retries.
        delay (int): Delay between retries in seconds.

    Returns:
        Optional[str]: The content of the URL if successful, None otherwise.
    """
    for attempt in range(retries):
        try:
            response = get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except HTTPError as http_err:
            if response.status_code == 404:
                logging.error(f"HTTP error 404: Not Found for URL: {url}")
                break
            elif response.status_code in [500, 502, 503, 504]:
                logging.error(f"HTTP error {response.status_code}: Server error for URL: {url}")
            else:
                logging.error(f"HTTP error occurred: {http_err}")
                break
        except RequestException as req_err:
            logging.error(f"Attempt {attempt + 1} failed with error: {req_err}")
        time.sleep(delay * (2 ** attempt))  # Exponential backoff
    logging.error(f"All attempts to fetch {url} have failed.")
    return None

def clean_data(title: str, rating: str, price: str, stock: str) -> tuple[str, str, float, str]:
    """
    Clean the extracted data.

    Args:
        title (str): Book title.
        rating (str): Book rating.
        price (str): Book price.
        stock (str): Book stock status.

    Returns:
        tuple[str, str, float, str]: Cleaned data.
    """
    try:
        # Remove unwanted characters from price and convert to float
        price = float(price.replace('Â£', '').replace('£', ''))
    except ValueError as e:
        logging.error(f"Error converting price '{price}' to float: {e}")
        price = float('nan')

    rating = rating.capitalize()
    stock = stock.strip()

    return title, rating, price, stock

def extract_book_info(book: BS) -> tuple[str, str, float, str]:
    """
    Extract book information from a BeautifulSoup tag.

    Args:
        book: BeautifulSoup tag containing book information.

    Returns:
        tuple[str, str, float, str]: Extracted book information (title, rating, price, stock).
    """
    title = book.h3.a['title']
    rating = book.p['class'][1]
    price = book.find('p', class_='price_color').text
    stock = book.find('p', class_='instock availability').text.strip()

    # Clean data
    return clean_data(title, rating, price, stock)

def validate_data(title: str, rating: str, price: float, stock: str) -> bool:
    """
    Validate the extracted book data.

    Args:
        title (str): Book title.
        rating (str): Book rating.
        price (float): Book price.
        stock (str): Book stock status.

    Returns:
        bool: True if data is valid, False otherwise.
    """
    # Validate title
    if not isinstance(title, str) or not title.strip():
        return False

    # Validate rating
    valid_ratings = {"One", "Two", "Three", "Four", "Five"}
    if not isinstance(rating, str) or rating not in valid_ratings:
        return False

    # Validate price
    if not isinstance(price, float) or price < 0:
        return False

    # Validate stock
    valid_stock_statuses = {"In stock", "Out of stock"}
    if not isinstance(stock, str) or stock not in valid_stock_statuses:
        return False

    return True

def get_max_pages_from_first_page(soup: BS) -> int:
    """
    Get the maximum number of pages from the first page's HTML soup.

    Args:
        soup (BS): The BeautifulSoup object of the first page.

    Returns:
        int: The maximum number of pages.
    """
    max_page_element = soup.find('li', class_='current')
    if max_page_element:
        max_page_text = max_page_element.text.strip()
        try:
            total_pages = int(max_page_text.split()[-1])
            return total_pages
        except (ValueError, IndexError) as e:
            logging.error(f"Error parsing the total number of pages: {e}")
            return 1
    else:
        logging.warning("Couldn't find the total number of pages. Defaulting to 1.")
        return 1

def scrape_page(page_url: str, extract_max_pages: bool = False) -> tuple[list[list[str]], Optional[int]]:
    """
    Scrape a single page for book information.

    Args:
        page_url (str): The URL of the page to scrape.
        extract_max_pages (bool): Whether to extract the maximum number of pages from the page.

    Returns:
        tuple[list[list[str]], Optional[int]]: A list of books information and optionally the maximum number of pages.
    """
    books = []
    max_pages = None
    page_content = fetch_url_with_retry(page_url)
    if page_content is None:
        logging.error(f"Failed to fetch page {page_url} after multiple retries.")
        return books, max_pages

    try:
        # Parse the page content using BeautifulSoup
        soup = BS(page_content, 'html.parser')
    except Exception as e:
        logging.error(f"Error parsing the content of page {page_url}: {e}")
        return books, max_pages

    # Find all book elements on the page
    books_element: list[BS] = soup.find_all("article", class_="product_pod")

    for book in books_element:
        # Extract book information
        title, rating, price, stock = extract_book_info(book)

        # Validate the extracted data
        if validate_data(title, rating, price, stock):
            # Append the valid book data to the list
            books.append([title, rating, price, stock])
        else:
            logging.warning(f"Invalid data found for book: {title}")

    # Optionally extract the maximum number of pages
    if extract_max_pages:
        max_pages = get_max_pages_from_first_page(soup)

    logging.info(f"Page {page_url} processed successfully.")
    return books, max_pages

def number_pages(total_pages: int) -> int:
    """
    Get user input for the number of pages to scrape.

    Returns:
        int: The number of pages to scrape.
    """
    while True:
        try:
            choice_input = int(input(f"How many pages do you want to scrape? Enter the number of pages or '0' for all pages (up to {total_pages}): "))
            if choice_input < 0:
                logging.warning("Please enter a positive integer or '0'.")
                continue
            choice = choice_input if 0 < choice_input <= total_pages else total_pages
            return choice
        except ValueError:
            logging.warning("Invalid input. Please enter a valid integer.")

def create_dataframe(data: list[list[str]]) -> pd:
    """
    Create a pandas DataFrame from the list of book data.

    Args:
        data (list[list[str]]): List of book data.

    Returns:
        pd: DataFrame containing the book data.
    """
    columns = ["Title", "Rating", "Price [£]", "Stock"]
    return pd(data, columns=columns)

def choose_type_file(df: pd):
    """
    Choose the type of file to export the data and save it.

    Args:
        df (pd): DataFrame containing the book data.
    """
    while True:
        file_type = input("Choose file type to export (csv, json): ").strip().lower()
        if file_type == "csv":
            df.to_csv("books.csv", index=False, encoding="utf-8")
            logging.info("Data exported to books.csv")
            break
        elif file_type == "json":
            df.to_json("books.json", index=False, orient='records', lines=True, force_ascii=False)
            logging.info("Data exported to books.json")
            break
        else:
            logging.warning("Invalid file type. Please choose 'csv' or 'json'.")

def get_rate_limiting_parameters() -> tuple[float, float]:
    """
    Ask the user for rate-limiting parameters (min_delay and max_delay).

    Returns:
        tuple[float, float]: The minimum and maximum delay between requests in seconds.
    """
    while True:
        try:
            min_delay = float(input("Enter the minimum delay between requests (seconds): "))
            max_delay = float(input("Enter the maximum delay between requests (seconds): "))
            if min_delay < 0 or max_delay < 0:
                logging.warning("Please enter positive integers for delays.")
                continue
            if min_delay > max_delay:
                logging.warning("Minimum delay should not be greater than maximum delay.")
                continue
            return min_delay, max_delay
        except ValueError:
            logging.warning("Invalid input. Please enter valid integers for delays.")

def dynamic_sleep(min_delay: float = 1, max_delay: float = 3):
    """
    Sleep for a random duration between min_delay and max_delay seconds.

    Args:
        min_delay (float): Minimum delay in seconds.
        max_delay (float): Maximum delay in seconds.
    """
    sleep_time = random.uniform(min_delay, max_delay)
    logging.info(f"Sleeping for {sleep_time:.2f} seconds to respect rate limits.")
    time.sleep(sleep_time) #rate limiting

def identify_issues(df: pd) -> tuple[pd, pd]:
    """
    Identify duplicates and rows with missing values in the DataFrame.

    Args:
        df (pd): The DataFrame to check.

    Returns:
        tuple[pd, pd]: DataFrames of duplicates and rows with missing values.
    """
    duplicates = df[df.duplicated(subset=['Title'], keep=False)]
    missing_values = df[df.isnull().any(axis=1)]
    return duplicates, missing_values

def write_issues_to_file(duplicates: pd, missing_values: pd, filename: str):
    """
    Write duplicates and rows with missing values to a CSV file.

    Args:
        duplicates (pd): DataFrame of duplicates.
        missing_values (pd): DataFrame of rows with missing values.
        filename (str): The name of the CSV file to write the issues to.
    """
    with open(filename, 'w', encoding='utf-8') as f:
        if not duplicates.empty:
            f.write("Duplicates:\n")
            duplicates.to_csv(f, index=False)
            f.write("\n")
            logging.info(f"Duplicates found and written to {filename}. Please, verify.")
        else:
            f.write("No duplicates found.\n\n")

        if not missing_values.empty:
            f.write("Missing values:\n")
            missing_values.to_csv(f, index=False)
            f.write("\n")
            logging.info(f"Missing values found and written to {filename}. Please, verify.")
        else:
            f.write("No missing values found.\n")

def remove_duplicates_and_missing(df: pd) -> pd:
    """
    Remove duplicates and rows with missing values from the DataFrame.

    Args:
        df (pd): The DataFrame to clean.

    Returns:
        pd: The cleaned DataFrame.
    """
    df.drop_duplicates(subset=['Title'], inplace=True)
    df.dropna(inplace=True)
    return df

def main_code():
    """
    Main function to run the data extraction process.
    """
    create_log_file()
    base_url: str = "https://books.toscrape.com/catalogue/"
    books_list: list[list[str]] = []

    logging.info("Starting data extraction process")

    # Get the first page and determine the maximum number of pages
    first_page_url = urljoin(base_url, "page-1.html")
    initial_books, total_pages = scrape_page(first_page_url, extract_max_pages=True)
    books_list.extend(initial_books)

    # If total_pages is None, default to 1
    if total_pages is None:
        total_pages = 1

    # Get the number of pages to scrape from the user
    pages_to_scrape = number_pages(total_pages)

    # Get rate-limiting parameters from the user
    min_delay, max_delay = get_rate_limiting_parameters()

    # Iterate through remaining pages
    for i in range(2, pages_to_scrape + 1):
        page: str = f"page-{i}.html"
        url: str = urljoin(base_url, page)

        # Fetch and scrape the page
        books_list.extend(scrape_page(url)[0])

        # Implement dynamic sleep to respect rate limits
        dynamic_sleep(min_delay=min_delay, max_delay=max_delay)

    # Create DataFrame from collected data
    books_df = create_dataframe(books_list)

    # Identify duplicates and missing values
    duplicates_df, missing_values_df = identify_issues(books_df)

    # Write duplicates and missing values to a file
    issues_filename = "data_issues.csv"
    write_issues_to_file(duplicates_df, missing_values_df, issues_filename)

    # Remove duplicates and rows with missing values
    books_df = remove_duplicates_and_missing(books_df)

    # Exporting cleaned data
    choose_type_file(books_df)

    logging.info("Data extraction process completed")

# Execute the main function when the script is run directly
if __name__ == "__main__":
    main_code()
