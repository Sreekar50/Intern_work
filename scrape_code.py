import os
import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://www.food.com/topic/"
OUTPUT_FILE = "front_data.csv"
SKIPPED_FILE = "Skipped_links.txt"
SKIPPED_CAT = "skip_temp_cat.txt"

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--enable-automation")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.binary_location = "/home/deepak21319/chrome/opt/google/chrome/google-chrome"
chromedriver_path = "/home/deepak21319/chromedriver/chromedriver-linux64/chromedriver"

def start_driver():
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

driver = start_driver()

def ensure_driver():
    global driver
    try:
        driver.title  # Accessing driver to check if it's still active
    except Exception:
        print("[WARNING] WebDriver closed unexpectedly. Restarting...")
        driver.quit()
        driver = start_driver()
        
# Load existing recipes
if os.path.exists(OUTPUT_FILE):
    existing_df = pd.read_csv(OUTPUT_FILE)
    extracted_recipes = set(existing_df["Recipe URL"])
else:
    extracted_recipes = set()

if os.path.exists(SKIPPED_FILE):
    with open(SKIPPED_FILE, "r") as f:
        skipped_links = set(f.read().splitlines())
else:
    skipped_links = set()
    
if os.path.exists(SKIPPED_CAT):
    with open(SKIPPED_CAT, "r") as f:
        skipped_cats = set(f.read().splitlines())
else:
    skipped_cats = set()


def get_page_source(url, retries=3):
    global driver
    full_url = urljoin(BASE_URL, url)
    if full_url in skipped_links:
        print(f"⏭️ Skipping previously failed URL: {full_url}")
        return None
    for attempt in range(retries):
        try:
            driver.get(full_url)
            time.sleep(2)
            return driver.execute_script("return document.documentElement.outerHTML")
        except Exception as e:
            print(f"⚠️ Timeout error loading {full_url}: {e} (Retry {attempt+1}/{retries})")
            if attempt == retries - 1:
                print(f"❌ Skipping {full_url} after 3 failed attempts.")
                with open(SKIPPED_FILE, "a") as f:
                    f.write(full_url + "\n")
                skipped_links.add(full_url)
    return None


def get_links(selector, soup):
    return [urljoin(BASE_URL, a["href"]) for a in soup.select(selector) if "href" in a.attrs]

def scroll_until_no_new_recipes():
    while True:
        try:
            # Click Load More if available
            load_more_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.gk-tile-footer button"))
            )
            driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
            load_more_button.click()
            time.sleep(3)
        except Exception:
            break  # Exit loop if no Load More button is found
    
    # Scroll down until no new recipes load
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Allow time for new recipes to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break  # Stop when no new content is loaded
        last_height = new_height
  
        
def scrape_recipe(recipe_url,category_name):
    if recipe_url in extracted_recipes or recipe_url in skipped_links:
        print(f"skipping recipe: {recipe_url}")
        return None
    print(f"Scraping recipe: {recipe_url}")
    page_source = get_page_source(recipe_url)
    if not page_source:
        print(f"Skipping {recipe_url} due to failed retrieval.")
        return None
    soup = BeautifulSoup(page_source, "html.parser")
    extracted_recipes.add(recipe_url)
    try:
        title = soup.select_one("h1.svelte-1muv3s8")
        ready_in = soup.select_one("dt.facts__label:contains('Ready In:') + dd")
        serves = soup.select_one("dt.facts__label:contains('Serves:') + dd")
        yield_quantity = soup.select_one("dt.facts__label:contains('Yields:') + dd span.value")
        yield_unit = yield_quantity.find_next_sibling(text=True).strip() if yield_quantity else ""
        yield_value = f"{yield_quantity.text.strip()} {yield_unit}" if yield_quantity else "NA"
        ingredients = " ; ".join([
            f"{qty.text.strip()} {name.text.strip()}"
            for qty, name in zip(soup.select("span.ingredient-quantity"), soup.select("span.ingredient-text"))
        ])
        directions = " ".join([d.text.strip() for d in soup.select("ul.direction-list li.direction")])
        nutrition_script = soup.find("script", type="application/ld+json")
        nutrition_data = {}
        if nutrition_script:
            try:
                json_data = json.loads(nutrition_script.string)
                if "nutrition" in json_data:
                    nutrition_info = json_data["nutrition"]
                    nutrition_data = {
                        "Calories": nutrition_info.get("calories", "NA"),
                        "Fat": nutrition_info.get("fatContent", "NA"),
                        "Saturated Fat": nutrition_info.get("saturatedFatContent", "NA"),
                        "Cholesterol": nutrition_info.get("cholesterolContent", "NA"),
                        "Sodium": nutrition_info.get("sodiumContent", "NA"),
                        "Carbohydrates": nutrition_info.get("carbohydrateContent", "NA"),
                        "Fiber": nutrition_info.get("fiberContent", "NA"),
                        "Sugar": nutrition_info.get("sugarContent", "NA"),
                        "Protein": nutrition_info.get("proteinContent", "NA"),
                    }
            except json.JSONDecodeError:
                print("Error parsing JSON nutrition data")
        
        recipe_data = {
            "Recipe URL": recipe_url,
            "Title": title.text.strip() if title else "NA",
            "Category": category_name,
            "Ready In": ready_in.text.strip() if ready_in else "NA",
            "Serves": serves.text.strip() if serves else "NA",
            "Yield": yield_value,
            "Ingredients": ingredients if ingredients else "NA",
            "Directions": directions if directions else "NA",
        }
        
        recipe_data.update(nutrition_data)
        return recipe_data
    except Exception as e:
        print(f"Error scraping {recipe_url}: {e}")
        return None


def scrape_recipes_from_section(section_soup,category_name):
    recipes = []
    for link in get_links(".tile-content h2 a", section_soup):
        if "/recipe/" in link:
            recipe_data = scrape_recipe(link,category_name)
            if recipe_data:
                recipes.append(recipe_data)
        elif "/ideas/" in link:
            group_soup = BeautifulSoup(get_page_source(link), "html.parser")
            for sub_recipe in get_links(".smart-info h2 a", group_soup):
                if "/recipe/" in sub_recipe:
                    recipe_data = scrape_recipe(sub_recipe,category_name)
                    if recipe_data:
                        recipes.append(recipe_data)
    return recipes


def scrape_foodcom():
    try:
        all_recipes = []
        for letter in [BASE_URL + c for c in "abc"]:
            print(f"scraping alphabet: {letter}")
            category_soup = BeautifulSoup(get_page_source(letter), "html.parser")
            for category in get_links(".content-col-list a", category_soup):
                if category in skipped_cats:
                    print(f"skipping category: {category}")
                    continue
                category_name = category.split("/")[-1].replace("-", " ").title()
                section_soup = BeautifulSoup(get_page_source(category), "html.parser")
                for section in get_links(".tile-filters li a", section_soup):
                    ensure_driver()
                    driver.get(section)
                    time.sleep(2)
                    scroll_until_no_new_recipes()
                    section_soup = BeautifulSoup(driver.page_source, "html.parser")
                    all_recipes.extend(scrape_recipes_from_section(section_soup,category_name))
                    if len(all_recipes) >= 20:
                        pd.DataFrame(all_recipes).to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False)
                        all_recipes = []
                print(f"completed category: {category}")
                with open(SKIPPED_CAT, "a") as file:
                    file.write(category + "\n")
                skipped_cats.add(category)
            print(f"completed alphabet: {letter}")
        if all_recipes:
            pd.DataFrame(all_recipes).to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False)
        print("completed scraping successfully!")
    except KeyboardInterrupt:
        print("Scraping interrupted. Saving progress...")
        pd.DataFrame(all_recipes).to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False)
    finally:
        driver.quit()
        
if __name__ == "__main__":
    scrape_foodcom()

