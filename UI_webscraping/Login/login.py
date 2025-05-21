from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from dotenv import load_dotenv
import os
from pathlib import Path


class SportyBetLoginBot:
    def __init__(self, url):
        self.driver = None
        self.url = url
        self.logged_in = False

    def open_browser(self):
        try:
            self.driver = webdriver.Chrome()
            print("[INFO] Browser opened successfully.")
        except Exception as e:
            print(f"[ERROR] Failed to open browser: {e}")

    def load_url(self, url):
        try:
            self.driver.get(url)
            print(f"[INFO] Loaded URL: {url}")
        except Exception as e:
            print(f"[ERROR] Failed to load URL: {e}")

    def enter_credentials(self, phone_number, password):
        try:
            wait = WebDriverWait(self.driver, 15)

            # Wait for and enter phone number
            phone_input = wait.until(EC.presence_of_element_located((By.NAME, "phone")))
            phone_input.clear()
            phone_input.send_keys(phone_number)
            print("[INFO] Phone number entered.")

            # Wait for and enter password
            password_input = wait.until(EC.presence_of_element_located((By.NAME, "psd")))
            password_input.clear()
            password_input.send_keys(password)
            print("[INFO] Password entered.")

        except Exception as e:
            print(f"[ERROR] Failed to enter credentials: {e}")

    def click_login(self):
        try:
            wait = WebDriverWait(self.driver, 15)

            # Wait for login button and click
            login_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "m-btn-login")))
            login_button.click()
            print("[INFO] Login button clicked.")

        except Exception as e:
            print(f"[ERROR] Failed to click login button: {e}")

    def close_browser(self):
        if self.driver:
            self.driver.quit()
            print("[INFO] Browser closed.")

    def login(self):
        try:
            
            # Build absolute path to the .env file
            env_path = Path(__file__).resolve().parents[2] / ".env"
            load_dotenv(dotenv_path=env_path)
            phone = os.getenv("SPORTY_PHONE")
            password = os.getenv("SPORTY_PASSWORD")
            if not phone or not password:
                raise ValueError("Phone number or password not found in .env file.")
            print("[INFO] Environment variables loaded.")

            if not self.logged_in:
                self.open_browser()
            self.load_url(self.url)
            time.sleep(2)  # Wait for the page to load

            self.enter_credentials(phone, password)
            self.click_login()

            # Optional: Wait before closing (to confirm login success)
            time.sleep(5)
            self.logged_in = True
            
        except Exception as e:
            print(f"[ERROR] An error occurred during login: {e}")
