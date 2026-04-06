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

    @staticmethod
    def get_credentials_from_env() -> tuple[str, str]:
        env_path = Path(__file__).resolve().parents[2] / ".env"
        load_dotenv(dotenv_path=env_path)
        phone = os.getenv("SPORTY_PHONE")
        password = os.getenv("SPORTY_PASSWORD")
        if not phone or not password:
            raise ValueError("Phone number or password not found in .env file.")
        return phone, password

    def is_header_login_form_visible(self) -> bool:
        """
        True when the top bar shows phone/password login (session expired / logged out).
        Matches SportyBet header: div.m-login-bar with visible phone input.
        """
        if not self.driver:
            return False
        try:
            for el in self.driver.find_elements(
                By.CSS_SELECTOR, ".m-login-bar input[name='phone']"
            ):
                try:
                    if el.is_displayed():
                        return True
                except Exception:
                    continue
            return False
        except Exception:
            return False

    def relogin_via_header(self) -> bool:
        """
        If the header login form is visible, submit credentials from .env.
        Returns True if a login attempt was made.
        """
        if not self.is_header_login_form_visible():
            return False
        phone, password = self.get_credentials_from_env()
        self.enter_credentials(phone, password)
        self.click_login()
        time.sleep(5)
        self.logged_in = True
        return True

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
            phone, password = self.get_credentials_from_env()
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
