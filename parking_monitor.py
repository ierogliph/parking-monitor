#!/usr/bin/env python3
import requests
import json
import time
import sqlite3
import re
from datetime import datetime
from pathlib import Path
import hashlib
import logging
import os
import sys
import threading
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

log_level = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/data/parking_monitor.log')
    ]
)
logger = logging.getLogger(__name__)


class ParkingMonitor:
    def __init__(self, telegram_token, telegram_chat_id, check_interval=300):
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.check_interval = check_interval
        self.url = "https://www.absrealty.ru/parking/?orderBy=price&type=ParkingType&project=pervyj-moskovskij"
        
        db_dir = Path('/app/data')
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / "parking_history.db"
        
        self.user_logger = logging.getLogger('user_actions')
        self.user_logger.setLevel(logging.INFO)
        self.user_logger.propagate = False
        if not self.user_logger.handlers:
            user_handler = logging.FileHandler(db_dir / "user_actions.log")
            user_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
            self.user_logger.addHandler(user_handler)
        
        self.init_db()
        self.last_state = None
        self.last_update_id = 0
        self.bot_token_valid = False
        
        logger.info("ParkingMonitor initialized")
        logger.info("DB: %s", self.db_path)
        logger.info("Interval: %d seconds", self.check_interval)
        self.verify_telegram_token()
        
    def verify_telegram_token(self):
        try:
            url = "https://api.telegram.org/bot{}/getMe".format(self.telegram_token)
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    bot_name = data.get('result', {}).get('username', 'unknown')
                    logger.info("Telegram bot verified: @%s", bot_name)
                    self.bot_token_valid = True
                    return True
                else:
                    logger.error("Telegram API error: %s", data.get('description', 'unknown'))
                    return False
            elif response.status_code == 401:
                logger.error("ERROR: Invalid Telegram token (401 Unauthorized)")
                return False
            else:
                logger.error("Telegram verification failed: status %d", response.status_code)
                return False
        except Exception as e:
            logger.error("Telegram verification error: %s", e)
            return False
        
    def get_db_connection(self):
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA cache_size=5000')
        return conn
        
    def init_db(self):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS parking_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    count INTEGER,
                    price_min REAL,
                    price_max REAL,
                    data_hash TEXT UNIQUE,
                    changes TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Миграция: добавляем колонки для существующих баз данных
            for col in ['username', 'first_name', 'last_name']:
                try:
                    cursor.execute(f'ALTER TABLE subscribers ADD COLUMN {col} TEXT')
                except sqlite3.OperationalError:
                    pass # Колонка уже существует

            # Автоматически подписываем администратора при инициализации
            if self.telegram_chat_id:
                try:
                    cursor.execute('INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)', (self.telegram_chat_id,))
                except Exception as e:
                    logger.warning("Failed to auto-subscribe admin: %s", e)
            conn.commit()
            conn.close()
            logger.info("Database initialized")
        except Exception as e:
            logger.error("Database init error: %s", e)
            raise
    
    def get_subscribers(self):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT chat_id FROM subscribers')
            rows = cursor.fetchall()
            conn.close()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error("Error getting subscribers: %s", e)
            return []

    def get_stats(self):
        try:
            subs = self.get_subscribers()
            
            log_path = self.db_path.parent / "user_actions.log"
            actions = []
            if log_path.exists():
                with open(log_path, 'r') as f:
                    actions = [line.strip() for line in f.readlines()[-10:]]
            
            msg = "📊 <b>Статистика</b>\nАктивных подписчиков: {}\n\n<b>Последние действия:</b>\n{}".format(
                len(subs), "\n".join(actions) if actions else "Нет записей"
            )
            return msg
        except Exception as e:
            logger.error("Stats error: %s", e)
            return "Ошибка получения статистики"

    def subscribe(self, chat_id, chat_info=None):
        try:
            username = chat_info.get('username') if chat_info else None
            first_name = chat_info.get('first_name') if chat_info else None
            last_name = chat_info.get('last_name') if chat_info else None
            
            conn = self.get_db_connection()
            # Обновляем данные, если пользователь уже был, или вставляем нового
            cursor = conn.execute('''
                INSERT OR REPLACE INTO subscribers (chat_id, added_at, username, first_name, last_name) 
                VALUES (?, COALESCE((SELECT added_at FROM subscribers WHERE chat_id=?), CURRENT_TIMESTAMP), ?, ?, ?)
            ''', (chat_id, chat_id, username, first_name, last_name))
            
            is_new = cursor.rowcount > 0
            conn.commit()
            conn.close()
            self.user_logger.info("SUBSCRIBE: %s", chat_id)
            
            if is_new and str(chat_id) != str(self.telegram_chat_id):
                name = "{} {}".format(first_name or '', last_name or '').strip()
                user_link = "@{}".format(username) if username else "нет"
                self.send_telegram("👤 <b>Новый подписчик!</b>\nID: <code>{}</code>\nИмя: {}\nUsername: {}".format(chat_id, name, user_link), self.telegram_chat_id)
            
            return True
        except Exception as e:
            logger.error("Subscribe error: %s", e)
            return False

    def unsubscribe(self, chat_id):
        try:
            conn = self.get_db_connection()
            conn.execute('DELETE FROM subscribers WHERE chat_id = ?', (chat_id,))
            conn.commit()
            conn.close()
            self.user_logger.info("UNSUBSCRIBE: %s", chat_id)
            return True
        except Exception as e:
            logger.error("Unsubscribe error: %s", e)
            return False

    def send_telegram(self, message, chat_id=None):
        try:
            if chat_id is None:
                chat_id = self.telegram_chat_id
            
            if not self.bot_token_valid:
                logger.warning("Bot token invalid, skipping send")
                return False
                
            url = "https://api.telegram.org/bot{}/sendMessage".format(self.telegram_token)
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info("Message sent to chat %s", chat_id)
                return True
            else:
                logger.error("Send error: status %d", response.status_code)
                if response.status_code == 401:
                    self.bot_token_valid = False
                return False
        except Exception as e:
            logger.error("Send error: %s", e)
            return False
    
    def parse_page(self):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            }
            response = requests.get(self.url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            # Получаем чистый текст страницы, разделяя блоки пробелами
            text_content = soup.get_text(separator=' ', strip=True)
            
            count_match = re.search(r'(\d+)\s+машино-мест', text_content)
            count = int(count_match.group(1)) if count_match else 0
            
            price_min = None
            price_max = None
            
            # Ищем цены в чистом тексте. Паттерн ищет "Стоимость" и затем две группы цифр с пробелами
            # Пример: Стоимость ... 2 772 485 ... 3 500 000
            price_pattern = r'Стоимость.*?(\d+\s+\d+\s+\d+).*?(\d+\s+\d+\s+\d+)'
            price_match = re.search(price_pattern, text_content, re.DOTALL)
            
            if price_match:
                try:
                    price_min_str = price_match.group(1).replace(' ', '')
                    price_max_str = price_match.group(2).replace(' ', '')
                    price_min = int(price_min_str)
                    price_max = int(price_max_str)
                except:
                    logger.error("Failed to parse price numbers")
                    return None
            else:
                logger.warning("Price pattern not found")
                return None
            
            return {
                'count': count,
                'price_min': price_min,
                'price_max': price_max,
                'timestamp': datetime.now().isoformat(),
                'url': self.url
            }
        except Exception as e:
            logger.error("Parse error: %s", e)
            return None
    
    def get_data_hash(self, data):
        data_str = json.dumps({
            'count': data['count'],
            'price_min': data['price_min'],
            'price_max': data['price_max']
        }, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def save_to_db(self, data, changes):
        max_retries = 3
        retry_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                data_hash = self.get_data_hash(data)
                conn = self.get_db_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO parking_history (count, price_min, price_max, data_hash, changes)
                    VALUES (?, ?, ?, ?, ?)
                ''', (data['count'], data['price_min'], data['price_max'], data_hash, changes))
                
                conn.commit()
                conn.close()
                return
                
            except sqlite3.IntegrityError:
                conn.close()
                logger.debug("No changes (duplicate)")
                return
                
            except sqlite3.OperationalError as e:
                conn.close()
                if "database is locked" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning("DB locked, retry %d/%d", attempt + 1, max_retries)
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error("DB locked after retries")
                        return
                else:
                    logger.error("DB error: %s", e)
                    return
                    
            except Exception as e:
                conn.close()
                logger.error("Save error: %s", e)
                return
    
    def detect_changes(self, new_data):
        if self.last_state is None:
            return None
        
        changes = []
        
        if new_data['count'] != self.last_state['count']:
            diff = new_data['count'] - self.last_state['count']
            changes.append("• Мест: {} ➡️ {} ({}{})".format(
                self.last_state['count'], new_data['count'], '+' if diff > 0 else '', diff
            ))
        
        if new_data['price_min'] != self.last_state['price_min']:
            changes.append("• Мин. цена: {:,} ➡️ {:,} ₽".format(
                self.last_state['price_min'], new_data['price_min']
            ).replace(',', ' '))
        
        if new_data['price_max'] != self.last_state['price_max']:
            changes.append("• Макс. цена: {:,} ➡️ {:,} ₽".format(
                self.last_state['price_max'], new_data['price_max']
            ).replace(',', ' '))
        
        return "\n".join(changes) if changes else None
    
    def get_current_info(self):
        data = self.parse_page()
        if not data:
            return None
        if self.last_state is None:
            self.last_state = data
        return data
    
    def handle_telegram_commands(self):
        if not self.bot_token_valid:
            logger.error("Bot token invalid, skipping commands")
            return
            
        logger.info("Telegram handler started")
        consecutive_errors = 0
        
        while True:
            try:
                url = "https://api.telegram.org/bot{}/getUpdates".format(self.telegram_token)
                params = {'offset': self.last_update_id + 1, 'timeout': 30}
                response = requests.get(url, params=params, timeout=35)
                
                if response.status_code == 401:
                    logger.error("Auth failed")
                    self.bot_token_valid = False
                    return
                
                if response.status_code != 200:
                    logger.error("getUpdates error: %d", response.status_code)
                    consecutive_errors += 1
                    if consecutive_errors >= 10:
                        logger.error("Too many errors, stopping")
                        return
                    time.sleep(5)
                    continue
                
                consecutive_errors = 0
                updates = response.json().get('result', [])
                
                for update in updates:
                    self.last_update_id = update.get('update_id', self.last_update_id)
                    message = update.get('message', {})
                    chat = message.get('chat', {})
                    chat_id = chat.get('id')
                    text = message.get('text', '').strip()
                    
                    if not chat_id or not text:
                        continue
                    
                    logger.info("Command from %s: %s", chat_id, text)
                    
                    if text == '/start':
                        first_name = chat.get('first_name', 'друг')
                        self.send_telegram("👋 Привет, {}!\n\n🅿️ Мониторинг парковки ЖК Первый Московский запущен.\n\nКоманды:\n/info - Текущий статус\n/subscribe - Подписаться\n/unsubscribe - Отписаться\n/help - Помощь".format(first_name), chat_id)
                    elif text == '/info' or text == '/current':
                        data = self.get_current_info()
                        if data:
                            dt = datetime.fromisoformat(data['timestamp']).strftime('%d.%m.%Y %H:%M:%S')
                            reply = "🅿️ <b>Статус парковки</b>\n\n🚗 Свободно мест: <b>{}</b>\n💰 Цена от: <b>{:,} ₽</b>\n💰 Цена до: <b>{:,} ₽</b>\n🕒 Обновлено: {}".format(
                                data['count'], data['price_min'], data['price_max'], dt
                            ).replace(',', ' ')
                        else:
                            reply = "❌ Ошибка получения данных"
                        self.send_telegram(reply, chat_id)
                    elif text == '/subscribe':
                        if self.subscribe(chat_id, chat):
                            self.send_telegram("✅ Вы подписались на обновления.", chat_id)
                        else:
                            self.send_telegram("❌ Ошибка подписки.", chat_id)
                    elif text == '/unsubscribe':
                        if self.unsubscribe(chat_id):
                            self.send_telegram("❎ Вы отписались от обновлений.", chat_id)
                        else:
                            self.send_telegram("❌ Ошибка отписки.", chat_id)
                    elif text == '/stats':
                        if str(chat_id) == str(self.telegram_chat_id):
                            self.send_telegram(self.get_stats(), chat_id)
                        else:
                            self.send_telegram("⛔ Доступ запрещен.", chat_id)
                    elif text == '/simulate':
                        if str(chat_id) == str(self.telegram_chat_id):
                            if self.last_state and 'count' in self.last_state:
                                # Уменьшаем счетчик в памяти на 1.
                                # При следующей проверке бот увидит реальное кол-во (на 1 больше) и пришлет уведомление.
                                self.last_state['count'] -= 1
                                self.send_telegram("✅ Симуляция: Внутренний счетчик уменьшен на 1. Ждите уведомления при следующей проверке.", chat_id)
                            else:
                                self.send_telegram("⚠️ Данных еще нет. Подождите первой проверки сайта.", chat_id)
                        else:
                            self.send_telegram("⛔ Доступ запрещен.", chat_id)
                    elif text == '/test':
                        if str(chat_id) == str(self.telegram_chat_id):
                            # Отправляем фейковое уведомление только админу для проверки верстки
                            test_msg = "🧪 <b>ТЕСТОВОЕ УВЕДОМЛЕНИЕ</b>\n\n<b>Что изменилось:</b>\n• Мест: 5 ➡️ 6 (+1)\n\n<b>Текущий статус:</b>\n🚗 Свободно: <b>6</b>\n💰 Цена от: <b>1 000 000 ₽</b>\n💰 Цена до: <b>2 000 000 ₽</b>\n🕒 <i>{}</i>".format(datetime.now().strftime('%d.%m.%Y %H:%M:%S'))
                            self.send_telegram(test_msg, chat_id)
                    elif text == '/help':
                        self.send_telegram("ℹ️ <b>Помощь</b>\n\n/info - Проверить места\n/subscribe - Включить уведомления\n/unsubscribe - Выключить уведомления\n/help - Справка", chat_id)
                    else:
                        self.send_telegram("Неизвестная команда. Введите /help", chat_id)
                        
            except requests.exceptions.Timeout:
                logger.warning("Timeout, reconnecting")
                time.sleep(5)
            except Exception as e:
                logger.error("Handler error: %s", e)
                time.sleep(5)
    
    def run(self):
        logger.info("=" * 60)
        logger.info("Starting monitor")
        logger.info("=" * 60)
        
        if self.bot_token_valid:
            telegram_thread = threading.Thread(target=self.handle_telegram_commands, daemon=True)
            telegram_thread.start()
            logger.info("Telegram handler started")
        else:
            logger.warning("Bot token invalid - commands disabled")
        
        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info("Check #%d", iteration)
                
                data = self.parse_page()
                if not data:
                    logger.warning("Failed to get data")
                    time.sleep(self.check_interval)
                    continue
                
                changes = self.detect_changes(data)
                self.save_to_db(data, changes or "")
                
                if changes:
                    dt = datetime.fromisoformat(data['timestamp']).strftime('%d.%m.%Y %H:%M:%S')
                    message = "🚨 <b>ОБНОВЛЕНИЕ ПАРКОВКИ</b>\n📍 <i>Первый Московский</i>\n\n<b>Что изменилось:</b>\n{}\n\n<b>Текущий статус:</b>\n🚗 Свободно: <b>{}</b>\n💰 Цена от: <b>{:,} ₽</b>\n💰 Цена до: <b>{:,} ₽</b>\n🕒 <i>{}</i>".format(
                        changes, data['count'], data['price_min'], data['price_max'], dt
                    ).replace(',', ' ')
                    subscribers = self.get_subscribers()
                    for sub_id in subscribers:
                        self.send_telegram(message, sub_id)
                    logger.info("CHANGES: Count=%d, Min=%d, Max=%d", data['count'], data['price_min'], data['price_max'])
                else:
                    logger.info("No changes: Count=%d, Price=%d-%d", data['count'], data['price_min'], data['price_max'])
                
                self.last_state = data
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Stopped")
                break
            except Exception as e:
                logger.error("Error: %s", e, exc_info=True)
                time.sleep(self.check_interval)


def main():
    logger.info("=" * 60)
    logger.info("PARKING MONITOR START")
    logger.info("=" * 60)
    
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
    CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 60))
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("ERROR: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        sys.exit(1)
    
    try:
        monitor = ParkingMonitor(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, CHECK_INTERVAL)
        monitor.run()
    except Exception as e:
        logger.error("CRITICAL: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()