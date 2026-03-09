#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv('DATA_DIR', '/app/data'))
DATA_DIR.mkdir(parents=True, exist_ok=True)

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DATA_DIR / 'parking_monitor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ParkingMonitor:
    def __init__(self, telegram_token, telegram_chat_id, check_interval=300):
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.check_interval = check_interval
        self.url = 'https://www.absrealty.ru/parking/?orderBy=price&type=ParkingType&project=pervyj-moskovskij'
        self.db_path = DATA_DIR / 'parking_history.db'
        self.last_state = None
        self.last_update_id = 0
        self.bot_token_valid = False
        self.telegram_commands_enabled = True
        self.telegram_failure_reason = None
        self.telegram_thread = None
        self.telegram_thread_lock = threading.Lock()
        self.parse_failures = 0

        self.user_logger = logging.getLogger('user_actions')
        self.user_logger.setLevel(logging.INFO)
        self.user_logger.propagate = False
        if not self.user_logger.handlers:
            user_handler = logging.FileHandler(DATA_DIR / 'user_actions.log', encoding='utf-8')
            user_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
            self.user_logger.addHandler(user_handler)

        self.init_db()

        logger.info('ParkingMonitor initialized')
        logger.info('DB: %s', self.db_path)
        logger.info('Interval: %d seconds', self.check_interval)
        self.verify_telegram_token()

    def verify_telegram_token(self):
        try:
            response = requests.get(
                f'https://api.telegram.org/bot{self.telegram_token}/getMe',
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    bot_name = data.get('result', {}).get('username', 'unknown')
                    logger.info('Telegram bot verified: @%s', bot_name)
                    self.bot_token_valid = True
                    self.telegram_commands_enabled = True
                    self.telegram_failure_reason = None
                    return True
                logger.error('Telegram API error: %s', data.get('description', 'unknown'))
                return False
            if response.status_code == 401:
                logger.error('ERROR: Invalid Telegram token (401 Unauthorized)')
                self.telegram_commands_enabled = False
                self.telegram_failure_reason = 'unauthorized'
                return False
            logger.error('Telegram verification failed: status %d', response.status_code)
            return False
        except Exception as exc:
            logger.error('Telegram verification error: %s', exc)
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
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS parking_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    count INTEGER,
                    price_min REAL,
                    price_max REAL,
                    data_hash TEXT UNIQUE,
                    changes TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                '''
            )
            for col in ['username', 'first_name', 'last_name']:
                try:
                    cursor.execute(f'ALTER TABLE subscribers ADD COLUMN {col} TEXT')
                except sqlite3.OperationalError:
                    pass

            if self.telegram_chat_id:
                try:
                    cursor.execute(
                        'INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)',
                        (self.telegram_chat_id,),
                    )
                except Exception as exc:
                    logger.warning('Failed to auto-subscribe admin: %s', exc)

            conn.commit()
            conn.close()
            logger.info('Database initialized')
        except Exception as exc:
            logger.error('Database init error: %s', exc)
            raise

    def get_subscribers(self):
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT chat_id FROM subscribers')
            rows = cursor.fetchall()
            conn.close()
            return [row[0] for row in rows]
        except Exception as exc:
            logger.error('Error getting subscribers: %s', exc)
            return []

    def get_stats(self):
        try:
            subs = self.get_subscribers()
            log_path = DATA_DIR / 'user_actions.log'
            actions = []
            if log_path.exists():
                actions = log_path.read_text(encoding='utf-8').splitlines()[-10:]

            actions_text = '\n'.join(actions) if actions else 'Нет записей'
            return (
                '📊 <b>Статистика</b>\n'
                f'Активных подписчиков: {len(subs)}\n\n'
                '<b>Последние действия:</b>\n'
                f'{actions_text}'
            )
        except Exception as exc:
            logger.error('Stats error: %s', exc)
            return 'Ошибка получения статистики'

    def subscribe(self, chat_id, chat_info=None):
        try:
            username = chat_info.get('username') if chat_info else None
            first_name = chat_info.get('first_name') if chat_info else None
            last_name = chat_info.get('last_name') if chat_info else None

            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM subscribers WHERE chat_id = ?', (chat_id,))
            was_existing = cursor.fetchone() is not None
            cursor.execute(
                '''
                INSERT INTO subscribers (chat_id, added_at, username, first_name, last_name)
                VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name
                ''',
                (chat_id, username, first_name, last_name),
            )
            conn.commit()
            conn.close()
            self.user_logger.info('SUBSCRIBE: %s', chat_id)

            if not was_existing and str(chat_id) != str(self.telegram_chat_id):
                name = f"{first_name or ''} {last_name or ''}".strip() or 'не указано'
                user_link = f'@{username}' if username else 'нет'
                self.send_telegram(
                    '👤 <b>Новый подписчик!</b>\n'
                    f'ID: <code>{chat_id}</code>\n'
                    f'Имя: {name}\n'
                    f'Username: {user_link}',
                    self.telegram_chat_id,
                )

            return True
        except Exception as exc:
            logger.error('Subscribe error: %s', exc)
            return False

    def unsubscribe(self, chat_id):
        try:
            conn = self.get_db_connection()
            conn.execute('DELETE FROM subscribers WHERE chat_id = ?', (chat_id,))
            conn.commit()
            conn.close()
            self.user_logger.info('UNSUBSCRIBE: %s', chat_id)
            return True
        except Exception as exc:
            logger.error('Unsubscribe error: %s', exc)
            return False

    def send_telegram(self, message, chat_id=None):
        try:
            if chat_id is None:
                chat_id = self.telegram_chat_id
            if not self.bot_token_valid:
                logger.warning('Bot token invalid, skipping send')
                return False

            response = requests.post(
                f'https://api.telegram.org/bot{self.telegram_token}/sendMessage',
                json={
                    'chat_id': chat_id,
                    'text': message,
                    'parse_mode': 'HTML',
                },
                timeout=10,
            )
            if response.status_code == 200:
                logger.info('Message sent to chat %s', chat_id)
                return True

            logger.error('Send error: status %d', response.status_code)
            if response.status_code == 401:
                self.bot_token_valid = False
                self.telegram_commands_enabled = False
                self.telegram_failure_reason = 'unauthorized'
            return False
        except Exception as exc:
            logger.error('Send error: %s', exc)
            return False

    def normalize_text(self, text):
        text = text.replace('\xa0', ' ').replace('\u202f', ' ')
        return re.sub(r'\s+', ' ', text).strip()

    def parse_number(self, value):
        digits = re.sub(r'\D', '', value)
        return int(digits) if digits else None

    def extract_prices(self, text_content):
        patterns = [
            r'Стоимость.*?(\d[\d\s]{4,})\D+(\d[\d\s]{4,})',
            r'Цена.*?от\s*(\d[\d\s]{4,}).*?до\s*(\d[\d\s]{4,})',
            r'от\s*(\d[\d\s]{4,}).*?до\s*(\d[\d\s]{4,})',
        ]
        for pattern in patterns:
            match = re.search(pattern, text_content, re.IGNORECASE | re.DOTALL)
            if not match:
                continue
            price_min = self.parse_number(match.group(1))
            price_max = self.parse_number(match.group(2))
            if price_min is not None and price_max is not None:
                return price_min, price_max

        numbers = [
            self.parse_number(value)
            for value in re.findall(r'\d[\d\s]{4,}', text_content)
        ]
        prices = [value for value in numbers if value is not None and value >= 100000]
        if len(prices) >= 2:
            return min(prices), max(prices)
        return None, None

    def notify_admin_parse_error(self):
        if not self.bot_token_valid or not self.telegram_chat_id:
            return
        if self.parse_failures == 3:
            self.send_telegram(
                '⚠️ Не удалось распарсить страницу парковки 3 раза подряд. '
                f'Проверьте структуру сайта: {self.url}',
                self.telegram_chat_id,
            )

    def parse_page(self):
        try:
            response = requests.get(
                self.url,
                headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'},
                timeout=15,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = self.normalize_text(soup.get_text(separator=' ', strip=True))

            count_match = re.search(r'(\d+)\s+машино[-\s]?мест', text_content, re.IGNORECASE)
            count = int(count_match.group(1)) if count_match else 0

            price_min, price_max = self.extract_prices(text_content)
            if price_min is None or price_max is None:
                self.parse_failures += 1
                logger.warning('Price pattern not found')
                if self.parse_failures >= 3:
                    self.notify_admin_parse_error()
                return None

            self.parse_failures = 0
            return {
                'count': count,
                'price_min': price_min,
                'price_max': price_max,
                'timestamp': datetime.now().isoformat(),
                'url': self.url,
            }
        except Exception as exc:
            self.parse_failures += 1
            logger.error('Parse error: %s', exc)
            if self.parse_failures >= 3:
                self.notify_admin_parse_error()
            return None

    def get_data_hash(self, data):
        data_str = json.dumps(
            {
                'count': data['count'],
                'price_min': data['price_min'],
                'price_max': data['price_max'],
            },
            sort_keys=True,
        )
        return hashlib.md5(data_str.encode()).hexdigest()

    def save_to_db(self, data, changes):
        max_retries = 3
        retry_delay = 0.5

        for attempt in range(max_retries):
            conn = None
            try:
                data_hash = self.get_data_hash(data)
                conn = self.get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    INSERT INTO parking_history (count, price_min, price_max, data_hash, changes)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (data['count'], data['price_min'], data['price_max'], data_hash, changes),
                )
                conn.commit()
                conn.close()
                return
            except sqlite3.IntegrityError:
                if conn:
                    conn.close()
                logger.debug('No changes (duplicate)')
                return
            except sqlite3.OperationalError as exc:
                if conn:
                    conn.close()
                if 'database is locked' in str(exc):
                    if attempt < max_retries - 1:
                        logger.warning('DB locked, retry %d/%d', attempt + 1, max_retries)
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.error('DB locked after retries')
                    continue
                logger.error('DB error: %s', exc)
                return
            except Exception as exc:
                if conn:
                    conn.close()
                logger.error('Save error: %s', exc)
                return

    def detect_changes(self, new_data):
        if self.last_state is None:
            return None

        changes = []
        if new_data['count'] != self.last_state['count']:
            diff = new_data['count'] - self.last_state['count']
            sign = '+' if diff > 0 else ''
            changes.append(f'• Мест: {self.last_state["count"]} ➡️ {new_data["count"]} ({sign}{diff})')
        if new_data['price_min'] != self.last_state['price_min']:
            changes.append(
                '• Мин. цена: {:,} ➡️ {:,} ₽'.format(
                    self.last_state['price_min'], new_data['price_min']
                ).replace(',', ' ')
            )
        if new_data['price_max'] != self.last_state['price_max']:
            changes.append(
                '• Макс. цена: {:,} ➡️ {:,} ₽'.format(
                    self.last_state['price_max'], new_data['price_max']
                ).replace(',', ' ')
            )
        return '\n'.join(changes) if changes else None

    def get_current_info(self):
        data = self.parse_page()
        if not data:
            return None
        if self.last_state is None:
            self.last_state = data
        return data

    def start_telegram_thread(self):
        if not self.bot_token_valid or not self.telegram_commands_enabled:
            return
        with self.telegram_thread_lock:
            if self.telegram_thread and self.telegram_thread.is_alive():
                return
            self.telegram_thread = threading.Thread(target=self.handle_telegram_commands, daemon=True)
            self.telegram_thread.start()
            logger.info('Telegram handler started')

    def handle_telegram_commands(self):
        if not self.bot_token_valid:
            logger.error('Bot token invalid, skipping commands')
            return

        consecutive_errors = 0
        backoff_delay = 5
        while True:
            try:
                response = requests.get(
                    f'https://api.telegram.org/bot{self.telegram_token}/getUpdates',
                    params={'offset': self.last_update_id + 1, 'timeout': 30},
                    timeout=35,
                )

                if response.status_code == 401:
                    logger.error('Auth failed')
                    self.bot_token_valid = False
                    self.telegram_commands_enabled = False
                    self.telegram_failure_reason = 'unauthorized'
                    return

                if response.status_code != 200:
                    consecutive_errors += 1
                    logger.error('getUpdates error: %d', response.status_code)
                    logger.warning('Telegram polling degraded, retry in %d seconds', backoff_delay)
                    time.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 2, 60)
                    continue

                consecutive_errors = 0
                backoff_delay = 5
                updates = response.json().get('result', [])
                for update in updates:
                    self.last_update_id = update.get('update_id', self.last_update_id)
                    message = update.get('message', {})
                    chat = message.get('chat', {})
                    chat_id = chat.get('id')
                    text = message.get('text', '').strip()
                    if not chat_id or not text:
                        continue

                    logger.info('Command from %s: %s', chat_id, text)
                    if text == '/start':
                        first_name = chat.get('first_name', 'друг')
                        self.send_telegram(
                            '👋 Привет, {}!\n\n'
                            '🅿️ Мониторинг парковки ЖК Первый Московский запущен.\n\n'
                            'Команды:\n'
                            '/info - Текущий статус\n'
                            '/subscribe - Подписаться\n'
                            '/unsubscribe - Отписаться\n'
                            '/help - Помощь'.format(first_name),
                            chat_id,
                        )
                    elif text in ('/info', '/current'):
                        data = self.get_current_info()
                        if data:
                            dt = datetime.fromisoformat(data['timestamp']).strftime('%d.%m.%Y %H:%M:%S')
                            reply = (
                                '🅿️ <b>Статус парковки</b>\n\n'
                                f'🚗 Свободно мест: <b>{data["count"]}</b>\n'
                                '💰 Цена от: <b>{:,} ₽</b>\n'
                                '💰 Цена до: <b>{:,} ₽</b>\n'
                                f'🕒 Обновлено: {dt}'
                            ).format(data['price_min'], data['price_max']).replace(',', ' ')
                        else:
                            reply = '❌ Ошибка получения данных'
                        self.send_telegram(reply, chat_id)
                    elif text == '/subscribe':
                        if self.subscribe(chat_id, chat):
                            self.send_telegram('✅ Вы подписались на обновления.', chat_id)
                        else:
                            self.send_telegram('❌ Ошибка подписки.', chat_id)
                    elif text == '/unsubscribe':
                        if self.unsubscribe(chat_id):
                            self.send_telegram('❎ Вы отписались от обновлений.', chat_id)
                        else:
                            self.send_telegram('❌ Ошибка отписки.', chat_id)
                    elif text == '/stats':
                        if str(chat_id) == str(self.telegram_chat_id):
                            self.send_telegram(self.get_stats(), chat_id)
                        else:
                            self.send_telegram('⛔ Доступ запрещен.', chat_id)
                    elif text == '/simulate':
                        if str(chat_id) == str(self.telegram_chat_id):
                            if self.last_state and 'count' in self.last_state:
                                self.last_state['count'] -= 1
                                self.send_telegram(
                                    '✅ Симуляция: внутренний счетчик уменьшен на 1. '
                                    'Ждите уведомления при следующей проверке.',
                                    chat_id,
                                )
                            else:
                                self.send_telegram('⚠️ Данных еще нет. Подождите первой проверки сайта.', chat_id)
                        else:
                            self.send_telegram('⛔ Доступ запрещен.', chat_id)
                    elif text == '/test':
                        if str(chat_id) == str(self.telegram_chat_id):
                            test_msg = (
                                '🧪 <b>ТЕСТОВОЕ УВЕДОМЛЕНИЕ</b>\n\n'
                                '<b>Что изменилось:</b>\n'
                                '• Мест: 5 ➡️ 6 (+1)\n\n'
                                '<b>Текущий статус:</b>\n'
                                '🚗 Свободно: <b>6</b>\n'
                                '💰 Цена от: <b>1 000 000 ₽</b>\n'
                                '💰 Цена до: <b>2 000 000 ₽</b>\n'
                                f'🕒 <i>{datetime.now().strftime("%d.%m.%Y %H:%M:%S")}</i>'
                            )
                            self.send_telegram(test_msg, chat_id)
                    elif text == '/help':
                        self.send_telegram(
                            'ℹ️ <b>Помощь</b>\n\n'
                            '/info - Проверить места\n'
                            '/subscribe - Включить уведомления\n'
                            '/unsubscribe - Выключить уведомления\n'
                            '/help - Справка',
                            chat_id,
                        )
                    else:
                        self.send_telegram('Неизвестная команда. Введите /help', chat_id)
            except requests.exceptions.Timeout:
                consecutive_errors += 1
                logger.warning('Timeout, reconnecting in %d seconds', backoff_delay)
                time.sleep(backoff_delay)
                backoff_delay = min(backoff_delay * 2, 60)
            except Exception as exc:
                consecutive_errors += 1
                logger.error('Handler error: %s', exc)
                time.sleep(backoff_delay)
                backoff_delay = min(backoff_delay * 2, 60)

    def run(self):
        logger.info('=' * 60)
        logger.info('Starting monitor')
        logger.info('=' * 60)

        if self.bot_token_valid:
            self.start_telegram_thread()
        else:
            logger.warning('Bot token invalid - commands disabled')

        iteration = 0
        while True:
            try:
                iteration += 1
                logger.info('Check #%d', iteration)

                if self.bot_token_valid and self.telegram_commands_enabled:
                    if not self.telegram_thread or not self.telegram_thread.is_alive():
                        logger.warning('Telegram handler stopped unexpectedly, restarting')
                        self.start_telegram_thread()

                data = self.parse_page()
                if not data:
                    logger.warning('Failed to get data')
                    time.sleep(self.check_interval)
                    continue

                changes = self.detect_changes(data)
                self.save_to_db(data, changes or '')
                if changes:
                    dt = datetime.fromisoformat(data['timestamp']).strftime('%d.%m.%Y %H:%M:%S')
                    message = (
                        '🚨 <b>ОБНОВЛЕНИЕ ПАРКОВКИ</b>\n'
                        '📍 <i>Первый Московский</i>\n\n'
                        f'<b>Что изменилось:</b>\n{changes}\n\n'
                        '<b>Текущий статус:</b>\n'
                        f'🚗 Свободно: <b>{data["count"]}</b>\n'
                        '💰 Цена от: <b>{:,} ₽</b>\n'
                        '💰 Цена до: <b>{:,} ₽</b>\n'
                        f'🕒 <i>{dt}</i>'
                    ).format(data['price_min'], data['price_max']).replace(',', ' ')
                    for sub_id in self.get_subscribers():
                        self.send_telegram(message, sub_id)
                    logger.info(
                        'CHANGES: Count=%d, Min=%d, Max=%d',
                        data['count'],
                        data['price_min'],
                        data['price_max'],
                    )
                else:
                    logger.info(
                        'No changes: Count=%d, Price=%d-%d',
                        data['count'],
                        data['price_min'],
                        data['price_max'],
                    )

                self.last_state = data
                time.sleep(self.check_interval)
            except KeyboardInterrupt:
                logger.info('Stopped')
                break
            except Exception as exc:
                logger.error('Error: %s', exc, exc_info=True)
                time.sleep(self.check_interval)


def main():
    logger.info('=' * 60)
    logger.info('PARKING MONITOR START')
    logger.info('=' * 60)

    telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
    telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
    check_interval = int(os.getenv('CHECK_INTERVAL', 60))

    if not telegram_token or not telegram_chat_id:
        logger.error('ERROR: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID')
        sys.exit(1)

    try:
        monitor = ParkingMonitor(telegram_token, telegram_chat_id, check_interval)
        monitor.run()
    except Exception as exc:
        logger.error('CRITICAL: %s', exc, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
