import time
import sqlite3
from collections import deque
import numpy as np

# Определение класса DBManager ПЕРЕД TaskManager
class DBManager:
    def __init__(self, db_name='tasks.db'):
        """
        Ініціалізує менеджер бази даних.
        Підключається до вказаної бази даних SQLite та створює таблицю завдань, якщо вона не існує.

        Args:
            db_name (str): Ім'я файлу бази даних SQLite.
        """
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self._connect()
        self._create_table()

    def _connect(self):
        """Встановлює з'єднання з базою даних."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            print(f"Підключено до бази даних: {self.db_name}")
        except sqlite3.Error as e:
            print(f"Помилка підключення до бази даних: {e}")
            raise

    def _create_table(self):
        """Створює таблицю 'tasks', якщо вона не існує."""
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user TEXT NOT NULL,
                    description TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.conn.commit()
            print("Таблиця 'tasks' перевірена/створена.")
        except sqlite3.Error as e:
            print(f"Помилка створення таблиці: {e}")
            raise

    def add_task(self, user_id, description, status='Очікує'):
        """
        Додає нове завдання до бази даних.

        Returns:
            int: ID новоствореного завдання.
        """
        try:
            self.cursor.execute(
                "INSERT INTO tasks (user, description, status) VALUES (?, ?, ?)",
                (user_id, description, status)
            )
            self.conn.commit()
            task_id = self.cursor.lastrowid
            return task_id
        except sqlite3.Error as e:
            print(f"Помилка додавання завдання: {e}")
            return None

    def update_task_status(self, task_id, new_status):
        """
        Оновлює статус завдання за його ID.
        """
        try:
            self.cursor.execute(
                "UPDATE tasks SET status = ? WHERE id = ?",
                (new_status, task_id)
            )
            self.conn.commit()
            return self.cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"Помилка оновлення статусу завдання {task_id}: {e}")
            return False

    def get_all_tasks(self):
        """
        Отримує всі завдання з бази даних.

        Returns:
            list: Список словників, кожен з яких представляє завдання.
        """
        try:
            self.cursor.execute("SELECT id, user, description, status, timestamp FROM tasks ORDER BY id")
            rows = self.cursor.fetchall()
            tasks = []
            for row in rows:
                tasks.append({
                    'id': row[0],
                    'user': row[1],
                    'description': row[2],
                    'status': row[3],
                    'timestamp': row[4]
                })
            return tasks
        except sqlite3.Error as e:
            print(f"Помилка отримання всіх завдань: {e}")
            return []

    def get_task_by_id(self, task_id):
        """
        Отримує завдання за його ID.

        Returns:
            dict: Словник з інформацією про завдання або None, якщо не знайдено.
        """
        try:
            self.cursor.execute("SELECT id, user, description, status, timestamp FROM tasks WHERE id = ?", (task_id,))
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'user': row[1],
                    'description': row[2],
                    'status': row[3],
                    'timestamp': row[4]
                }
            return None
        except sqlite3.Error as e:
            print(f"Помилка отримання завдання {task_id}: {e}")
            return None

    def close(self):
        """Закриває з'єднання з базою даних."""
        if self.conn:
            self.conn.close()
            print(f"З'єднання з базою даних {self.db_name} закрито.")


# Определение класса TaskManager
class TaskManager:
    def __init__(self, db_name='tasks.db'):
        """
        Ініціалізує менеджер завдань.
        Містить чергу завдань (queue) та використовує DBManager для взаємодії з базою даних.
        """
        self.db_manager = DBManager(db_name) # Теперь DBManager доступен
        self.task_queue = deque()
        self._load_pending_tasks_to_queue()
        print("\n--- Симуляція багатокористувацької системи з чергою завдань (з SQLite) ---")

    def _load_pending_tasks_to_queue(self):
        """
        Завантажує завдання зі статусом 'Очікує' або 'В процесі' з БД у чергу.
        """
        all_db_tasks = self.db_manager.get_all_tasks()
        for task in all_db_tasks:
            if task['status'] == 'Очікує' or task['status'] == 'В процесі':
                self.task_queue.append(task['id'])
        print(f"Завантажено {len(self.task_queue)} завдань у чергу з БД.")
        self.display_queue_status()

    def add_task(self, user_id, task_description):
        """
        Додає нове завдання до бази даних та до черги.
        """
        task_id = self.db_manager.add_task(user_id, task_description, 'Очікує')
        if task_id is not None:
            self.task_queue.append(task_id)
            print(f"[{user_id}] Додано завдання: ID {task_id} - '{task_description}'")
        self.display_queue_status()

    def process_next_task(self):
        """
        Обробляє наступне завдання з черги.
        """
        if not self.task_queue:
            print("Черга завдань порожня. Немає завдань для обробки.")
            return False

        task_id = self.task_queue.popleft()
        task_info = self.db_manager.get_task_by_id(task_id)

        if task_info:
            self.db_manager.update_task_status(task_id, 'В процесі')
            print(
                f"\n[Система] Обробка завдання: ID {task_id} від користувача '{task_info['user']}' - '{task_info['description']}'")
            time.sleep(1)

            if np.random.rand() < 0.8:
                new_status = 'Виконано'
            else:
                new_status = 'Помилка'

            self.db_manager.update_task_status(task_id, new_status)
            print(f"[Система] Завдання ID {task_id} - '{task_info['description']}' - Статус: {new_status}.")
        else:
            print(f"[Система] Помилка: Завдання ID {task_id} не знайдено в базі даних.")
            return False

        self.display_queue_status()
        return True

    def display_queue_status(self):
        """
        Відображає поточний стан черги (ID завдань).
        """
        print(f"Поточний стан черги (ID): {list(self.task_queue)}")

    def display_all_tasks_status(self):
        """
        Відображає статус всіх завдань, зчитаних з бази даних.
        """
        print("\n--- Загальний статус всіх завдань (з БД) ---")
        all_tasks = self.db_manager.get_all_tasks()
        if not all_tasks:
            print("Немає завдань у базі даних.")
            return

        print(f"{'ID':<5} | {'Користувач':<15} | {'Статус':<10} | {'Опис':<30} | {'Час створення':<20}")
        print("-" * 90)
        for task in all_tasks:
            print(
                f"{task['id']:<5} | {task['user']:<15} | {task['status']:<10} | {task['description']:<30} | {task['timestamp']:<20}")
        print("-" * 90)

    def generate_report(self):
        """
        Генерує звіт про оброблені та необроблені завдання, зчитані з бази даних.
        """
        print("\n--- Звіт про обробку завдань (з БД) ---")
        all_tasks = self.db_manager.get_all_tasks()

        processed_count = 0
        unprocessed_count = 0
        processed_list = []
        unprocessed_list = []

        for task in all_tasks:
            if task['status'] == 'Виконано':
                processed_count += 1
                processed_list.append(task)
            elif task['status'] == 'Помилка':
                unprocessed_count += 1
                unprocessed_list.append(task)
            # Также можно добавить 'Очікує' и 'В процесі' как необработанные, если они не были завершены
            elif task['status'] == 'Очікує' or task['status'] == 'В процесі':
                unprocessed_count += 1
                unprocessed_list.append(task)


        print(f"Всього зареєстровано завдань у БД: {len(all_tasks)}")
        print(f"Кількість успішно оброблених завдань: {processed_count}")
        print(f"Кількість завдань, що завершилися з помилкою або знаходяться в черзі: {unprocessed_count}")

        if processed_list:
            print("\nУспішно оброблені завдання:")
            for task in processed_list:
                print(f"- ID {task['id']} (Користувач: {task['user']}, Опис: '{task['description']}')")
        else:
            print("\nНемає успішно оброблених завдань.")

        if unprocessed_list:
            print("\nНеоброблені (з помилкою або в черзі) завдання:")
            for task in unprocessed_list:
                print(f"- ID {task['id']} (Користувач: {task['user']}, Опис: '{task['description']}', Статус: {task['status']})")
        else:
            print("\nНемає завдань, що завершилися з помилкой або знаходяться в черзі.")


    def __del__(self):
        """
        Деструктор: гарантує закриття з'єднання з базою даних при завершенні роботи об'єкта.
        """
        if hasattr(self, 'db_manager') and self.db_manager is not None:
            self.db_manager.close()


if __name__ == "__main__":
    # Запустіть цей файл, і він створить/використає tasks.db
    manager = TaskManager('tasks.db')

    manager.add_task("UserA", "Надіслати звіт до 17:00")
    manager.add_task("UserB", "Створити новий проект")
    manager.add_task("UserC", "Відповісти на email")
    manager.add_task("UserA", "Забронювати переговорну")
    manager.add_task("UserB", "Підготувати презентацію")

    print("\n--- Початок обробки завдань ---")

    for _ in range(4):
        if not manager.process_next_task():
            break
        time.sleep(0.5)

    manager.add_task("UserD", "Перевірити базу даних")
    manager.add_task("UserC", "Оновити програмне забезпечення")

    while manager.task_queue:
        if not manager.process_next_task():
            break
        time.sleep(0.5)

    print("\n--- Обробка завдань завершена ---")

    manager.display_all_tasks_status()

    manager.generate_report()