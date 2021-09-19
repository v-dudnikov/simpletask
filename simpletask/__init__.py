import time
import os
import sqlite3
import threading
import pid


class SQLiteQueueConnector:
    def __init__(self, queue=None):
        self.queue = 'default' or queue

        self.workdir = os.path.join(os.path.expanduser('~'), '.simpletask')
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        self.filename = os.path.join(self.workdir, self.queue)
        self.connection = sqlite3.connect(self.filename)
        self.cursor = self.connection.cursor()

        self._db_create_table()

    def add(self, task, commit=True):
        query = '''
        INSERT INTO tasks (command, period, timestamp, oneshot, enabled)
        VALUES (?, ?, ?, ?, ?)
        '''

        self.cursor.execute(
            query,
            (task['command'], task['period'], task['timestamp'],
            task['oneshot'], task['enabled'])
        )

        if commit:
            self.connection.commit()

    def get(self, id):
        query = '''SELECT * FROM tasks WHERE id=?'''
        self.cursor.execute(query, (id,))
        task = self.cursor.fetchall()

        if task:
            return task[0]

        return None

    def delete(self, id, commit=True):
        query = '''DELETE FROM tasks WHERE id=?'''

        self.cursor.execute(query, (id,))
        if commit:
            self.connection.commit()

    def update(self, id, task, commit=True):
        query = '''
        UPDATE tasks SET
            command=?,
            period=?,
            timestamp=?,
            oneshot=?,
            enabled=?
        WHERE id=?
        '''
        self.cursor.execute(query, (
            task['command'],
            task ['period'],
            task['timestamp'],
            task['oneshot'],
            task['enabled'],
            task['id']
        ))

        if commit:
            self.connection.commit()

    def update_column(self, id, column, value, commit=True):
        query = '''
        UPDATE tasks SET
            %s=?
        WHERE id=?
        ''' % column

        self.cursor.execute(query, (value, id))
        if commit:
            self.connection.commit()

    def all(self):
        query = '''
        SELECT id, command, period, timestamp, oneshot, enabled
        FROM tasks
        '''

        self.cursor.execute(query)

        tasks = []
        for row in self.cursor.fetchall():
            id, command, period, timestamp, oneshot, enabled = row
            tasks.append(dict(
                id=id,
                command=command,
                period=period,
                timestamp=timestamp,
                oneshot=oneshot,
                enabled=enabled
            ))

        return tasks

    def commit(self):
        self.connection.commit()

    def _db_create_table(self):
        query = '''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            period INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            oneshot BOOLEAN DEFAULT 1,
            enabled BOOLEAN DEFAULT 1
        )
        '''

        self.cursor.execute(query)
        self.connection.commit()


class Scheduler:
    def __init__(self, queue=None):
        self.connector = SQLiteQueueConnector(queue)

    def _step(self):
        def execute(command):
            os.system(command)

        tasks = self.connector.all()
        to_execute = []

        for task in tasks:
            if not task['enabled']:
                continue

            now = int(time.time())
            if now - task['timestamp'] > task['period']:
                to_execute.append(task)

        for task in to_execute:
            thread = threading.Thread(target=execute, args=(task['command'],), daemon=True)
            thread.start()

            if not task['oneshot']:
                self.connector.update_column(task['id'], 'timestamp', int(time.time()))
            else:
                self.connector.delete(task['id'])

    def add_task(self, command, period, oneshot):
        task = dict(
            command=command,
            period=period,
            oneshot=oneshot,
            timestamp=int(time.time()),
            enabled=True
        )

        self.connector.add(task)

    def run(self, interval):
        pid_fn = os.path.join(self.connector.workdir, self.connector.queue)
        with pid.PidFile(pid_fn) as p:
            try:
                while True:
                    self._step()
                    time.sleep(interval)
            except KeyboardInterrupt:
                exit(0)

