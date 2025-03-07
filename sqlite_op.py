from sqlite_base import *


class SQLiteInstance(SQLiteBase):
    def create_table(self):
        sql = '''CREATE TABLE IF NOT EXISTS deployments (
            INSTANCE_ID CHAR(50) NOT NULL,
            DEPLOYMENT_ID CHAR(50) NOT NULL,
            CHUTE_ID CHAR(50) NOT NULL,
            HOST_IP CHAR(50) NOT NULL,
            GPU_TYPE CHAR(50) NOT NULL,
            CREATED_AT CHAR(50) NOT NULL,
            GPU_COUNT INT NOT NULL,
            DELETED_AT CHAR(50) DEFAULT 0
        )'''

        self.cursor.execute(sql)
        self.conn.commit()

    def insert_instance(self, instance_data):
        sql = '''INSERT OR IGNORE INTO deployments (
            INSTANCE_ID,
            DEPLOYMENT_ID,
            CHUTE_ID,
            HOST_IP,
            GPU_TYPE,
            CREATED_AT,
            GPU_COUNT
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
        '''
        self.cursor.execute(sql, instance_data)
        self.conn.commit()

    def query_active_instances(self):
        sql = '''SELECT * FROM deployments WHERE DELETED_AT=0 or DELETED_AT IS NULL ORDER BY CREATED_AT DESC;'''
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def check_instance_if_exists(self, instance_data):
        sql = '''SELECT * FROM deployments
            WHERE INSTANCE_ID = ?
            AND DEPLOYMENT_ID = ?
            AND CHUTE_ID = ?
            AND HOST_IP = ?
            AND GPU_TYPE = ?
        ;'''

        self.cursor.execute(sql, instance_data)
        return self.cursor.fetchall()

    def update_instance_deleted_at(self, instance_data):
        sql = '''UPDATE deployments SET DELETED_AT = ? WHERE INSTANCE_ID = ?;'''

        self.cursor.execute(sql, instance_data)
        self.conn.commit()


if __name__ == "__main__":
    db_name = "chutes_deployments.db"
    instance_db = SQLiteInstance(db_name)
    instance_db.connect()
    instance_db.create_table()
    # instance_db.insert_instance((1,2,3,4,5,6,7))
    # results = instance_db.query_active_instances()
    # result = instance_db.check_instance_if_exists((1,2,3,4,5))
    # instance_db.update_instance_deleted_at((111, 1))
    instance_db.close_connection()

