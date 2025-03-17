from sqlite_base import *

class QueryResult:
    def __init__(self, instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count):
        self.instance_id = instance_id
        self.deployment_id = deployment_id
        self.chute_id = chute_id
        self.host_ip = host_ip
        self.gpu_type = gpu_type
        self.created_at = created_at
        self.gpu_count = gpu_count


    def __repr__(self):
        return (
                f"'instance_id': '{self.instance_id}',"
                f"'deployment_id': '{self.deployment_id}',"
                f"'chute_id': '{self.chute_id}',"
                f"'host_ip': '{self.host_ip}',"
                f"'gpu_type': '{self.gpu_type}',"
                f"'created_at': '{self.created_at}',"
                f"'gpu_count': '{self.gpu_count}'"
                )


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

        chutes_sql = '''CREATE TABLE IF NOT EXISTS chutes (
            CHUTE_ID CHAR(50) NOT NULL,
            MODEL_NAME CHAR(50) NOT NULL,
            DELETED_AT CHAR(50) DEFAULT 0
        )'''

        self.cursor.execute(sql)
        self.cursor.execute(chutes_sql)
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
        sql = '''SELECT instance_id, deployment_id, chute_id, host_ip, gpu_type, created_at, gpu_count
                FROM deployments
                WHERE DELETED_AT=0 or DELETED_AT IS NULL ORDER BY CREATED_AT DESC
        ;'''
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        results = [QueryResult(*row) for row in rows]
        return [QueryResult(*row) for row in rows]

    def instance_exists(self, instance_data):
        sql = '''SELECT COUNT(*) FROM deployments
            WHERE INSTANCE_ID = ?
            AND DEPLOYMENT_ID = ?
            AND CHUTE_ID = ?
            AND HOST_IP = ?
            AND GPU_TYPE = ?
        ;'''
        self.cursor.execute(sql, instance_data[:5])
        return self.cursor.fetchone()[0] > 0

    def update_instance_deleted_at(self, instance_data):
        sql = '''UPDATE deployments SET DELETED_AT = ? 
            WHERE INSTANCE_ID = ?
        ;'''
        self.cursor.execute(sql, instance_data)
        self.conn.commit()

    def insert_chute_model(self, chute_data):
        sql = '''INSERT OR IGNORE INTO chutes (
            CHUTE_ID,
            MODEL_NAME
            ) VALUES (?, ?);
        '''
        self.cursor.execute(sql, chute_data)
        self.conn.commit()

    def chute_model_exists(self, chute_data):
        sql = '''SELECT COUNT(*) FROM chutes
            WHERE CHUTE_ID = ?
        ;'''
        self.cursor.execute(sql, chute_data)
        return self.cursor.fetchone()[0] > 0

    def query_chute_model_name(self, chute_data):
        sql = '''SELECT MODEL_NAME FROM chutes
            WHERE CHUTE_ID = ?
        ;'''
        self.cursor.execute(sql, chute_data)
        return self.cursor.fetchone()[0]


if __name__ == "__main__":
    db_name = "chutes_deployments.db"
    instance_db = SQLiteInstance(db_name)
    instance_db.connect()
    instance_db.create_table()
    # instance_db.insert_instance((1,2,3,4,5,6,7))
    # results = instance_db.query_active_instances()
    # result = instance_db.instance_exists((1,2,3,4,5))
    # instance_db.update_instance_deleted_at((111, 1))
    instance_db.close_connection()

