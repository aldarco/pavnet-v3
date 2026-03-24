'''
This code is the class that operates the load of the PAVNET data into the INFLUX DB 
Author 

'''

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import time
import json
from datetime import datetime
import sqlite3
import threading
import urllib3.exceptions
import requests.exceptions


class DBClient(InfluxDBClient):
    def __init__(self, location_key, localdbname="upload_queue_data.db"):
        # variables on env
        self.INFLUXDB_URL = os.getenv('INFLUXDB_URL')     #'http://localhost:8086')
        self.INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN') #
        self.INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
        self.INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')
        self.LOC = location_key
        self.INFLUXDB_MEASUREMENT = f'AMP-{self.LOC}'  # nombre de la medición
        print(f'''
        URL   : {self.INFLUXDB_URL},
        TOKEN : {self.INFLUXDB_TOKEN},
        ORG   : {self.INFLUXDB_ORG},
        BUVKET: {self.INFLUXDB_BUCKET},
        MEAS  : {self.INFLUXDB_MEASUREMENT}
        ''')

        
        super().__init__(
                        url=self.INFLUXDB_URL,
                        token=self.INFLUXDB_TOKEN,
                        org=self.INFLUXDB_ORG
                        )
        self.writer = self.write_api(write_options=SYNCHRONOUS)
        
        # Local DB SQLite for backup when conectionis lost
        self.localdb_path = os.path.join(os.path.dirname(__file__),localdbname)
        self.init_sqlite()

        self._start_retry_thread()

        # just to check params
        print(f'''
        URL   : {self.INFLUXDB_URL},
        TOKEN : {self.INFLUXDB_TOKEN},
        ORG   : {self.INFLUXDB_ORG},
        BUVKET: {self.INFLUXDB_BUCKET},
        MEAS  : {self.INFLUXDB_MEASUREMENT}
        ''')

        # print("DB Client Created")                        
        
        # print("DB writer ready")

    def init_sqlite(self):
        conn = sqlite3.connect(self.localdb_path)
        c_ = conn.cursor()
        c_.execute('''
                    CREATE TABLE IF NOT EXISTS pending (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXTNOT NULL,
                        data TEXT NOT NULL --json amplitudes
                        )
                    ''')
        conn.commit()
        conn.close()
    
    def _start_retry_thread(self):
        # the pending data is tried to upload in a different thread
        self.retry_thread = threading.Thread(target=self._retry_loop, daemon=True)
        self.retry_thread.start()
    
    #def send_to_pendingdb(self):

    def _send_to_remotedb(self,timestamp, amp_data):
        point = Point(self.INFLUXDB_MEASUREMENT)
        point.time(timestamp)
        #print(type(amp_data))
        # in dict amp_data, a key only has a value/float
        for key, value in amp_data.items():
            point.field(key, value)
        
        self.writer.write(bucket=self.INFLUXDB_BUCKET, record=point)


    def send_to_db(self, timestamp, amp_data, metadata=None):
        '''
        Loads the processed pavnet data to InfluxDB as long as there is connection to the Influx DB,
        otherwise it writes in a local SQLite DB pending for load
        Args:
        - timestamp: (datetime) creation data
        - amp_data : (Dict) amplitudes of transmitters {"Tx_A":amp_A, ...}
        - metadata : Wether to save metadata
        '''
        try:
            # TODO: metadata stage
            self._send_to_remotedb(timestamp, amp_data)
            
        #connection lost
        except (urllib3.exceptions.NewConnectionError,
            urllib3.exceptions.MaxRetryError,
            requests.exceptions.ConnectionError) as e:
            # cannot connect to influx DB -> sending to sqlite (local)
            print("+-----> Connection lost")
            self._send_to_localdb(timestamp, amp_data)


            
        except Exception as e:
            print(" -> Err in send_to_db")
            print(f" -> Error {e}")
            self._send_to_localdb(timestamp, amp_data)
        # so far this sends the datapoint to "remote" DB
        # but what if it fails?
        # TODO: a local backup is needed
        # ... DONE. Conditional in send_to_db

    def _send_to_localdb(self, ct, amp_data):
        t0 = time.perf_counter()
        record = {
            'timestamp': ct.isoformat() if isinstance(ct, datetime) else str(ct),
            'amplitudes': {k: v for k, v in amp_data.items()}
            # 'metadata': {
            #     'filename': os.path.basename(filepath),
            #     'source': os.path.basename(self.arrival_dir)
            # }

        }
        t0 = time.perf_counter()
        conn = sqlite3.connect(self.localdb_path)
        c_ = conn.cursor()
        c_.execute("INSERT INTO pending (timestamp, data) VALUES (?, ?)",
                    (record["timestamp"], json.dumps(record))
                )
        conn.commit()
        inserted_id = c_.lastrowid
        conn.close()
        t1 = time.perf_counter()
        print(f"  Writed in SQLite in {t1-t0:.4f} s")

    def _retry_loop(self):
        while True:
            conn = sqlite3.connect(self.localdb_path)
            c_ = conn.cursor()
            c_.execute("SELECT id, timestamp, data FROM pending")
            rows = c_.fetchall()
            nrows_found = len(rows)
            # only try reloading if there are rows
            if nrows_found:
                print(f"Found {nrows_found} pending to load.")
                for r in rows:
                    pid, ts , data_json = r
                    data = json.loads(data_json)
                    try:
                        self._send_to_remotedb(data["timestamp"], data["amplitudes"])
                        c_.execute('DELETE FROM pending WHERE id = ?', (pid,))
                    # connection still down
                    except (urllib3.exceptions.NewConnectionError,
                        urllib3.exceptions.MaxRetryError,
                        requests.exceptions.ConnectionError) as e:
                        print("   - Reload Connection Failed")
                        break
                    except Exception as e:
                        print("   --- Error reloading :",e)
                        break
            
            conn.commit()
            conn.close()
            time.sleep(10)


if __name__ == "__main__":
    # a dummie/demo test
    import random
    #ct = datetime(2026, 2, 19,15,2,0)
    for _k in range(1):
        ct = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        values = [random.random() for i in range(5)]
        data_amp = {"NPM":values[0],"NAA":10*values[1],"NLK":values[2], "NLM":values[3],"NAU":values[4]}
        client = DBClient("MiPC")
        client.send_to_db(ct, data_amp)
        print(f"{_k} item", end=":: ")
        for k, v in data_amp.items():
            print(f"{k}={v:.4f}", end="\t")
        time.sleep(1)
        print()
    print("\n---")
