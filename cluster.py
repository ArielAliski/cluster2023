import mysql.connector as connection
import time
import pandas as pd
import psutil
from bounded_pool_executor import BoundedProcessPoolExecutor
import warnings

warnings.filterwarnings(\"ignore\")


def loadDB(id_kota):
    t = time.time()
    try:
        mydb = connection.connect(host=\"localhost\",
                                  database='CBT_JATIM',
                                  user=\"root123\",
                                  password=\"root123\", use_pure=True)
        # query = 'select nama from Kota where id=%d;' % id_kota
        
        query = 'select id_siswa, nama, nrp, jawaban, jawaban_benar, id_mapel from soal_jawaban where id_kota=%d LIMIT 5000;' % id_kota
        ujian_siswa = pd.read_sql(query, mydb)

        mydb.close()  # close the connection
    except Exception as e:
        mydb.close()
        print(str(e))

    elapsed = time.time() - t
    print(\"Time Load DB  = {:.3f}\".format(elapsed))
    ujian_siswa.loc[ujian_siswa['jawaban'] == ujian_siswa['jawaban_benar'], ['score']] = 1
    ujian_siswa = ujian_siswa.fillna(0)
    result = ujian_siswa.groupby(['id_siswa', 'nama', 'nrp', 'id_mapel'])['score'].agg('sum')
    result.to_csv(\"id_kota_%d.csv\" % id_kota)


if __name__ == '__main__':

    tAll = time.time()
    n_jobs = psutil.cpu_count()
    print(\"Ready to worker\")
    cnt = 0
    with BoundedProcessPoolExecutor(max_workers=n_jobs) as worker:
        for id_kota in range(1,5):
            print('#%d Worker initialization %s' % (cnt, id_kota))
            cnt += 1
            print(\"Load DB %d, please wait ...\" % id_kota)
            # loadDB(id_kota)
            worker.submit(loadDB, id_kota)
    elapsed = time.time() - tAll
    print(\"Time selesai  = {:.3f}\".format(elapsed))
