"""Small HTTP benchmark, copies only. See --help. No production capacity claims."""
import argparse,base64,concurrent.futures,hashlib,hmac,json,os,platform,socket,sqlite3,statistics,subprocess,sys,time
from pathlib import Path
import httpx
p=argparse.ArgumentParser()
p.add_argument('--python',type=Path,required=True);p.add_argument('--database',type=Path,required=True);p.add_argument('--binary',type=Path,required=True);p.add_argument('--workdir',type=Path,default=Path('benchmark-work'))
a=p.parse_args();a.workdir.mkdir(exist_ok=True,parents=True)
results={}
def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def b64(v):return base64.urlsafe_b64encode(v).rstrip(b'=')
secret=b'benchmark-secret'
for name in ['python','go']:
    db=(a.workdir/(name+'.db')).resolve()
    if db.exists():db.unlink()
    with sqlite3.connect(a.database.resolve().as_uri()+'?mode=ro',uri=True) as source,sqlite3.connect(db) as target:source.backup(target)
    with sqlite3.connect(db) as c:
        uid=c.execute("insert into users(phone,password_hash,gender,height,weight,name,birth_year,activity_level) values('benchmark','unused',0,170,70,'Benchmark',1990,3)").lastrowid
        for i in range(40):
            mid=c.execute("insert into meals(user_id,date,meal_type,created_at,updated_at) values(?,?,1,'2026-01-01','2026-01-01')",(uid,f'2026-{i//28+1:02d}-{i%28+1:02d}')).lastrowid
            c.executemany("insert into meal_items(meal_id,food_id,food_name,calories,amount,unit) values(?,0,'test',52,1,'serving')",[(mid,)]*3)
    signing=b64(b'{"alg":"HS256","typ":"JWT"}')+b'.'+b64(json.dumps({'sub':uid,'exp':int(time.time())+3600}).encode())
    token=(signing+b'.'+b64(hmac.new(secret,signing,hashlib.sha256).digest())).decode()
    env=os.environ.copy();env.update(DATABASE_URL='sqlite:///'+str(db),JWT_SECRET=secret.decode(),APP_TIMEZONE='Asia/Tehran')
    n=port();log=(a.workdir/(name+'.log')).open('w')
    command=([sys.executable,'-m','uvicorn',a.python.stem+':app','--host','127.0.0.1','--port',str(n),'--no-access-log'] if name=='python' else [str(a.binary.resolve()),'-db',str(db),'-listen',f'127.0.0.1:{n}'])
    proc=subprocess.Popen(command,cwd=a.python.resolve().parent,env=env,stdout=log,stderr=log)
    try:
        with httpx.Client(base_url=f'http://127.0.0.1:{n}',headers={'Authorization':'Bearer '+token},timeout=60) as client:
            for _ in range(300):
                try:
                    if client.get('/').status_code==200:break
                except httpx.TransportError:time.sleep(.1)
            else:raise RuntimeError('startup timeout')
            cases=[('foods_100','GET','/foods',None,400),('profile','GET','/me',None,400),('meals_40_with_120_items','GET','/meals',None,300),('replace_meal','POST','/meals',{'meal_id':4,'date':'2026-11-01','foods':[{'food_id':0,'name':'test','calories':52,'amount':2,'unit':'g'}]},120)]
            results[name]={}
            for label,method,path,body,count in cases:
                for _ in range(10):client.request(method,path,json=body) if body else client.request(method,path)
                durations=[];errors=[]
                def request(_):
                    start=time.perf_counter();r=client.request(method,path,json=body) if body else client.request(method,path);elapsed=(time.perf_counter()-start)*1000
                    return elapsed,r.status_code
                start=time.perf_counter()
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    for duration,status in pool.map(request,range(count)):
                        durations.append(duration)
                        if status not in [200,201]:errors.append(status)
                elapsed=time.perf_counter()-start;durations.sort()
                results[name][label]={'requests':count,'concurrency':8,'requests_per_second':round(count/elapsed,1),'median_ms':round(statistics.median(durations),2),'p95_ms':round(durations[int(.95*len(durations))-1],2),'errors':len(errors)}
                print(name,label,results[name][label],flush=True)
    finally:
        proc.terminate()
        try:proc.wait(timeout=10)
        except subprocess.TimeoutExpired:proc.kill();proc.wait()
        log.close()
report={'environment':{'platform':platform.platform(),'python':platform.python_version(),'timestamp':time.strftime('%Y-%m-%dT%H:%M:%S%z')},'note':'Local HTTP microbenchmark, 8 concurrent clients, one Python/Uvicorn process; not a production load or capacity guarantee. Both use private SQLite copies with the same journal mode. Client overhead is included.','results':results}
(a.workdir/'benchmark.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
