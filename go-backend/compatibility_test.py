"""Differential tests against original main.py; never opens the original DB writable.
Usage: python compatibility_test.py --python ../main.py --database ../food.db --binary ./food-diary.exe
Requires the Python application's dependencies plus httpx. Runtime needs no Python.
All temporary files are created inside --workdir (default ./test-work).
"""
import argparse, base64, concurrent.futures, hashlib, hmac, importlib.util, json, os
from pathlib import Path
import random, shutil, socket, sqlite3, subprocess, sys, time
from datetime import datetime
import httpx

p=argparse.ArgumentParser()
p.add_argument('--python',type=Path,required=True)
p.add_argument('--database',type=Path,required=True)
p.add_argument('--binary',type=Path,required=True)
p.add_argument('--workdir',type=Path,default=Path('test-work'))
args=p.parse_args()
root=args.workdir.resolve();root.mkdir(parents=True,exist_ok=True)
original=args.database.resolve()
before_hash=hashlib.sha256(original.read_bytes()).hexdigest()
def backup(src,dst):
    if dst.exists():dst.unlink()
    with sqlite3.connect(src.as_uri()+'?mode=ro',uri=True) as source,sqlite3.connect(dst) as target:source.backup(target)
pydb=root/'python.db';godb=root/'go.db'
backup(original,pydb);backup(original,godb)
os.environ['DATABASE_URL']='sqlite:///'+str(pydb)
os.environ['JWT_SECRET']='differential-test-secret'
os.environ['APP_TIMEZONE']='Asia/Tehran'
spec=importlib.util.spec_from_file_location('reference',args.python.resolve());reference=importlib.util.module_from_spec(spec);sys.modules['reference']=reference;spec.loader.exec_module(reference)
from fastapi.testclient import TestClient
py=TestClient(reference.app,raise_server_exceptions=False)
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
log=(root/'go.log').open('w',encoding='utf-8')
proc=subprocess.Popen([str(args.binary.resolve()),'-db',str(godb),'-listen',f'127.0.0.1:{port}'],stdout=log,stderr=log,env=os.environ.copy())
go=httpx.Client(base_url=f'http://127.0.0.1:{port}',timeout=60,follow_redirects=False)
failures=[];count=0;completed=False
def check(label,condition,detail=None):
    global count
    count+=1
    if not condition:
        failures.append({'label':label,'detail':detail})
        print('FAIL',label,json.dumps(detail,ensure_ascii=False,default=str)[:1600],flush=True)
def normalize(v):
    if isinstance(v,dict):return {k:('TIMESTAMP' if k in ['created_at','updated_at'] else token_payload(x) if k=='access_token' else normalize(x)) for k,x in v.items()}
    if isinstance(v,list):return [normalize(x) for x in v]
    return v
def token_payload(token):
    body=token.split('.')[1];v=json.loads(base64.urlsafe_b64decode(body+'='*(-len(body)%4)));v.pop('exp',None);return v
def decode(resp):
    try:return resp.json()
    except Exception:return resp.text
def dynamic_values_match(a,b):
    if isinstance(a,dict) and isinstance(b,dict):
        for key,value in a.items():
            if key not in b:continue
            if key in ['created_at','updated_at']:
                try:
                    left,right=datetime.fromisoformat(value),datetime.fromisoformat(b[key])
                    if left.utcoffset()!=right.utcoffset() or abs((left-right).total_seconds())>5:return False
                except (TypeError,ValueError):return False
            elif key=='access_token':
                def payload(t):
                    s=t.split('.')[1];return json.loads(base64.urlsafe_b64decode(s+'='*(-len(s)%4)))
                lp,rp=payload(value),payload(b[key])
                if abs(lp['exp']-rp['exp'])>5 or abs(rp['exp']-(time.time()+30*86400))>5:return False
            elif not dynamic_values_match(value,b[key]):return False
    elif isinstance(a,list) and isinstance(b,list):
        return all(dynamic_values_match(x,y) for x,y in zip(a,b))
    return True
def compare(method,path,**kw):
    rp=py.request(method,path,follow_redirects=False,**kw)
    rg=go.request(method,path,**kw)
    a,b=normalize(decode(rp)),normalize(decode(rg))
    equal=rp.status_code==rg.status_code and a==b and dynamic_values_match(decode(rp),decode(rg))
    check(method+' '+path,equal,{'request':kw,'python':[rp.status_code,a],'go':[rg.status_code,b]} if not equal else None)
    return rp,rg
def db_snapshot(path,normalize_generated=True):
    with sqlite3.connect(path) as c:
        schema=c.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
        data={t:c.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall() for t in ['foods','users','meals','meal_items','sqlite_sequence']}
        # Password salts and newly-created timestamp values are independently random.
        if normalize_generated:
            data['users']=[tuple('<hash>' if i==2 else v for i,v in enumerate(row)) for row in data['users']]
            data['meals']=[row[:4]+('<time>','<time>') for row in data['meals']]
        return schema,data
try:
    for attempt in range(300):
        if proc.poll() is not None:raise RuntimeError('Go startup failed: '+(root/'go.log').read_text())
        try:
            if go.get('/').status_code==200:break
        except httpx.TransportError:time.sleep(.1)
    else:raise RuntimeError('Go startup timeout')
    check('startup preserves exact schema and data',db_snapshot(pydb,False)==db_snapshot(godb,False))
    for path in ['/','/foods','/foods?limit=5&offset=3','/foods/search?q=1','/foods/search?q=۱','/foods/search?q=١','/foods/search?q=سیب','/foods/search?q=%25','/foods/search?q=９','/foods/search?q=abc','/foods/search?q=0001','/foods/search?q=+1','/foods/search?q=999999999999999999999999','/Foods','/no-such-route']:
        compare('GET',path)
    for path in ['/foods?limit=0&offset=-1','/foods?limit=1001','/foods?limit=1.0','/foods?limit=+2','/foods?limit=1_0','/foods?limit=true','/foods?limit=1e2','/foods?limit=۲','/foods?limit=1&limit=2','/foods?offset=','/foods/search','/foods/search?q=','/foods/search?q='+'a'*101,'/foods?limit='+'9'*100,'/foods?offset='+'9'*100,'/%66oods'] :compare('GET',path)
    for method,path in [('HEAD','/'),('OPTIONS','/foods'),('PUT','/profile'),('DELETE','/meals')]:compare(method,path)
    for path in ['/me','/profile','/meals?day=bad']:
        for auth in [None,'','Basic x','Bearer','Bearer a.b.c']:
            compare('GET',path,headers={'Authorization':auth} if auth is not None else {})
    base={'phone':'compat-09120000','password':'p@ss فارسی🙂','gender':0,'height':178,'weight':74,'name':'نام🙂','birth_year':1994}
    rp,rg=compare('POST','/auth/register',json=base)
    if rp.status_code!=201 or rg.status_code!=201:raise RuntimeError('Cannot register test user')
    auth={'Authorization':'Bearer '+rp.json()['access_token']};uid=token_payload(rp.json()['access_token'])['sub']
    # Verify tokens issued by both implementations are accepted by the other.
    check('Go token accepted by Python',py.get('/me',headers={'Authorization':'Bearer '+rg.json()['access_token']}).status_code==200)
    compare('POST','/auth/register',json=base)
    compare('POST','/auth/login',json={'phone':base['phone'],'password':base['password']})
    compare('POST','/auth/login',json={'phone':base['phone'],'password':'wrong'})
    # Cross-verify both password formats with existing Python implementation.
    with sqlite3.connect(godb) as c:gh=c.execute('select password_hash from users where id=?',(uid,)).fetchone()[0]
    check('Python verifies Go password',reference.verify_password(base['password'],gh))
    with sqlite3.connect(pydb) as c:ph=c.execute('select password_hash from users where id=?',(uid,)).fetchone()[0]
    with sqlite3.connect(godb) as c:c.execute('update users set password_hash=? where id=?',(ph,uid))
    compare('POST','/auth/login',json={'phone':base['phone'],'password':base['password']})
    for path in ['/me','/profile','/meals','/meals?day=2026-10-25']:compare('GET',path,headers=auth)
    for payload in [{},{'weight':75},{'gender':0},{'age':30,'height':180,'activity_level':4},{'weight':None},{'weight':None,'gender':1},{'name':'x'},{'weight':0,'height':301,'age':131,'gender':2,'activity_level':0}]:compare('PATCH','/profile',json=payload,headers=auth)
    custom={'food_id':0,'amount':1.25,'name':'  کیک\u001c','calories':350.5,'unit':' عدد '}
    validmeal={'meal_id':1,'date':'2026-10-25','foods':[{'food_id':1,'amount':2},custom]}
    compare('POST','/meals',json=validmeal,headers=auth)
    for foods in [[custom],[{'food_id':1,'amount':2}]*3,[custom]]:compare('POST','/meals',json=validmeal|{'foods':foods},headers=auth)
    for foodid in [989,1243,1353,1487,999999]:compare('POST','/meals',json={'meal_id':2,'date':'2026-10-25','foods':[{'food_id':foodid,'amount':1}]},headers=auth)
    for date in [None,'1900-01-01','2100-12-31','9999-12-31','0001-01-01','2026-02-29','2026-13-01','2026-01-32','25/10/2026','',123,0,86400,True,{},'2026-01-01T00:00:00Z','2026-01-01T00:00:00+03:30','2026-01-01T01:00:00','2026-01-01T00:00:00.000','2026-01-01 00:00:00','2026-01-01junk']:
        compare('POST','/meals',json=validmeal|{'date':date},headers=auth)
        if isinstance(date,str):compare('GET','/meals',params={'day':date},headers=auth)
    compare('POST','/meals',json={'meal_id':4,'foods':[custom]},headers=auth)
    for date in ['2026-01-01T','2026-01-01T0','2026-01-01T00','2026-01-01T00:','2026-01-01T00:00','2026-01-01T00:00:','2026-01-01T24:00:00','2026-01-01T00:60:00','2026-01-01T00:00:60','2026-01-01T00:00:00+24:00','2026-01-01T00:00:00.0000001','2026-01-01T00:00:00+0330','2026-01-01T00:00:00,0','2026-01-01_00:00:00','0000-01-01','2026-1-1','1700000000','+86400','86400.0','1e5','20260101',' 2026-01-01','2026-01-01 ','NaN','Infinity','2026-01-01T00:00:00Zjunk']:
        compare('GET','/meals',params={'day':date},headers=auth)
    for payload in [{},{'meal_id':0,'foods':[]},{'meal_id':5,'foods':[]},{'meal_id':1,'foods':[{}]},{'meal_id':1,'foods':[custom]*101},{'meal_id':1,'foods':[1,None,[],{},'a']},validmeal|{'extra':1},validmeal|{'foods':[custom|{'extra':True}]},validmeal|{'foods':[custom|{'name':' '}]},validmeal|{'foods':[custom|{'calories':None}]},validmeal|{'foods':[{'food_id':1,'amount':1},{'food_id':999999,'amount':1}]}]:compare('POST','/meals',json=payload,headers=auth)
    # Exercise every field's type, length and boundary validation.
    values=[None,True,False,'',0,-1,1,1.0,1.5,'1',[],{},'x'*301,'🙂'*101,100001,10**40]
    for path,template,fields,headers in [('/auth/register',base,list(base)+['activity_level'],{}),('/auth/login',{'phone':'none','password':'pw'},['phone','password'],{}),('/profile',{'weight':70},['height','weight','age','gender','activity_level'],auth),('/meals',validmeal,['meal_id','foods'],auth)]:
        for field in fields:
            for value in values:
                payload=template|{field:value}
                # Avoid unintended valid new registrations (duplicates still test semantics).
                if path=='/auth/register' and field=='phone' and isinstance(value,str):payload['phone']='compat-'+value
                compare('PATCH' if path=='/profile' else 'POST',path,json=payload,headers=headers)
    for field in custom:
        for value in values:compare('POST','/meals',json=validmeal|{'foods':[custom|{field:value}]},headers=auth)
    for raw in ['', 'null','[]','1','true','"text"','{','{"weight":}','{"weight":1,}','{"weight" 1}','{"weight":01}','{"weight":1} {}','{"name":"\\x"}','{"name":"unterminated}','[1,]','{"🙂":}']:
        for headers in [{'Content-Type':'application/json'},auth|{'Content-Type':'application/json'}]:compare('PATCH','/profile',content=raw,headers=headers)
    for ct in ['text/plain','application/x-www-form-urlencoded','application/problem+json','application/json; charset=utf-8']:
        compare('PATCH','/profile',content='{"weight":80}',headers=auth|{'Content-Type':ct})
    for raw in ['{"weight":NaN}','{"weight":Infinity}','{"weight":-Infinity}','{"weight":1e400}','{"x":NaN}','{"weight":80}'.encode('utf-16'),'\ufeff{"weight":80}'.encode(),'{"weight":80}'.encode('utf-32'),b'\xff']:
        for headers in [auth|{'Content-Type':'application/json'},{'Content-Type':'application/json'}]:compare('PATCH','/profile',content=raw,headers=headers)
    compare('GET','/meals',headers=auth)
    # Every row of the supplied catalog, including all messy calorie formats.
    catalog=[]
    for offset in range(0,100000,1000):
        rp,rg=compare('GET','/foods',params={'offset':offset,'limit':1000})
        catalog.extend(rp.json())
        if len(rp.json())<1000:break
    for food in catalog:
        compare('POST','/meals',json={'meal_id':2,'date':'2026-11-01','foods':[{'food_id':food['id'],'amount':1.25}]},headers=auth)
    check('schema and stored values match after all paired writes',db_snapshot(pydb)==db_snapshot(godb),None if db_snapshot(pydb)==db_snapshot(godb) else 'Inspect copied test databases')
    # Simultaneous replacements must all succeed and leave exactly one complete meal.
    def worker(i):return go.post('/meals',headers=auth,json={'meal_id':3,'date':'2026-12-01','foods':[custom|{'amount':i+1}]}).status_code
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:codes=list(executor.map(worker,range(24)))
    check('24 concurrent writes succeed',all(c in [200,201] for c in codes),codes)
    with sqlite3.connect(godb) as c:
        check('concurrent writes do not duplicate meal',c.execute("select count(*) from meals where user_id=? and date='2026-12-01' and meal_type=3",(uid,)).fetchone()[0]==1)
        check('SQLite foreign keys intact',c.execute('PRAGMA foreign_key_check').fetchall()==[])
        check('SQLite integrity intact',c.execute('PRAGMA integrity_check').fetchone()[0]=='ok')
    for path in ['/docs','/docs/assets/swagger-ui-bundle.js','/docs/assets/swagger-ui.css','/openapi.json','/docs/oauth2-redirect']:
        r=go.get(path);check('offline docs '+path,r.status_code==200 and len(r.content)>50)
    doc=go.get('/openapi.json').json();check('all API paths documented',set(reference.app.openapi()['paths'])==set(doc['paths']))
    check('Bearer authorization in Swagger',doc['components']['securitySchemes']['BearerAuth']['scheme']=='bearer')
    # Deleted-user status matches across every authenticated endpoint.
    for db in [pydb,godb]:
        with sqlite3.connect(db) as c:
            c.execute('delete from meal_items where meal_id in (select id from meals where user_id=?)',(uid,));c.execute('delete from meals where user_id=?',(uid,));c.execute('delete from users where id=?',(uid,))
    for method,path,data in [('GET','/me',None),('GET','/profile',None),('PATCH','/profile',{'weight':80}),('POST','/meals',validmeal),('GET','/meals',None)]:compare(method,path,headers=auth,**({'json':data} if data is not None else {}))
    completed=True
finally:
    go.close();proc.terminate()
    try:proc.wait(timeout=10)
    except subprocess.TimeoutExpired:proc.kill();proc.wait()
    log.close();py.close();reference.engine.dispose()
    check('original database byte-for-byte unchanged',hashlib.sha256(original.read_bytes()).hexdigest()==before_hash)
    report={'completed':completed,'checks':count,'failures':len(failures),'details':failures,'reference_versions':{name:__import__(name).__version__ for name in ['fastapi','pydantic','sqlalchemy','httpx']}}
    (root/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    print(f'{count} checks; {len(failures)} failures',flush=True)
if failures:sys.exit(1)
