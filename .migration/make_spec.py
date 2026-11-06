import json
from pathlib import Path

spec=json.loads(Path('.migration/openapi.json').read_text(encoding='utf-8'))
schemas=spec['components']['schemas']
def obj(props,required=None):
    return {'type':'object','properties':props,'required':required or list(props)}
def ref(n): return {'$ref':'#/components/schemas/'+n}
integer={'type':'integer'}
string={'type':'string'}
number={'type':'number'}
schemas['Token']=obj({'access_token':string,'token_type':{'type':'string','const':'bearer'}})
schemas['User']=obj({k:integer for k in ['id','gender','height','weight','birth_year','activity_level','age']}|{k:string for k in ['phone','name']})
schemas['Food']=obj({'id':integer,'name':{'type':['string','null']},'calories':{'type':['string','number','null']}})
schemas['MealItem']=obj({k:integer for k in ['id','meal_id','food_id']}|{k:string for k in ['food_name','unit']}|{k:number for k in ['calories','amount']})
schemas['Meal']=obj({k:integer for k in ['id','user_id','meal_id']}|{k:string for k in ['date','meal_name','created_at','updated_at']}|{'total_calories':number,'foods':{'type':'array','items':ref('MealItem')}})
schemas['Meals']=obj({'date':{'type':['string','null'],'format':'date'},'meals':{'type':'array','items':ref('Meal')}})
schemas['Error']=obj({'detail':string})
spec['components']['securitySchemes']={'BearerAuth':{'type':'http','scheme':'bearer','bearerFormat':'JWT'}}
response_types={'/auth/register':'Token','/auth/login':'Token','/me':'User','/profile':'User','/meals':'Meals'}
for path,ops in spec['paths'].items():
    for method,op in ops.items():
        op['tags']=['Authentication' if path.startswith('/auth/') else 'Foods' if path.startswith('/foods') else 'Meals' if path=='/meals' else 'Profile' if path in ['/me','/profile'] else 'Service']
        if path in ['/me','/profile','/meals']:
            op['security']=[{'BearerAuth':[]}]
            op['parameters']=[p for p in op.get('parameters',[]) if p['in']!='header']
        if path.startswith('/foods'): schema={'type':'array','items':ref('Food')}
        elif path=='/': schema=obj({'message':string,'docs':string})
        elif path=='/meals' and method=='post':schema=ref('Meal')
        else:schema=ref(response_types[path])
        for code,response in op['responses'].items():
            if code in ['200','201']:response['content']['application/json']['schema']=schema
        if path=='/meals' and method=='post':
            op['responses']['200']={'description':'Existing meal replaced atomically; id and created_at remain unchanged','content':{'application/json':{'schema':schema}}}
            op['description']='Replaces all items for the authenticated user, date and meal type. Defaults to today in APP_TIMEZONE. Catalog foods use cached or parsed calories and unit serving. Custom foods (food_id=0) require name, calories and unit. Any failure rolls back the entire request.'
        errors={'401':'Authentication failed','404':'Record not found','409':'Phone already registered','400':'No profile changes'}
        codes=[]
        if path in ['/auth/login','/me','/profile','/meals']:codes.append('401')
        if path=='/auth/register':codes+=['409']
        if path=='/profile' and method=='patch':codes+=['400','404']
        if path=='/meals' and method=='post':codes+=['404']
        for code in codes:op['responses'][code]={'description':errors[code],'content':{'application/json':{'schema':ref('Error')}}}
        if '422' in op['responses']:
            op['responses']['422']['content']['application/json']['schema']={'oneOf':[ref('HTTPValidationError'),ref('Error')]}
spec['info']['description']='Compatible Food Diary API. Swagger UI assets are served locally. Keep the original JWT_SECRET, APP_TIMEZONE and SQLite database when replacing the Python service.'
source=Path('go-backend/main.go')
with source.open('a',encoding='utf-8') as f:
    f.write('\nfunc(s *server) openAPI()[]byte {\nvar doc object\nif err:=json.Unmarshal([]byte(openAPITemplate), &doc);err!=nil{panic(err)}\ndoc["components"].(map[string]any)["schemas"].(map[string]any)["RegisterIn"].(map[string]any)["properties"].(map[string]any)["birth_year"].(map[string]any)["maximum"]=s.birthMax\nb,err:=json.Marshal(doc);if err!=nil{panic(err)};return b\n}\n')
    f.write('const openAPITemplate = `'+json.dumps(spec,ensure_ascii=False,separators=(',',':'))+'`\n')
